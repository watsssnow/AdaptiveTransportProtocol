import socket
import threading
import time
import re
import signal
import sys

HOST = '0.0.0.0'
TCP_SERVICE_PORT = 8080
UDP_TELEMETRY_PORT = 8081
UDP_SYNC_PORT = 8082
UDP_ADAPTIVELOSS_PORT = 8083
TCP_ADAPTIVELOSS_PORT = 8084
UDP_ADAPTIVEDELAY_PORT = 8085
TCP_ADAPTIVEDELAY_PORT = 8086

running = True
server_start_us = int(time.time_ns() // 1000)

# ---- служебный сокет (глобальный) ----
service_sock = None

# ---- синхронизация ----
clock_offset_us = 0
clock_synced = False
offset_lock = threading.Lock()
last_owd = None

# ---- обучение дрейфу ----
offset_history = []
drift_rate = 0.0
drift_ready = False
MIN_HISTORY = 3

# ---- статистика ----
class ChanStats:
    def __init__(self, name):
        self.name = name
        self.received = 0
        self.total_sent = 0
        self.delays_us = []
        self.last_sent = None

telemetry_stats = ChanStats("TELEMETRY")
adaptive_loss_stats = ChanStats("ADAPTIVE_LOSS")
adaptive_delay_stats = ChanStats("ADAPTIVE_DELAY")
stats_lock = threading.Lock()

# ---- AdaptiveLoss ----
loss_packets_received = 0
loss_last_total_sent = None
loss_protocol = "UDP"
loss_switch_cooldown = 0
loss_pending_return = False
loss_lock = threading.Lock()

# ---- AdaptiveDelay (UDP -> TCP при высокой задержке) ----
delay_packets_received = 0
delay_delays = []
delay_protocol = "UDP"
delay_switch_cooldown = 0
delay_pending_return = False
delay_lock = threading.Lock()

# ---- парсинг ----
def extract_total_sent(msg):
    m = re.search(r'Total sent:\s*(\d+)', msg)
    return int(m.group(1)) if m else None

def extract_timestamp(msg):
    m = re.search(r'Timestamp:\s*(\d+)\s*us', msg)
    return int(m.group(1)) if m else None

def extract_protocol(msg):
    m = re.search(r'Protocol:\s*(\w+)', msg)
    return m.group(1) if m else None

# ---- OWD ----
def compute_owd(esp_ts):
    global clock_offset_us, clock_synced, server_start_us
    global drift_rate, drift_ready

    if not clock_synced:
        return None

    with offset_lock:
        base_offset = clock_offset_us
    now_rel = int(time.time_ns() // 1000) - server_start_us

    if drift_ready and len(offset_history) > 0:
        last_T_srv, last_offset = offset_history[-1]
        delta_t = now_rel - last_T_srv
        corrected_offset = last_offset + drift_rate * delta_t
    else:
        corrected_offset = base_offset

    owd = now_rel - (esp_ts + corrected_offset)
    return float(owd)

# ---- Telemetry ----
def process_telemetry(msg):
    sent = extract_total_sent(msg)
    ts = extract_timestamp(msg)
    if sent is None:
        return

    owd = compute_owd(ts) if ts is not None else None

    with stats_lock:
        telemetry_stats.received += 1
        telemetry_stats.total_sent = sent
        if owd is not None and owd >= 0:
            telemetry_stats.delays_us.append(owd)

    print(f"\n--- [TELEMETRY] #{telemetry_stats.received} ---")
    print(msg)
    print(f"  Total sent: {sent}, Received: {telemetry_stats.received}", end="")
    if owd is not None and owd >= 0:
        print(f", OWD: {owd:.1f} us ({owd/1000:.3f} ms)")
    else:
        print(f", OWD: N/A")
    print("---")

# ---- AdaptiveLoss ----
def process_adaptive_loss(msg):
    global loss_protocol, loss_switch_cooldown, loss_pending_return
    global loss_packets_received, loss_last_total_sent

    sent = extract_total_sent(msg)
    ts = extract_timestamp(msg)
    proto = extract_protocol(msg)
    if sent is None:
        return

    owd = compute_owd(ts) if ts is not None else None

    with stats_lock:
        adaptive_loss_stats.received += 1
        adaptive_loss_stats.total_sent = sent
        if owd is not None and owd >= 0:
            adaptive_loss_stats.delays_us.append(owd)

    with loss_lock:
        if loss_packets_received == 0:
            loss_last_total_sent = sent
        loss_packets_received += 1

        print(f"\n--- [ADAPTIVE_LOSS] #{adaptive_loss_stats.received} ({proto}) ---")
        print(msg)
        print(f"  Total sent: {sent}, Received: {adaptive_loss_stats.received}", end="")
        if owd is not None and owd >= 0:
            print(f", OWD: {owd:.1f} us ({owd/1000:.3f} ms)")
        else:
            print(f", OWD: N/A")
        print(f"  Loss window: {loss_packets_received}/100")
        print("---")

        if loss_packets_received >= 100:
            expected = sent - loss_last_total_sent + 1
            loss_pct = (expected - loss_packets_received) / expected * 100
            print(f"[ADAPTIVE_LOSS WINDOW] expected={expected}, received={loss_packets_received}, loss_pct={loss_pct:.1f}%, protocol={loss_protocol}, cooldown_ok={time.time() >= loss_switch_cooldown}")
            loss_packets_received = 0
            loss_last_total_sent = None

            if loss_protocol == "UDP" and loss_pct > 18 and time.time() >= loss_switch_cooldown:
                loss_protocol = "TCP"
                loss_switch_cooldown = time.time() + 10
                loss_pending_return = True
                cmd = "SWITCH_ADAPTIVELOSS_TO_TCP\n"
                if service_sock:
                    try: service_sock.sendall(cmd.encode())
                    except: pass
                print(f"[ADAPTIVE_LOSS] Loss {loss_pct:.1f}% > 18% -> SWITCH TO TCP, cooldown 10s")

        if loss_pending_return and time.time() >= loss_switch_cooldown:
            loss_protocol = "UDP"
            loss_pending_return = False
            loss_packets_received = 0
            loss_last_total_sent = None
            cmd = "SWITCH_ADAPTIVELOSS_TO_UDP\n"
            if service_sock:
                try: service_sock.sendall(cmd.encode())
                except: pass
            print(f"[ADAPTIVE_LOSS] Cooldown expired -> SWITCH TO UDP")

# ---- AdaptiveDelay (UDP -> TCP при высокой задержке) ----
def process_adaptive_delay(msg):
    global delay_protocol, delay_switch_cooldown, delay_pending_return
    global delay_packets_received, delay_delays

    sent = extract_total_sent(msg)
    ts = extract_timestamp(msg)
    proto = extract_protocol(msg)
    if sent is None:
        return

    owd = compute_owd(ts) if ts is not None else None

    # Для окна переключения: N/A или отрицательные -> 0
    owd_for_window = owd if (owd is not None and owd >= 0) else 0.0

    with stats_lock:
        adaptive_delay_stats.received += 1
        adaptive_delay_stats.total_sent = sent
        if owd is not None and owd >= 0:
            adaptive_delay_stats.delays_us.append(owd)

    with delay_lock:
        if delay_protocol == "UDP":
            delay_delays.append(owd_for_window)
        delay_packets_received += 1

        print(f"\n--- [ADAPTIVE_DELAY] #{adaptive_delay_stats.received} ({proto}) ---")
        print(msg)
        print(f"  Total sent: {sent}, Received: {adaptive_delay_stats.received}", end="")
        if owd is not None and owd >= 0:
            print(f", OWD: {owd:.1f} us ({owd/1000:.3f} ms)")
        else:
            print(f", OWD: N/A")
        print(f"  Delay window: {len(delay_delays)}/100")
        print("---")

        if len(delay_delays) >= 100:
            avg_owd = sum(delay_delays) / len(delay_delays)
            delay_packets_received = 0
            delay_delays = []

            if delay_protocol == "UDP" and avg_owd > 170000 and time.time() >= delay_switch_cooldown:
                delay_protocol = "TCP"
                delay_switch_cooldown = time.time() + 10
                delay_pending_return = True
                cmd = "SWITCH_ADAPTIVEDELAY_TO_TCP\n"
                if service_sock:
                    try: service_sock.sendall(cmd.encode())
                    except: pass
                print(f"[ADAPTIVE_DELAY] Avg OWD {avg_owd:.1f} us > 170000 us -> SWITCH TO TCP, cooldown 10s")

        if delay_pending_return and time.time() >= delay_switch_cooldown:
            delay_protocol = "UDP"
            delay_pending_return = False
            delay_packets_received = 0
            delay_delays = []
            cmd = "SWITCH_ADAPTIVEDELAY_TO_UDP\n"
            if service_sock:
                try: service_sock.sendall(cmd.encode())
                except: pass
            print(f"[ADAPTIVE_DELAY] Cooldown expired -> SWITCH TO UDP")

# ---- Telemetry UDP ----
def udp_telemetry_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, UDP_TELEMETRY_PORT))
    s.settimeout(0.1)
    print(f"[UDP TELEMETRY] port {UDP_TELEMETRY_PORT}")

    while running:
        try:
            data, addr = s.recvfrom(4096)
            if data:
                process_telemetry(data.decode().strip())
        except socket.timeout:
            continue
        except Exception as e:
            if running: print(f"[UDP TELEMETRY] error: {e}")
    s.close()

# ---- AdaptiveLoss UDP ----
def udp_adaptive_loss_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, UDP_ADAPTIVELOSS_PORT))
    s.settimeout(0.1)
    print(f"[UDP ADAPTIVE_LOSS] port {UDP_ADAPTIVELOSS_PORT}")

    while running:
        try:
            data, addr = s.recvfrom(4096)
            if data:
                process_adaptive_loss(data.decode().strip())
        except socket.timeout:
            continue
        except Exception as e:
            if running: print(f"[UDP ADAPTIVE_LOSS] error: {e}")
    s.close()

# ---- AdaptiveLoss TCP ----
def tcp_adaptive_loss_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, TCP_ADAPTIVELOSS_PORT))
    s.listen(5)
    s.settimeout(0.1)
    print(f"[TCP ADAPTIVE_LOSS] port {TCP_ADAPTIVELOSS_PORT}")

    while running:
        try:
            cl, addr = s.accept()
            print(f"[TCP ADAPTIVE_LOSS] connected: {addr}")
            cl.settimeout(0.1)
            buf = ""
            delim = "===========================\n"
            while running:
                try:
                    data = cl.recv(4096)
                    if not data: break
                    buf += data.decode()
                    msgs = buf.split(delim)
                    buf = msgs[-1]
                    for m in msgs[:-1]:
                        if m.strip():
                            process_adaptive_loss(m)
                except socket.timeout:
                    continue
                except:
                    break
            cl.close()
            print(f"[TCP ADAPTIVE_LOSS] disconnected: {addr}")
        except socket.timeout:
            continue
        except Exception as e:
            if running: print(f"[TCP ADAPTIVE_LOSS] error: {e}")
    s.close()

# ---- AdaptiveDelay UDP ----
def udp_adaptive_delay_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, UDP_ADAPTIVEDELAY_PORT))
    s.settimeout(0.1)
    print(f"[UDP ADAPTIVE_DELAY] port {UDP_ADAPTIVEDELAY_PORT}")

    while running:
        try:
            data, addr = s.recvfrom(4096)
            if data:
                process_adaptive_delay(data.decode().strip())
        except socket.timeout:
            continue
        except Exception as e:
            if running: print(f"[UDP ADAPTIVE_DELAY] error: {e}")
    s.close()

# ---- AdaptiveDelay TCP ----
def tcp_adaptive_delay_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, TCP_ADAPTIVEDELAY_PORT))
    s.listen(5)
    s.settimeout(0.1)
    print(f"[TCP ADAPTIVE_DELAY] port {TCP_ADAPTIVEDELAY_PORT}")

    while running:
        try:
            cl, addr = s.accept()
            print(f"[TCP ADAPTIVE_DELAY] connected: {addr}")
            cl.settimeout(0.1)
            buf = ""
            delim = "===========================\n"
            while running:
                try:
                    data = cl.recv(4096)
                    if not data: break
                    buf += data.decode()
                    msgs = buf.split(delim)
                    buf = msgs[-1]
                    for m in msgs[:-1]:
                        if m.strip():
                            process_adaptive_delay(m)
                except socket.timeout:
                    continue
                except:
                    break
            cl.close()
            print(f"[TCP ADAPTIVE_DELAY] disconnected: {addr}")
        except socket.timeout:
            continue
        except Exception as e:
            if running: print(f"[TCP ADAPTIVE_DELAY] error: {e}")
    s.close()

# ---- Sync ----
def udp_sync_server():
    global clock_offset_us, clock_synced, last_owd
    global offset_history, drift_rate, drift_ready

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, UDP_SYNC_PORT))
    s.settimeout(0.1)
    print(f"[UDP SYNC] port {UDP_SYNC_PORT}")
    history = {}

    while running:
        try:
            data, addr = s.recvfrom(128)
            msg = data.decode().strip()
            if msg.startswith("SYNC:"):
                try:
                    T_esp = int(msg.split(":")[1])
                except: continue
                T_srv = int(time.time_ns() // 1000) - server_start_us
                history[T_esp] = T_srv
                s.sendto(str(T_srv).encode(), addr)

            elif msg.startswith("OFFSET_DATA:"):
                try:
                    parts = msg.split(":")[1].split(",")
                    T_esp = int(parts[0])
                    owd = int(parts[1])
                    if T_esp not in history:
                        continue
                    T_srv = history[T_esp]
                    new_offset = T_srv - (T_esp + owd)

                    if not clock_synced:
                        with offset_lock:
                            clock_offset_us = new_offset
                            clock_synced = True
                        last_owd = owd
                        offset_history.append((T_srv, new_offset))
                        print(f"[SYNC] Initial offset = {clock_offset_us} us")
                        continue

                    if last_owd is not None and owd > last_owd * 3:
                        print(f"[SYNC] OWD {owd} us too large vs last {last_owd} us — ignoring")
                        continue
                    if abs(new_offset - clock_offset_us) > 5000:
                        print(f"[SYNC] New offset {new_offset} differs too much — ignoring")
                        continue

                    with offset_lock:
                        clock_offset_us = new_offset
                    last_owd = owd
                    offset_history.append((T_srv, new_offset))
                    print(f"[SYNC] Offset updated = {clock_offset_us} us")

                    if len(offset_history) >= MIN_HISTORY:
                        n = len(offset_history)
                        sum_x = sum(h[0] for h in offset_history)
                        sum_y = sum(h[1] for h in offset_history)
                        sum_xy = sum(h[0] * h[1] for h in offset_history)
                        sum_x2 = sum(h[0] * h[0] for h in offset_history)

                        if n * sum_x2 - sum_x * sum_x != 0:
                            drift_rate = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
                            drift_ready = True
                            print(f"[SYNC] Drift rate = {drift_rate:.6f} us/s ({drift_rate*1000000:.2f} ppm)")

                        if len(offset_history) > 10:
                            offset_history = offset_history[-10:]

                except Exception as e:
                    print(f"[SYNC] parse error: {e}")
        except socket.timeout:
            continue
        except Exception as e:
            if running: print(f"[SYNC] error: {e}")
    s.close()

# ---- итоги ----
def print_final(s: ChanStats):
    print(f"\n{'='*50}")
    print(f"  {s.name}")
    print(f"{'='*50}")
    print(f"  Received: {s.received}")
    print(f"  Total sent (last): {s.total_sent}")
    if s.total_sent:
        loss = (s.total_sent - s.received) / s.total_sent * 100
        print(f"  Packet Loss: {loss:.2f}%")
    if s.delays_us:
        d = s.delays_us
        print(f"  Delay samples: {len(d)}")
        print(f"  Avg OWD: {sum(d)/len(d):.1f} us ({sum(d)/len(d)/1000:.3f} ms)")
        print(f"  Min OWD: {min(d):.1f} us")
        print(f"  Max OWD: {max(d):.1f} us")
    else:
        print(f"  Delay: N/A")
    print(f"{'='*50}\n")

def sig_handler(sig, frame):
    global running
    print("\n[!] Ctrl+C — stopping...")
    running = False

def main():
    global running, service_sock
    signal.signal(signal.SIGINT, sig_handler)

    print("=== MULTI-CHANNEL + TELEMETRY + ADAPTIVE ===")
    print(f"Service:       TCP {TCP_SERVICE_PORT}")
    print(f"Telemetry:     UDP {UDP_TELEMETRY_PORT}")
    print(f"Sync:          UDP {UDP_SYNC_PORT}")
    print(f"AdaptiveLoss:  UDP {UDP_ADAPTIVELOSS_PORT} / TCP {TCP_ADAPTIVELOSS_PORT}")
    print(f"AdaptiveDelay: UDP {UDP_ADAPTIVEDELAY_PORT} / TCP {TCP_ADAPTIVEDELAY_PORT}")
    print("================================================\n")

    service_sock = None

    threads = [
        threading.Thread(target=udp_sync_server, daemon=True),
        threading.Thread(target=udp_telemetry_server, daemon=True),
        threading.Thread(target=udp_adaptive_loss_server, daemon=True),
        threading.Thread(target=tcp_adaptive_loss_server, daemon=True),
        threading.Thread(target=udp_adaptive_delay_server, daemon=True),
        threading.Thread(target=tcp_adaptive_delay_server, daemon=True),
    ]
    for t in threads: t.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, TCP_SERVICE_PORT))
    s.listen(5)
    s.settimeout(0.1)
    print(f"[TCP SERVICE] port {TCP_SERVICE_PORT}")

    while running:
        try:
            cl, addr = s.accept()
            print(f"[TCP SERVICE] connected: {addr}")
            service_sock = cl
            cl.settimeout(0.1)
            while running:
                try:
                    data = cl.recv(4096)
                    if not data:
                        break
                    print(f"[SERVICE] {data.decode().strip()}")
                except socket.timeout:
                    continue
                except:
                    break
            cl.close()
            service_sock = None
            print(f"[TCP SERVICE] disconnected: {addr}")
        except socket.timeout:
            continue
        except Exception as e:
            if running: print(f"[TCP SERVICE] error: {e}")
    s.close()

    running = False
    time.sleep(0.5)

    print("\n=== FINAL STATS ===")
    print_final(telemetry_stats)
    print_final(adaptive_loss_stats)
    print_final(adaptive_delay_stats)

if __name__ == "__main__":
    main()
