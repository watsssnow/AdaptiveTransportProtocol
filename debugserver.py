import socket
import threading
import time
import re
import signal
import sys

HOST = '0.0.0.0'
TCP_PORT = 8080
UDP_PORT = 8081
SYNC_PORT = 8082

running = True

clock_offset_us = 0
clock_synced = False
offset_lock = threading.Lock()

# Время старта сервера (относительный ноль)
server_start_us = int(time.time_ns() // 1000)

class PacketStats:
    def __init__(self, protocol_name):
        self.protocol = protocol_name
        self.total_received = 0
        self.total_sent = 0
        self.delays_us = []

tcp_stats = PacketStats("TCP")
udp_stats = PacketStats("UDP")
stats_lock = threading.Lock()


def extract_total_sent(message):
    match = re.search(r'Total sent:\s*(\d+)', message)
    if match:
        return int(match.group(1))
    return None


def extract_timestamp(message):
    match = re.search(r'^Timestamp:\s*(\d+)\s*us', message, re.MULTILINE)
    if match:
        return int(match.group(1))
    return None


def compute_owd(esp_timestamp):
    global clock_offset_us, clock_synced, server_start_us
    
    if not clock_synced:
        return None
    
    # Текущее ОТНОСИТЕЛЬНОЕ время сервера
    current_server_relative = int(time.time_ns() // 1000) - server_start_us
    
    # Время ESP32 в системе координат сервера
    esp_time_in_server_frame = esp_timestamp + clock_offset_us
    
    owd = current_server_relative - esp_time_in_server_frame
    
    return float(owd)


def process_packet(message, stats):
    with stats_lock:
        stats.total_received += 1
        
        sent = extract_total_sent(message)
        if sent is not None:
            stats.total_sent = sent
        
        esp_timestamp = extract_timestamp(message)
        
        owd = None
        if esp_timestamp is not None:
            owd = compute_owd(esp_timestamp)
            if owd is not None and owd >= 0:
                stats.delays_us.append(owd)
        
        print(f"\n--- [{stats.protocol}] Пакет #{stats.total_received} ---")
        print(message)
        print(f"  Total sent: {sent}, Timestamp: {esp_timestamp} us")
        if owd is not None:
            print(f"  Задержка (OWD): {owd:.1f} мкс ({owd/1000:.3f} мс)")
        else:
            print(f"  Задержка (OWD): Н/Д (нет синхронизации)")
        print(f"  Всего получено {stats.protocol}: {stats.total_received}")
        print("---")


def handle_tcp_client(client_socket, client_address):
    print(f"\n[TCP] === Подключен: {client_address[0]}:{client_address[1]} ===\n")
    
    buffer = ""
    delimiter = "===========================\n"
    
    try:
        while running:
            data = client_socket.recv(4096)
            if not data:
                break
            
            buffer += data.decode('utf-8')
            messages = buffer.split(delimiter)
            buffer = messages[-1]
            
            for message in messages[:-1]:
                if message.strip():
                    process_packet(message, tcp_stats)
                    
    except Exception as e:
        print(f"[TCP] Ошибка при обработке {client_address}: {e}")
    finally:
        client_socket.close()
        print(f"[TCP] === Отключен: {client_address[0]}:{client_address[1]} ===\n")


def tcp_server():
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_socket.bind((HOST, TCP_PORT))
    tcp_socket.listen(5)
    tcp_socket.settimeout(1.0)
    
    print(f"[TCP] Сервер запущен на {HOST}:{TCP_PORT}")
    
    while running:
        try:
            client_socket, client_address = tcp_socket.accept()
            client_thread = threading.Thread(
                target=handle_tcp_client,
                args=(client_socket, client_address),
                daemon=True
            )
            client_thread.start()
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                print(f"[TCP] Ошибка: {e}")
            break
    
    tcp_socket.close()


def udp_server():
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_socket.bind((HOST, UDP_PORT))
    udp_socket.settimeout(1.0)
    
    print(f"[UDP] Сервер запущен на {HOST}:{UDP_PORT}")
    
    while running:
        try:
            data, client_address = udp_socket.recvfrom(4096)
            if data:
                message = data.decode('utf-8').strip()
                process_packet(message, udp_stats)
                
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                print(f"[UDP] Ошибка: {e}")
            break
    
    udp_socket.close()


def sync_server():
    global clock_offset_us, clock_synced, server_start_us
    
    sync_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sync_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sync_socket.bind((HOST, SYNC_PORT))
    sync_socket.settimeout(1.0)
    
    sync_history = {}
    
    print(f"[SYNC] Сервер синхронизации запущен на {HOST}:{SYNC_PORT}")
    
    while running:
        try:
            data, client_address = sync_socket.recvfrom(128)
            if data:
                message = data.decode('utf-8').strip()
                
                if message.startswith("SYNC:"):
                    try:
                        T_esp_send = int(message.split(":")[1])
                    except:
                        continue
                    
                    T_srv_relative = int(time.time_ns() // 1000) - server_start_us
                    sync_history[T_esp_send] = T_srv_relative
                    
                    response = str(T_srv_relative).encode('utf-8')
                    sync_socket.sendto(response, client_address)
                
                elif message.startswith("OFFSET_DATA:"):
                    try:
                        payload = message.split(":")[1]
                        parts = payload.split(",")
                        T_esp_send = int(parts[0])
                        OWD = int(parts[1])
                        
                        if T_esp_send in sync_history:
                            T_srv_relative = sync_history[T_esp_send]
                            clock_offset_us = T_srv_relative - (T_esp_send + OWD)
                            clock_synced = True
                            print(f"\n[SYNC] Offset вычислен: {clock_offset_us} us")
                            print(f"  T_esp_send = {T_esp_send}")
                            print(f"  T_srv_rel  = {T_srv_relative}")
                            print(f"  OWD        = {OWD}")
                            print(f"  Offset = {T_srv_relative} - ({T_esp_send} + {OWD}) = {clock_offset_us}")
                            print(f"[SYNC] Синхронизация установлена!\n")
                        else:
                            print(f"\n[SYNC] Ошибка: T_esp_send={T_esp_send} не найден в истории\n")
                    except Exception as e:
                        print(f"\n[SYNC] Ошибка разбора OFFSET_DATA: {e}\n")
                    
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                print(f"[SYNC] Ошибка: {e}")
            break
    
    sync_socket.close()


def print_final_stats(stats):
    print(f"\n{'='*60}")
    print(f"  ИТОГОВАЯ СТАТИСТИКА — {stats.protocol}")
    print(f"{'='*60}")
    print(f"  Всего получено пакетов:  {stats.total_received}")
    print(f"  Всего отправлено (из Total sent): {stats.total_sent}")
    
    if stats.total_sent > 0:
        loss_percent = ((stats.total_sent - stats.total_received) / stats.total_sent) * 100
        print(f"  Потери (Packet Loss):    {loss_percent:.2f}%")
    else:
        print(f"  Потери (Packet Loss):    Н/Д")
    
    if stats.delays_us:
        delays = stats.delays_us
        avg_us = sum(delays) / len(delays)
        min_us = min(delays)
        max_us = max(delays)
        print(f"  Замеров задержки:        {len(delays)}")
        print(f"  Средняя задержка (OWD):  {avg_us:.1f} мкс ({avg_us/1000:.3f} мс)")
        print(f"  Минимальная задержка:    {min_us:.1f} мкс ({min_us/1000:.3f} мс)")
        print(f"  Максимальная задержка:   {max_us:.1f} мкс ({max_us/1000:.3f} мс)")
    else:
        print(f"  Задержка:                Н/Д (нет данных или нет синхронизации)")
    
    print(f"{'='*60}\n")


def signal_handler(sig, frame):
    global running
    print("\n\n[!] Ctrl+C — завершаем работу, ждите итоговую статистику...")
    running = False


def main():
    global running, server_start_us
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Фиксируем время старта сервера
    server_start_us = int(time.time_ns() // 1000)
    
    print("=" * 50)
    print("ЗАПУСК СЕРВЕРОВ (UDP + TCP + SYNC)")
    print("=" * 50)
    print(f"TCP порт:  {TCP_PORT}")
    print(f"UDP порт:  {UDP_PORT}")
    print(f"SYNC порт: {SYNC_PORT}")
    print("Все принятые пакеты выводятся в консоль.")
    print("Нажмите Ctrl+C для остановки и вывода итоговой статистики...\n")
    
    tcp_thread = threading.Thread(target=tcp_server, daemon=True)
    udp_thread = threading.Thread(target=udp_server, daemon=True)
    sync_thread = threading.Thread(target=sync_server, daemon=True)
    
    tcp_thread.start()
    udp_thread.start()
    sync_thread.start()
    
    try:
        while running:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    
    running = False
    time.sleep(1.5)
    
    print("\n\n" + "=" * 60)
    print("  СТАТИСТИКА ПО ЗАВЕРШЕНИИ РАБОТЫ")
    print("=" * 60)
    
    print_final_stats(udp_stats)
    print_final_stats(tcp_stats)
    
    print("Сервер остановлен.")
    sys.exit(0)


if __name__ == "__main__":
    main()
