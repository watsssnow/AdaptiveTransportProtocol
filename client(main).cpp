#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClient.h>
#include <WiFiUdp.h>

// ---- конфигурация сети ----
const char* ssid = "ssid"; // ssid вашей Wi-Fi сети
const char* password = "password"; // пароль вашей Wi-Fi сети
const char* serverIP = "0.0.0.0"; // IP вашего сервера

// ---- порты логических каналов ----
const int TCP_SERVICE_PORT = 8080;
const int UDP_SYNC_PORT = 8082;
const int UDP_TELEMETRY_PORT = 8081;
const int UDP_ADAPTIVELOSS_PORT = 8083;
const int TCP_ADAPTIVELOSS_PORT = 8084;
const int UDP_ADAPTIVEDELAY_PORT = 8085;
const int TCP_ADAPTIVEDELAY_PORT = 8086;

// ---- сокеты ----
WiFiClient tcpService;
WiFiUDP udpSync;
WiFiUDP udpTelemetry;

// AdaptiveLoss (старт в UDP, переход в TCP при высоких потерях)
WiFiUDP udpAdaptiveLoss;
WiFiClient tcpAdaptiveLoss;
bool adaptiveLossUseTCP = false;
bool adaptiveLossTCPConnected = false;
unsigned int adaptiveLossCounter = 0;
unsigned long lastAdaptiveLossMs = 0;

// AdaptiveDelay (старт в UDP, переход в TCP при высокой задержке)
WiFiUDP udpAdaptiveDelay;
WiFiClient tcpAdaptiveDelay;
bool adaptiveDelayUseTCP = false;            // старт в UDP
bool adaptiveDelayTCPConnected = false;
unsigned int adaptiveDelayCounter = 0;
unsigned long lastAdaptiveDelayMs = 0;

// ---- счётчики ----
unsigned int telemetryCounter = 0;
unsigned long lastTelemetryMs = 0;

// ---- тайминги (мс) ----
unsigned long lastServiceMs = 0;
const unsigned long SERVICE_INTERVAL = 10000;

// ---- синхронизация ----
bool clockSynced = false;
unsigned long lastSyncMs = 0;
const unsigned long SYNC_INTERVAL = 30000;

// ---- буфер пакета ----
char pkt[1300];

// ====================== WiFi ======================
void setupWiFi() {
    Serial.println("\n=== WiFi Connect ===");
    WiFi.begin(ssid, password);
    int a = 0;
    while (WiFi.status() != WL_CONNECTED && a < 20) {
        delay(500); Serial.print("."); a++;
        digitalWrite(2, !digitalRead(2));
    }
    if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\nConnected! IP: " + WiFi.localIP().toString());
        digitalWrite(2, HIGH);
    } else {
        Serial.println("\nWiFi FAILED!");
    }
}

// ====================== синхронизация ======================
void runSync() {
    const int N = 100;
    const int INTERVAL = 5;

    Serial.println("\n=== Clock Sync (100 packets) ===");

    long long minRTT = 0x7FFFFFFFFFFFFFFF;
    unsigned long bestTesp = 0;
    int ok = 0;

    for (int i = 0; i < N; i++) {
        unsigned long tSend = micros();
        udpSync.beginPacket(serverIP, UDP_SYNC_PORT);
        udpSync.print("SYNC:" + String(tSend));
        udpSync.endPacket();

        unsigned long to = micros() + 10000;
        while (micros() < to) {
            if (udpSync.parsePacket()) {
                char buf[64];
                int len = udpSync.read(buf, 63);
                buf[len] = 0;
                unsigned long tRecv = micros();
                long long rtt = tRecv - tSend;
                if (rtt > 0 && rtt < minRTT) { minRTT = rtt; bestTesp = tSend; }
                ok++;
                break;
            }
        }
        delay(INTERVAL);
    }

    if (ok == 0) { Serial.println("Sync FAILED"); return; }

    long long owd = minRTT / 2;
    udpSync.beginPacket(serverIP, UDP_SYNC_PORT);
    udpSync.print("OFFSET_DATA:" + String(bestTesp) + "," + String((long)owd));
    udpSync.endPacket();
    clockSynced = true;
    Serial.println("Sync OK, minRTT=" + String((long)minRTT) + " us, OWD=" + String((long)owd) + " us");
}

// ====================== служебный TCP ======================
void ensureServiceTCP() {
    static bool conn = false;
    if (conn && tcpService.connected()) return;
    if (tcpService.connect(serverIP, TCP_SERVICE_PORT)) {
        conn = true;
        tcpService.setNoDelay(true);
        Serial.println("Service TCP connected");
    }
}

void handleServiceIn() {
    if (!tcpService.connected()) return;
    while (tcpService.available()) {
        String line = tcpService.readStringUntil('\n');
        line.trim();
        if (line.length() == 0) continue;
        Serial.print("[SRV] "); Serial.println(line);
        if (line.indexOf("SWITCH_ADAPTIVELOSS_TO_TCP") >= 0) {
            adaptiveLossUseTCP = true;
            Serial.println("  -> AdaptiveLoss switched to TCP");
        } else if (line.indexOf("SWITCH_ADAPTIVELOSS_TO_UDP") >= 0) {
            adaptiveLossUseTCP = false;
            Serial.println("  -> AdaptiveLoss switched to UDP");
        } else if (line.indexOf("SWITCH_ADAPTIVEDELAY_TO_TCP") >= 0) {
            adaptiveDelayUseTCP = true;
            Serial.println("  -> AdaptiveDelay switched to TCP");
        } else if (line.indexOf("SWITCH_ADAPTIVEDELAY_TO_UDP") >= 0) {
            adaptiveDelayUseTCP = false;
            Serial.println("  -> AdaptiveDelay switched to UDP");
        }
    }
}

void sendServiceStatus() {
    ensureServiceTCP();
    if (!tcpService.connected()) return;
    String st = "STATUS:";
    st += "telemetry:" + String(telemetryCounter);
    st += ":adaptiveLoss:" + String(adaptiveLossCounter);
    st += ":adaptiveDelay:" + String(adaptiveDelayCounter);
    st += ":rssi:" + String(WiFi.RSSI());
    st += ":lossProto:" + String(adaptiveLossUseTCP ? "TCP" : "UDP");
    st += ":delayProto:" + String(adaptiveDelayUseTCP ? "TCP" : "UDP");
    String full = "=== SERVICE ===\nTimestamp: " + String(micros()) + " us\n" + st + "\n===========================\n";
    tcpService.print(full);
    tcpService.flush();
}

// ====================== Телеметрия UDP ======================
void sendTelemetry() {
    telemetryCounter++;
    unsigned long ts = micros();
    int rssi = WiFi.RSSI();

    int len = snprintf(pkt, sizeof(pkt),
        "=== TELEMETRY ===\nProtocol: UDP\nTime: %lu s\nTimestamp: %lu us\n"
        "IP: %s\nRSSI: %d dBm\nPacketID: %lu\nTotal sent: %u\n"
        "===========================\n",
        ts / 1000000, ts,
        WiFi.localIP().toString().c_str(), rssi, ts, telemetryCounter);

    udpTelemetry.beginPacket(serverIP, UDP_TELEMETRY_PORT);
    udpTelemetry.print(pkt);
    udpTelemetry.endPacket();
}

// ====================== AdaptiveLoss ======================
void ensureAdaptiveLossTCP() {
    if (adaptiveLossTCPConnected && tcpAdaptiveLoss.connected()) return;
    if (tcpAdaptiveLoss.connect(serverIP, TCP_ADAPTIVELOSS_PORT)) {
        adaptiveLossTCPConnected = true;
        tcpAdaptiveLoss.setNoDelay(true);
        Serial.println("AdaptiveLoss TCP connected");
    }
}

void sendAdaptiveLoss() {
    adaptiveLossCounter++;
    unsigned long ts = micros();
    int rssi = WiFi.RSSI();

    int len = snprintf(pkt, sizeof(pkt),
        "=== ADAPTIVE_LOSS ===\nProtocol: %s\nTime: %lu s\nTimestamp: %lu us\n"
        "IP: %s\nRSSI: %d dBm\nPacketID: %lu\nTotal sent: %u\n"
        "===========================\n",
        adaptiveLossUseTCP ? "TCP" : "UDP",
        ts / 1000000, ts,
        WiFi.localIP().toString().c_str(), rssi, ts, adaptiveLossCounter);

    if (adaptiveLossUseTCP) {
        ensureAdaptiveLossTCP();
        if (tcpAdaptiveLoss.connected()) {
            tcpAdaptiveLoss.print(pkt);
            tcpAdaptiveLoss.flush();
        }
    } else {
        udpAdaptiveLoss.beginPacket(serverIP, UDP_ADAPTIVELOSS_PORT);
        udpAdaptiveLoss.print(pkt);
        udpAdaptiveLoss.endPacket();
    }
}

// ====================== AdaptiveDelay (UDP -> TCP) ======================
void ensureAdaptiveDelayTCP() {
    if (adaptiveDelayTCPConnected && tcpAdaptiveDelay.connected()) return;
    if (tcpAdaptiveDelay.connect(serverIP, TCP_ADAPTIVEDELAY_PORT)) {
        adaptiveDelayTCPConnected = true;
        tcpAdaptiveDelay.setNoDelay(true);
        Serial.println("AdaptiveDelay TCP connected");
    }
}

void sendAdaptiveDelay() {
    adaptiveDelayCounter++;
    unsigned long ts = micros();
    int rssi = WiFi.RSSI();

    int len = snprintf(pkt, sizeof(pkt),
        "=== ADAPTIVE_DELAY ===\n"
        "Protocol: %s\n"
        "Time: %lu s\n"
        "Timestamp: %lu us\n"
        "IP: %s\n"
        "RSSI: %d dBm\n"
        "PacketID: %lu\n"
        "Total sent: %u\n"
        "===========================\n",
        adaptiveDelayUseTCP ? "TCP" : "UDP",
        ts / 1000000, ts,
        WiFi.localIP().toString().c_str(), rssi, ts, adaptiveDelayCounter);

    if (adaptiveDelayUseTCP) {
        ensureAdaptiveDelayTCP();
        if (tcpAdaptiveDelay.connected()) {
            tcpAdaptiveDelay.print(pkt);
            tcpAdaptiveDelay.flush();
        }
    } else {
        udpAdaptiveDelay.beginPacket(serverIP, UDP_ADAPTIVEDELAY_PORT);
        udpAdaptiveDelay.print(pkt);
        udpAdaptiveDelay.endPacket();
    }
}

// ====================== setup / loop ======================
void setup() {
    Serial.begin(115200);
    delay(1000);
    pinMode(2, OUTPUT); digitalWrite(2, LOW);

    Serial.println("\n=== MULTI-CHANNEL + TELEMETRY + ADAPTIVE ===");
    Serial.println("Service:       TCP " + String(TCP_SERVICE_PORT));
    Serial.println("Sync:          UDP " + String(UDP_SYNC_PORT));
    Serial.println("Telemetry:     UDP " + String(UDP_TELEMETRY_PORT));
    Serial.println("AdaptiveLoss:  UDP " + String(UDP_ADAPTIVELOSS_PORT) + " / TCP " + String(TCP_ADAPTIVELOSS_PORT) + " (inactive)");
    Serial.println("AdaptiveDelay: UDP " + String(UDP_ADAPTIVEDELAY_PORT) + " / TCP " + String(TCP_ADAPTIVEDELAY_PORT));
    Serial.println("=====================================\n");

    setupWiFi();
    if (WiFi.status() != WL_CONNECTED) return;

    ensureServiceTCP();
    runSync();

    Serial.println("System ready\n");
}

void loop() {
    if (WiFi.status() != WL_CONNECTED) {
        digitalWrite(2, LOW);
        setupWiFi();
        delay(1000);
        return;
    }

    ensureServiceTCP();
    handleServiceIn();

    unsigned long now = millis();

    if (now - lastSyncMs >= SYNC_INTERVAL) {
        lastSyncMs = now;
        runSync();
        handleServiceIn();
    }

    // Обычная телеметрия UDP
    //if (clockSynced && now - lastTelemetryMs >= 1) {
    //    lastTelemetryMs = now;
    //    sendTelemetry();
    //}

    // AdaptiveLoss
    //if (clockSynced && now - lastAdaptiveLossMs >= 1) {
    //    lastAdaptiveLossMs = now;
    //    sendAdaptiveLoss();
    //}

    // AdaptiveDelay
    if (clockSynced && now - lastAdaptiveDelayMs >= 1) {
        lastAdaptiveDelayMs = now;
        sendAdaptiveDelay();
    }

    if (now - lastServiceMs >= SERVICE_INTERVAL) {
        lastServiceMs = now;
        sendServiceStatus();
    }
}
