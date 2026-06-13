#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClient.h>
#include <WiFiUdp.h>

const int ConnectionType = 1;  // 1=UDP, 2=TCP

const char* ssid = "TP-LINK_0E36"; // ssid вашей сети
const char* password = "password"; // пароль вашей сети
const char* serverIP = "0.0.0.0";  // IP адрес вашего сервера

const int serverTCPPort = 8080;
const int serverUDPPort = 8081;
const int SYNC_PORT = 8082;

WiFiClient tcpClient;
WiFiUDP udpClient;

unsigned long lastSendTime = 0;
const long sendInterval = 1;
unsigned long lastRSSITime = 0;
const long rssiInterval = 5000;

bool firstPacketSent = false;
unsigned long firstPacketTime = 0;

unsigned int UDPPacketCounter = 0;
unsigned int TCPPacketCounter = 0;

bool clockSynced = false;

// ===== ПРОФАЙЛИНГ =====
const int PROFILE_WINDOW = 10000;
unsigned long profilePacketCount = 0;

unsigned long long udpStringAccum = 0;
unsigned long long udpBeginAccum = 0;
unsigned long long udpPrintAccum = 0;
unsigned long long udpEndAccum = 0;

unsigned long long tcpStringAccum = 0;
unsigned long long tcpPrintAccum = 0;
unsigned long long tcpFlushAccum = 0;
unsigned long long tcpCheckAccum = 0;

unsigned long lastUdpStringTime = 0;
unsigned long lastUdpBeginTime = 0;
unsigned long lastUdpPrintTime = 0;
unsigned long lastUdpEndTime = 0;

unsigned long lastTcpCheckTime = 0;
unsigned long lastTcpStringTime = 0;
unsigned long lastTcpPrintTime = 0;
unsigned long lastTcpFlushTime = 0;
// ======================

char packetBuffer[1300];

String getProtocolName() {
    if (ConnectionType == 1) return "UDP";
    if (ConnectionType == 2) return "TCP";
    return "UNKNOWN";
}

void printSignalQuality(int rssi) {
    Serial.print("RSSI quality: ");
    if (rssi > -50) Serial.println("EXCELLENT");
    else if (rssi > -60) Serial.println("GOOD");
    else if (rssi > -70) Serial.println("AVERAGE");
    else if (rssi > -80) Serial.println("POOR");
    else Serial.println("VERY_POOR");
}

void setupWiFi() {
    Serial.println("\n=== WiFi Connect ===");
    Serial.print("Connecting to: ");
    Serial.println(ssid);
    
    WiFi.begin(ssid, password);
    
    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 20) {
        delay(500);
        Serial.print(".");
        attempts++;
        digitalWrite(2, !digitalRead(2));
    }
    
    if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\nConnected!");
        Serial.print("IP: ");
        Serial.println(WiFi.localIP());
        Serial.print("RSSI: ");
        Serial.print(WiFi.RSSI());
        Serial.println(" dBm");
        printSignalQuality(WiFi.RSSI());
        digitalWrite(2, HIGH);
    } else {
        Serial.println("\nWiFi FAILED!");
        for(int i = 0; i < 10; i++) {
            digitalWrite(2, !digitalRead(2));
            delay(200);
        }
    }
}

// ===== КАЛИБРОВКА ЧАСОВ =====
bool synchronizeClock() {
    const int CALIBRATION_PACKETS = 100;
    const int CALIBRATION_INTERVAL_MS = 5;
    
    WiFiUDP syncUdp;
    
    Serial.println("\n=== Clock Calibration (100 packets) ===");
    Serial.print("Sending ");
    Serial.print(CALIBRATION_PACKETS);
    Serial.println(" sync packets...");
    
    long long minRTT = 0x7FFFFFFFFFFFFFFF;
    unsigned long best_T_esp_send = 0;
    int successful = 0;
    
    for (int i = 0; i < CALIBRATION_PACKETS; i++) {
        unsigned long T_esp_send = micros() - firstPacketTime;
        
        syncUdp.beginPacket(serverIP, SYNC_PORT);
        syncUdp.print("SYNC:" + String(T_esp_send));
        syncUdp.endPacket();
        
        unsigned long timeout = micros() + 200000;
        while (micros() < timeout) {
            int packetSize = syncUdp.parsePacket();
            if (packetSize) {
                char buf[64];
                int len = syncUdp.read(buf, 63);
                buf[len] = '\0';
                
                unsigned long T_esp_recv = micros() - firstPacketTime;
                long long RTT = T_esp_recv - T_esp_send;
                
                if (RTT > 0 && RTT < minRTT) {
                    minRTT = RTT;
                    best_T_esp_send = T_esp_send;
                }
                successful++;
                break;
            }
        }
        
        delay(CALIBRATION_INTERVAL_MS);
    }
    
    if (successful == 0) {
        Serial.println("FAILED — no responses");
        syncUdp.stop();
        return false;
    }
    
    long long OWD = minRTT / 2;
    
    Serial.println("Calibration complete");
    Serial.print("  Successful responses: "); Serial.println(successful);
    Serial.print("  Min RTT: "); Serial.print((long)minRTT); Serial.println(" us");
    Serial.print("  OWD (est): "); Serial.print((long)OWD); Serial.println(" us");
    Serial.print("  Best T_esp_send: "); Serial.println(best_T_esp_send);
    
    // Отправляем данные на SYNC_PORT — сервер сам вычислит Offset
    syncUdp.beginPacket(serverIP, SYNC_PORT);
    syncUdp.print("OFFSET_DATA:" + String(best_T_esp_send) + "," + String((long)OWD));
    syncUdp.endPacket();
    Serial.println("  Offset data sent to server");
    Serial.println("=============================\n");
    
    syncUdp.stop();
    return true;
}

// ===== ОТПРАВКА UDP =====
void sendUDPPacket() {
    unsigned long t_start, t_before_begin, t_before_print, t_before_end;
    
    int rssi = WiFi.RSSI();
    
    if (!firstPacketSent) {
        firstPacketSent = true;
        firstPacketTime = micros();
        lastSendTime = millis();
        lastRSSITime = millis();
    }

    UDPPacketCounter++;
    unsigned long timestamp = micros() - firstPacketTime;
    
    t_start = micros();
    
    int len = snprintf(packetBuffer, sizeof(packetBuffer),
        "=== ESP32 DATA PACKET ===\n"
        "Protocol: UDP\n"
        "Time: %lu s\n"
        "Timestamp: %lu us\n"
        "IP: %s\n"
        "RSSI: %d dBm\n"
        "PacketID: %lu\n"
        "Total sent: %u\n"
        "===========================\n",
        timestamp / 1000000,
        timestamp,
        WiFi.localIP().toString().c_str(),
        rssi,
        timestamp,
        UDPPacketCounter
    );
    
    int paddingNeeded = 1200 - len;

    //paddingNeeded = 0;

    if (paddingNeeded > 0) {
        memmove(packetBuffer + paddingNeeded, packetBuffer, len);
        memset(packetBuffer, 'A', paddingNeeded);
    }
    
    lastUdpStringTime = micros() - t_start;
    
    t_before_begin = micros();
    udpClient.beginPacket(serverIP, serverUDPPort);
    lastUdpBeginTime = micros() - t_before_begin;
    
    t_before_print = micros();
    udpClient.print(packetBuffer);
    lastUdpPrintTime = micros() - t_before_print;
    
    t_before_end = micros();
    udpClient.endPacket();
    lastUdpEndTime = micros() - t_before_end;
    
    if (profilePacketCount < PROFILE_WINDOW) {
        udpStringAccum += lastUdpStringTime;
        udpBeginAccum += lastUdpBeginTime;
        udpPrintAccum += lastUdpPrintTime;
        udpEndAccum += lastUdpEndTime;
    }
}

void sendTCPPacket() {
    static bool tcpConnected = false;
    
    unsigned long t_start, t_before_print, t_before_flush;
    
    unsigned long t_check_start = micros();
    
    if (!tcpConnected) {
        if (!tcpClient.connect(serverIP, serverTCPPort)) {
            Serial.println("TCP connect FAILED");
            lastTcpCheckTime = micros() - t_check_start;
            return;
        }
        tcpConnected = true;
        tcpClient.setNoDelay(false);
        Serial.println("TCP connected");
    }

    static int checkCounter = 0;
    if (++checkCounter >= 1000) {
        checkCounter = 0;
        if (!tcpClient.connected()) {
            tcpConnected = false;
            Serial.println("TCP lost, reconnecting...");
            lastTcpCheckTime = micros() - t_check_start;
            return;
        }
    }
    
    lastTcpCheckTime = micros() - t_check_start;
    
    int rssi = WiFi.RSSI();
    TCPPacketCounter++;
    
    if (!firstPacketSent) {
        firstPacketSent = true;
        firstPacketTime = micros();
        lastSendTime = millis();
        lastRSSITime = millis();
    }
    
    unsigned long timestamp = micros() - firstPacketTime;
    
    t_start = micros();
    
    int len = snprintf(packetBuffer, sizeof(packetBuffer),
        "=== ESP32 DATA PACKET ===\n"
        "Protocol: TCP\n"
        "Time: %lu s\n"
        "Timestamp: %lu us\n"
        "IP: %s\n"
        "RSSI: %d dBm\n"
        "PacketID: %lu\n"
        "Total sent: %u\n"
        "===========================\n",
        timestamp / 1000000,
        timestamp,
        WiFi.localIP().toString().c_str(),
        rssi,
        timestamp,
        TCPPacketCounter
    );
    
    int paddingNeeded = 1200 - len;

    //paddingNeeded = 0;

    if (paddingNeeded > 0) {
        memmove(packetBuffer + paddingNeeded, packetBuffer, len);
        memset(packetBuffer, 'A', paddingNeeded);
    }

    lastTcpStringTime = micros() - t_start;
    
    t_before_print = micros();
    tcpClient.print(packetBuffer);
    lastTcpPrintTime = micros() - t_before_print;
    
    t_before_flush = micros();
    tcpClient.flush();
    lastTcpFlushTime = micros() - t_before_flush;
    
    if (profilePacketCount < PROFILE_WINDOW) {
        tcpStringAccum += lastTcpStringTime;
        tcpCheckAccum += lastTcpCheckTime;
        tcpPrintAccum += lastTcpPrintTime;
        tcpFlushAccum += lastTcpFlushTime;
    }
}

void sendPacket() {
    profilePacketCount++;
    
    if (ConnectionType == 1) {
        sendUDPPacket();
    } else if (ConnectionType == 2) {
        sendTCPPacket();
    }
    
    if (profilePacketCount >= PROFILE_WINDOW) {
        profilePacketCount = 0;
        
        Serial.println("\n========== PROFILE STATS (avg over 10000 packets) ==========");
        
        if (ConnectionType == 1) {
            Serial.println("--- UDP ---");
            Serial.print("UDP String build: ");
            Serial.print((unsigned long)(udpStringAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            Serial.print("UDP beginPacket:  ");
            Serial.print((unsigned long)(udpBeginAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            Serial.print("UDP print:        ");
            Serial.print((unsigned long)(udpPrintAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            Serial.print("UDP endPacket:    ");
            Serial.print((unsigned long)(udpEndAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            unsigned long udpTotal = (udpStringAccum + udpBeginAccum + 
                                     udpPrintAccum + udpEndAccum) / PROFILE_WINDOW;
            Serial.print("UDP TOTAL:        ");
            Serial.print(udpTotal);
            Serial.println(" us");
            
        } else if (ConnectionType == 2) {
            Serial.println("--- TCP ---");
            Serial.print("TCP check conn:    ");
            Serial.print((unsigned long)(tcpCheckAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            Serial.print("TCP String build:  ");
            Serial.print((unsigned long)(tcpStringAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            Serial.print("TCP print:         ");
            Serial.print((unsigned long)(tcpPrintAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            Serial.print("TCP flush:         ");
            Serial.print((unsigned long)(tcpFlushAccum / PROFILE_WINDOW));
            Serial.println(" us");
            
            unsigned long tcpTotal = (tcpCheckAccum + tcpStringAccum + 
                                     tcpPrintAccum + tcpFlushAccum) / PROFILE_WINDOW;
            Serial.print("TCP TOTAL:         ");
            Serial.print(tcpTotal);
            Serial.println(" us");
        }
        
        Serial.println("============================================================\n");
        
        udpStringAccum = udpBeginAccum = udpPrintAccum = udpEndAccum = 0;
        tcpCheckAccum = tcpStringAccum = tcpPrintAccum = tcpFlushAccum = 0;
    }
}

void showRSSIInfo() {
    int rssi = WiFi.RSSI();
    
    Serial.println("----------------------------------------");
    Serial.print("RSSI: ");
    Serial.print(rssi);
    Serial.println(" dBm");
    printSignalQuality(rssi);
    Serial.print("IP: ");
    Serial.println(WiFi.localIP());
    Serial.print("Protocol: ");
    Serial.println(getProtocolName());
    Serial.print("Packets sent (UDP): ");
    Serial.println(UDPPacketCounter);
    Serial.print("Packets sent (TCP): ");
    Serial.println(TCPPacketCounter);
    Serial.print("Clock synced: ");
    Serial.println(clockSynced ? "YES" : "NO");
    Serial.println("----------------------------------------");
}

void setup() {
    Serial.begin(115200);
    delay(1000);

    pinMode(2, OUTPUT);
    digitalWrite(2, LOW);
    
    Serial.println("\n=== PROTOCOL CONFIG ===");
    Serial.print("Type: ");
    if (ConnectionType == 1) Serial.println("UDP");
    else if (ConnectionType == 2) Serial.println("TCP");
    Serial.println("========================\n");
    
    setupWiFi();
    
    if (WiFi.status() == WL_CONNECTED) {
        firstPacketSent = true;
        firstPacketTime = micros();
        
        if (synchronizeClock()) {
            clockSynced = true;
            Serial.println("Clock sync SUCCESS");
        } else {
            Serial.println("Clock sync FAILED");
        }
        
        Serial.println("\nSystem ready");
        Serial.print("Target: ");
        Serial.print(serverIP);
        Serial.print(":");
        Serial.println(ConnectionType == 1 ? serverUDPPort : serverTCPPort);
        Serial.print("Interval: ");
        Serial.print(sendInterval);
        Serial.println(" ms");
        Serial.print("Profile window: ");
        Serial.print(PROFILE_WINDOW);
        Serial.println(" packets\n");
        
        sendPacket();
    }
}

void loop() {
    if (WiFi.status() == WL_CONNECTED) {
        if (millis() - lastSendTime >= sendInterval) {
            lastSendTime = millis();
            sendPacket();
        }
        
        if (millis() - lastRSSITime >= rssiInterval) {
            lastRSSITime = millis();
            showRSSIInfo();
        }
    } else {
        Serial.println("WiFi lost! Reconnecting...");
        digitalWrite(2, LOW);
        setupWiFi();
        delay(1000);
    }
}
