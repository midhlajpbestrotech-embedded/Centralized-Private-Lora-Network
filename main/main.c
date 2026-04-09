/*
 * =============================================================================
 * Estrotech — Centralized Private LoRa Sensor Network
 * Main Gateway — SENSOR-COMPATIBLE VERSION (Inline HTML)
 * =============================================================================
 *
 * Owner        : Midhlaj
 * Framework    : ESP-IDF v5.5.2
 * Board        : ESP32 DevKit V1 38-pin (ESP32-WROOM-32)
 * LoRa Module  : AI-Thinker Ra-02 SX1278
 *
 * BUILD FIX: Uses inline HTML string (no external files needed)
 *
 * SENSOR COMPATIBILITY UPDATES:
 *   ✓ Handles sensor-specific sentinel values:
 *     - Temperature (Node 0x01, 0x04): 0x8000 = DHT11 failure
 *     - Humidity (Node 0x02, 0x05):    0x0000 = DHT11 failure
 *     - Light (Node 0x03, 0x06):       0x0000 = valid stub (no hardware)
 *   ✓ JSON output includes "sensor_error" flag for failed readings
 *   ✓ Proper decoding of sensor values with failure detection
 *
 * SYNCHRONIZATION FIXES:
 *   ✓ Timestamp validation removed (clock-independent operation)
 *   ✓ Offline timeout: 90 seconds (allows 2 missed cycles)
 *   ✓ Queue depth: 8 (matches Local Gateway)
 *   ✓ Packet ID tracking for gap detection
 *
 * =============================================================================
 */

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <math.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"
#include "driver/spi_master.h"
#include "driver/gpio.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "nvs_flash.h"
#include "esp_netif.h"
#include "esp_http_server.h"
#include "sdkconfig.h"

/* ============================================================
 * SECTION 1 — PIN DEFINITIONS (ESP32 DevKit V1)
 * ============================================================ */
#define PIN_SCK     18
#define PIN_MISO    19
#define PIN_MOSI    23
#define PIN_NSS     5
#define PIN_RST     14
#define PIN_DIO0    26

/* ============================================================
 * SECTION 2 — SX1278 REGISTER ADDRESSES
 * ============================================================ */
#define REG_FIFO                0x00
#define REG_OP_MODE             0x01
#define REG_FRF_MSB             0x06
#define REG_FRF_MID             0x07
#define REG_FRF_LSB             0x08
#define REG_PA_CONFIG           0x09
#define REG_LNA                 0x0C
#define REG_FIFO_ADDR_PTR       0x0D
#define REG_FIFO_TX_BASE_ADDR   0x0E
#define REG_FIFO_RX_BASE_ADDR   0x0F
#define REG_FIFO_RX_CURR_ADDR   0x10
#define REG_IRQ_FLAGS           0x12
#define REG_RX_NB_BYTES         0x13
#define REG_PKT_RSSI_VALUE      0x1A
#define REG_MODEM_CONFIG1       0x1D
#define REG_MODEM_CONFIG2       0x1E
#define REG_PAYLOAD_LENGTH      0x22
#define REG_MODEM_CONFIG3       0x26
#define REG_SYNC_WORD           0x39
#define REG_DIO_MAPPING1        0x40
#define REG_VERSION             0x42

/* Operating modes */
#define MODE_LONG_RANGE_MODE    0x80
#define MODE_SLEEP              0x00
#define MODE_STDBY              0x01
#define MODE_TX                 0x03
#define MODE_RX_CONTINUOUS      0x05

/* IRQ flags */
#define IRQ_TX_DONE_MASK        0x08
#define IRQ_RX_DONE_MASK        0x40
#define IRQ_PAYLOAD_CRC_ERR     0x20

/* ============================================================
 * SECTION 3 — NETWORK CONSTANTS
 * ============================================================ */
#define MAIN_GW_ID              0x00
#define LAYER2_PREAMBLE         0xBB
#define LAYER2_ACK_BYTE         0xAA
#define SYNC_WORD               0x12

/* Layer 2: 434.0 MHz (from Local Gateways) */
#define FREQ_L2_MSB             0x6C
#define FREQ_L2_MID             0x80
#define FREQ_L2_LSB             0x00

#define L2_PACKET_SIZE          35
#define L2_ACK_SIZE             1

#define CYCLE_DURATION_MS       30000
#define OFFLINE_TIMEOUT_MS      90000
#define MISSED_CYCLES_OFFLINE   3
#define TX_WATCHDOG_TIMEOUT_MS  5000

/* Sensor-specific sentinel values */
#define SENTINEL_TEMP_FAIL      0x8000
#define SENTINEL_HUMI_FAIL      0x0000
#define SENTINEL_LIGHT_STUB     0x0000
#define SENTINEL_PACKET_ID      0xFFFF

#define SX1278_VERSION_EXPECTED 0x12

/* WiFi configuration */
#define WIFI_SSID               "LoRa-Gateway"
#define WIFI_PASS               "estrotech"
#define WIFI_MAX_CONN           4

/* ============================================================
 * SECTION 4 — DATA STRUCTURES
 * ============================================================ */
typedef enum {
    SENSOR_TYPE_TEMPERATURE = 1,
    SENSOR_TYPE_HUMIDITY    = 2,
    SENSOR_TYPE_LIGHT       = 3,
    SENSOR_TYPE_UNKNOWN     = 0
} sensor_type_t;

typedef struct {
    uint8_t  node_id;
    uint16_t packet_id;
    uint32_t timestamp;
    uint16_t sensor_value;
    bool     is_sentinel;
    bool     is_sensor_error;
} node_block_t;

typedef struct {
    uint8_t      gw_id;
    uint32_t     gw_timestamp;
    node_block_t blocks[3];
} validated_l2_t;

typedef struct {
    uint8_t  node_id;
    uint8_t  gateway_id;
    bool     online;
    uint32_t timestamp;
    uint32_t gw_timestamp;
    uint32_t last_packet_time_ms;
    uint8_t  missed_cycles;
    
    float    sensor_value_float;
    uint16_t sensor_value_lux;
    bool     is_light_node;
    bool     sensor_error;
} node_data_t;

/* ============================================================
 * SECTION 5 — GLOBAL STATE
 * ============================================================ */
static const char *TAG = "MGW";

static spi_device_handle_t s_spi_handle;
static SemaphoreHandle_t   s_lora_mutex;
static SemaphoreHandle_t   s_l3_packet_id_mutex;

static volatile bool       s_tx_active = false;
static volatile uint32_t   s_tx_start_time = 0;

static QueueHandle_t s_raw_rx_queue;
static QueueHandle_t s_validated_queue;
static QueueHandle_t s_json_trigger;

static node_data_t g_data_store[6];
static SemaphoreHandle_t g_data_store_mutex;

static uint32_t s_last_gw_packet_ms[2] = {0, 0};
static uint32_t s_last_gw_timestamp[2] = {0, 0};

static uint32_t s_l3_packet_id = 0;

static httpd_handle_t s_server = NULL;
static int ws_client_fds[WIFI_MAX_CONN] = {-1, -1, -1, -1};
static SemaphoreHandle_t ws_clients_mutex;
static bool s_dashboard_connected = false;

/* ============================================================
 * SECTION 6 — INLINE HTML DASHBOARD
 * ============================================================ */
static const char *dashboard_html = 
"<!DOCTYPE html>"
"<html>"
"<head>"
"<meta charset='UTF-8'>"
"<meta name='viewport' content='width=device-width,initial-scale=1.0'>"
"<title>Estrotech LoRa Gateway</title>"
"<style>"
"*{margin:0;padding:0;box-sizing:border-box}"
"body{font-family:Arial,sans-serif;background:#f0f2f5;padding:20px}"
".container{max-width:1200px;margin:0 auto}"
"h1{color:#2c3e50;margin-bottom:20px;text-align:center}"
".status{background:#fff;padding:15px;border-radius:8px;margin-bottom:20px;box-shadow:0 2px 4px rgba(0,0,0,0.1)}"
".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px}"
".node-card{background:#fff;padding:20px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1)}"
".node-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:15px}"
".node-title{font-size:18px;font-weight:bold;color:#2c3e50}"
".status-badge{padding:4px 12px;border-radius:12px;font-size:12px;font-weight:bold}"
".online{background:#d4edda;color:#155724}"
".offline{background:#f8d7da;color:#721c24}"
".error{background:#fff3cd;color:#856404}"
".value{font-size:32px;font-weight:bold;color:#3498db;margin:10px 0}"
".unit{font-size:16px;color:#7f8c8d;margin-left:5px}"
".info{font-size:12px;color:#95a5a6;margin-top:10px}"
".ws-status{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:8px}"
".ws-connected{background:#28a745}"
".ws-disconnected{background:#dc3545}"
"</style>"
"</head>"
"<body>"
"<div class='container'>"
"<h1>🌐 Estrotech LoRa Sensor Network</h1>"
"<div class='status'>"
"<span class='ws-status ws-disconnected' id='ws-indicator'></span>"
"<span id='ws-text'>Connecting to WebSocket...</span>"
"<span style='float:right;color:#7f8c8d' id='last-update'>Waiting for data...</span>"
"</div>"
"<div class='grid' id='nodes-grid'></div>"
"</div>"
"<script>"
"let ws;let reconnectTimer;"
"function connectWS(){"
"ws=new WebSocket('ws://'+location.hostname+'/ws');"
"ws.onopen=()=>{"
"document.getElementById('ws-indicator').className='ws-status ws-connected';"
"document.getElementById('ws-text').textContent='Connected';"
"clearTimeout(reconnectTimer);"
"};"
"ws.onmessage=(e)=>{"
"try{"
"const data=JSON.parse(e.data);"
"updateDashboard(data);"
"}catch(err){console.error('Parse error:',err);}"
"};"
"ws.onerror=(e)=>{console.error('WebSocket error:',e);};"
"ws.onclose=()=>{"
"document.getElementById('ws-indicator').className='ws-status ws-disconnected';"
"document.getElementById('ws-text').textContent='Disconnected - Reconnecting...';"
"reconnectTimer=setTimeout(connectWS,3000);"
"};"
"}"
"function updateDashboard(data){"
"const grid=document.getElementById('nodes-grid');"
"grid.innerHTML='';"
"document.getElementById('last-update').textContent='Last update: '+new Date().toLocaleTimeString();"
"data.nodes.forEach(node=>{"
"const card=document.createElement('div');"
"card.className='node-card';"
"let statusClass,statusText;"
"if(!node.online){"
"statusClass='offline';statusText='OFFLINE';"
"}else if(node.sensor_error){"
"statusClass='error';statusText='SENSOR ERROR';"
"}else{"
"statusClass='online';statusText='ONLINE';"
"}"
"let valueDisplay;"
"if(!node.online){"
"valueDisplay='<div class=\"value\" style=\"color:#95a5a6\">---</div>';"
"}else if(node.sensor_error){"
"valueDisplay='<div class=\"value\" style=\"color:#e67e22\">ERROR</div>';"
"}else if(node.sensor_value!==null){"
"if(node.node_id===1||node.node_id===4){"
"valueDisplay='<div class=\"value\">'+node.sensor_value.toFixed(1)+'<span class=\"unit\">°C</span></div>';"
"}else if(node.node_id===2||node.node_id===5){"
"valueDisplay='<div class=\"value\">'+node.sensor_value.toFixed(1)+'<span class=\"unit\">%</span></div>';"
"}else{"
"valueDisplay='<div class=\"value\">'+node.sensor_value+'<span class=\"unit\">lux</span></div>';"
"}"
"}else{"
"valueDisplay='<div class=\"value\" style=\"color:#95a5a6\">null</div>';"
"}"
"let sensorType;"
"if(node.node_id===1||node.node_id===4)sensorType='Temperature';"
"else if(node.node_id===2||node.node_id===5)sensorType='Humidity';"
"else sensorType='Light';"
"card.innerHTML="
"'<div class=\"node-header\">'+"
"'<div class=\"node-title\">Node '+node.node_id+' - '+sensorType+'</div>'+"
"'<div class=\"status-badge '+statusClass+'\">'+statusText+'</div>'+"
"'</div>'+"
"valueDisplay+"
"'<div class=\"info\">Gateway: '+node.gateway_id+'</div>'+"
"(node.timestamp?'<div class=\"info\">Last seen: '+Math.floor((Date.now()-node.timestamp)/1000)+'s ago</div>':'');"
"grid.appendChild(card);"
"});"
"}"
"connectWS();"
"</script>"
"</body>"
"</html>";

/* ============================================================
 * SECTION 7 — SX1278 SPI DRIVER
 * ============================================================ */
static void write_reg(uint8_t addr, uint8_t value)
{
    uint8_t tx[2] = { (uint8_t)(addr | 0x80), value };
    spi_transaction_t t = {
        .length = 16,
        .tx_buffer = tx,
    };
    spi_device_transmit(s_spi_handle, &t);
}

static uint8_t read_reg(uint8_t addr)
{
    uint8_t tx[2] = { (uint8_t)(addr & 0x7F), 0x00 };
    uint8_t rx[2] = { 0 };
    spi_transaction_t t = {
        .length = 16,
        .tx_buffer = tx,
        .rx_buffer = rx,
    };
    spi_device_transmit(s_spi_handle, &t);
    return rx[1];
}

static void read_fifo(uint8_t *data, uint8_t len)
{
    uint8_t tx[L2_PACKET_SIZE + 1];
    uint8_t rx[L2_PACKET_SIZE + 1];
    memset(tx, 0, sizeof(tx));
    tx[0] = REG_FIFO & 0x7F;
    spi_transaction_t t = {
        .length = (size_t)(len + 1) * 8,
        .tx_buffer = tx,
        .rx_buffer = rx,
    };
    spi_device_transmit(s_spi_handle, &t);
    memcpy(data, &rx[1], len);
}

static void write_fifo(const uint8_t *data, uint8_t len)
{
    uint8_t tx[L2_ACK_SIZE + 1];
    tx[0] = REG_FIFO | 0x80;
    memcpy(&tx[1], data, len);
    spi_transaction_t t = {
        .length = (size_t)(len + 1) * 8,
        .tx_buffer = tx,
    };
    spi_device_transmit(s_spi_handle, &t);
}

/* ============================================================
 * SECTION 8 — SX1278 INITIALIZATION
 * ============================================================ */
static void sx1278_init_spi(void)
{
    spi_bus_config_t bus_cfg = {
        .mosi_io_num   = PIN_MOSI,
        .miso_io_num   = PIN_MISO,
        .sclk_io_num   = PIN_SCK,
        .quadwp_io_num = -1,
        .quadhd_io_num = -1,
    };
    ESP_ERROR_CHECK(spi_bus_initialize(SPI2_HOST, &bus_cfg, SPI_DMA_CH_AUTO));

    spi_device_interface_config_t dev_cfg = {
        .clock_speed_hz = 1000000,
        .mode           = 0,
        .spics_io_num   = PIN_NSS,
        .queue_size     = 4,
    };
    ESP_ERROR_CHECK(spi_bus_add_device(SPI2_HOST, &dev_cfg, &s_spi_handle));
}

static void sx1278_reset(void)
{
    gpio_set_direction(PIN_RST, GPIO_MODE_OUTPUT);
    gpio_set_level(PIN_RST, 0);
    vTaskDelay(pdMS_TO_TICKS(10));
    gpio_set_level(PIN_RST, 1);
    vTaskDelay(pdMS_TO_TICKS(10));
}

static void sx1278_sanity_check(void)
{
    uint8_t version = read_reg(REG_VERSION);
    if (version != SX1278_VERSION_EXPECTED) {
        ESP_LOGE(TAG, "FATAL: SX1278 reg 0x42 = 0x%02X, expected 0x12", version);
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }
    ESP_LOGI(TAG, "SX1278 detected — reg 0x42 = 0x12");
}

static void sx1278_configure_radio(void)
{
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_SLEEP);
    vTaskDelay(pdMS_TO_TICKS(10));

    write_reg(REG_FRF_MSB, FREQ_L2_MSB);
    write_reg(REG_FRF_MID, FREQ_L2_MID);
    write_reg(REG_FRF_LSB, FREQ_L2_LSB);

    write_reg(REG_SYNC_WORD,      SYNC_WORD);
    write_reg(REG_MODEM_CONFIG1,  0x72);
    write_reg(REG_MODEM_CONFIG2,  0x74);
    write_reg(REG_MODEM_CONFIG3,  0x04);
    write_reg(REG_PA_CONFIG,      0xF8);
    write_reg(REG_LNA,            0x23);
    write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1,   0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_TX_BASE_ADDR, 0x80);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);

    ESP_LOGI(TAG, "SX1278 configured: 434MHz SF7 BW125 CR4/5");
}

/* ============================================================
 * SECTION 9 — CRC16-CCITT
 * ============================================================ */
static uint16_t crc16_ccitt(const uint8_t *data, uint16_t length)
{
    uint16_t crc = 0xFFFF;
    for (uint16_t i = 0; i < length; i++) {
        crc ^= (uint16_t)data[i] << 8;
        for (uint8_t bit = 0; bit < 8; bit++) {
            crc = (crc & 0x8000)
                  ? (uint16_t)((crc << 1) ^ 0x1021)
                  : (uint16_t)(crc << 1);
        }
    }
    return crc;
}

/* ============================================================
 * SECTION 10 — PARSING HELPERS
 * ============================================================ */
static inline uint16_t parse_u16_be(const uint8_t *buf, uint8_t offset)
{
    return (uint16_t)(((uint16_t)buf[offset] << 8) | buf[offset + 1]);
}

static inline uint32_t parse_u32_be(const uint8_t *buf, uint8_t offset)
{
    return ((uint32_t)buf[offset]     << 24)
         | ((uint32_t)buf[offset + 1] << 16)
         | ((uint32_t)buf[offset + 2] <<  8)
         |  (uint32_t)buf[offset + 3];
}

/* ============================================================
 * SECTION 11 — SENSOR TYPE & SENTINEL DETECTION
 * ============================================================ */
static sensor_type_t get_sensor_type(uint8_t node_id)
{
    switch (node_id) {
        case 0x01:
        case 0x04:
            return SENSOR_TYPE_TEMPERATURE;
        case 0x02:
        case 0x05:
            return SENSOR_TYPE_HUMIDITY;
        case 0x03:
        case 0x06:
            return SENSOR_TYPE_LIGHT;
        default:
            return SENSOR_TYPE_UNKNOWN;
    }
}

static bool is_sensor_error(uint8_t node_id, uint16_t sensor_value)
{
    sensor_type_t type = get_sensor_type(node_id);
    
    switch (type) {
        case SENSOR_TYPE_TEMPERATURE:
            return (sensor_value == SENTINEL_TEMP_FAIL);
            
        case SENSOR_TYPE_HUMIDITY:
            return (sensor_value == SENTINEL_HUMI_FAIL);
            
        case SENSOR_TYPE_LIGHT:
            return false;
            
        default:
            return false;
    }
}

/* ============================================================
 * SECTION 12 — LAYER 2 PACKET VALIDATION
 * ============================================================ */
static bool validate_l2_packet(const uint8_t *raw, validated_l2_t *out)
{
    uint8_t  preamble     = raw[0];
    uint8_t  gw_id        = raw[1];
    uint32_t gw_timestamp = parse_u32_be(raw, 2);
    uint16_t received_crc = parse_u16_be(raw, 33);

    if (preamble != LAYER2_PREAMBLE) {
        ESP_LOGW(TAG, "L2 preamble FAIL — got 0x%02X", preamble);
        return false;
    }

    uint16_t computed_crc = crc16_ccitt(raw, 33);
    if (computed_crc != received_crc) {
        ESP_LOGW(TAG, "L2 CRC FAIL — computed 0x%04X received 0x%04X",
                 computed_crc, received_crc);
        return false;
    }

    if (gw_id < 0x01 || gw_id > 0x02) {
        ESP_LOGE(TAG, "INVALID GW_ID 0x%02X", gw_id);
        return false;
    }

    uint8_t gw_idx = gw_id - 1;
    if (gw_timestamp == s_last_gw_timestamp[gw_idx]) {
        ESP_LOGW(TAG, "Duplicate GW_TS %lu", (unsigned long)gw_timestamp);
        return false;
    }
    s_last_gw_timestamp[gw_idx] = gw_timestamp;

    s_last_gw_packet_ms[gw_idx] = (uint32_t)(esp_timer_get_time() / 1000ULL);

    ESP_LOGI(TAG, "L2 VALID from GW 0x%02X — GW_TS=%lu CRC=0x%04X",
             gw_id, (unsigned long)gw_timestamp, received_crc);

    out->gw_id = gw_id;
    out->gw_timestamp = gw_timestamp;

    for (int i = 0; i < 3; i++) {
        int offset = 6 + (i * 9);
        
        uint8_t  node_id      = raw[offset + 0];
        uint16_t packet_id    = parse_u16_be(raw, offset + 1);
        uint32_t timestamp    = parse_u32_be(raw, offset + 3);
        uint16_t sensor_value = parse_u16_be(raw, offset + 7);

        out->blocks[i].node_id      = node_id;
        out->blocks[i].packet_id    = packet_id;
        out->blocks[i].timestamp    = timestamp;
        out->blocks[i].sensor_value = sensor_value;

        if (packet_id == SENTINEL_PACKET_ID) {
            out->blocks[i].is_sentinel = true;
            out->blocks[i].is_sensor_error = false;
        } else {
            out->blocks[i].is_sentinel = false;
            out->blocks[i].is_sensor_error = is_sensor_error(node_id, sensor_value);
        }
    }

    return true;
}

/* ============================================================
 * SECTION 13 — ACK TRANSMISSION
 * ============================================================ */
static void send_ack_l2(void)
{
    uint8_t ack_byte = LAYER2_ACK_BYTE;

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    vTaskDelay(pdMS_TO_TICKS(5));
    write_reg(REG_IRQ_FLAGS, 0xFF);
    write_reg(REG_FIFO_ADDR_PTR, 0x80);
    write_fifo(&ack_byte, L2_ACK_SIZE);
    write_reg(REG_PAYLOAD_LENGTH, L2_ACK_SIZE);
    write_reg(REG_DIO_MAPPING1, 0x40);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_TX);

    uint32_t timeout = 200;
    while (timeout-- > 0) {
        if (read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK) {
            break;
        }
        vTaskDelay(pdMS_TO_TICKS(1));
    }

    write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    ESP_LOGI(TAG, "ACK 0xAA sent");
}

/* ============================================================
 * SECTION 14 — OFFLINE DETECTION
 * ============================================================ */
static void check_offline_fallback(void)
{
    uint32_t now_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);

    for (int gw_idx = 0; gw_idx < 2; gw_idx++) {
        if (s_last_gw_packet_ms[gw_idx] == 0) continue;

        uint32_t silence = now_ms - s_last_gw_packet_ms[gw_idx];

        if (silence > OFFLINE_TIMEOUT_MS) {
            uint8_t node_start = (gw_idx == 0) ? 1 : 4;
            uint8_t node_end   = (gw_idx == 0) ? 3 : 6;

            xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);
            for (uint8_t nid = node_start; nid <= node_end; nid++) {
                node_data_t *nd = &g_data_store[nid - 1];
                if (nd->online) {
                    nd->online = false;
                    nd->sensor_value_float = NAN;
                    nd->sensor_value_lux = 0;
                    nd->sensor_error = false;
                    ESP_LOGW(TAG, "Node %u: 90s silence — forced offline", nid);
                }
            }
            xSemaphoreGive(g_data_store_mutex);
        }
    }
}

/* ============================================================
 * SECTION 15 — DATA STORE UPDATE
 * ============================================================ */
static void update_data_store(const validated_l2_t *vl2)
{
    for (int i = 0; i < 3; i++) {
        uint8_t node_id = vl2->blocks[i].node_id;

        if (node_id < 1 || node_id > 6) continue;

        int idx = node_id - 1;

        xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);

        node_data_t *nd = &g_data_store[idx];
        nd->node_id = node_id;
        nd->gateway_id = vl2->gw_id;

        if (vl2->blocks[i].is_sentinel) {
            nd->missed_cycles++;

            if (nd->missed_cycles >= MISSED_CYCLES_OFFLINE) {
                nd->online = false;
                nd->sensor_value_float = NAN;
                nd->sensor_value_lux = 0;
                nd->sensor_error = false;
            }
        } else if (vl2->blocks[i].is_sensor_error) {
            nd->missed_cycles = 0;
            nd->online = true;
            nd->sensor_error = true;
            nd->timestamp = vl2->blocks[i].timestamp;
            nd->gw_timestamp = vl2->gw_timestamp;
            nd->last_packet_time_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
            
            nd->sensor_value_float = NAN;
            nd->sensor_value_lux = 0;

            ESP_LOGW(TAG, "Node %u: SENSOR ERROR — DHT11 read failed", node_id);
        } else {
            nd->missed_cycles = 0;
            nd->online = true;
            nd->sensor_error = false;
            nd->timestamp = vl2->blocks[i].timestamp;
            nd->gw_timestamp = vl2->gw_timestamp;
            nd->last_packet_time_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);

            uint16_t raw = vl2->blocks[i].sensor_value;
            sensor_type_t type = get_sensor_type(node_id);

            switch (type) {
                case SENSOR_TYPE_TEMPERATURE:
                    nd->sensor_value_float = (int16_t)raw / 10.0f;
                    nd->is_light_node = false;
                    ESP_LOGI(TAG, "Node %u (Temp): %.1f°C", node_id, nd->sensor_value_float);
                    break;

                case SENSOR_TYPE_HUMIDITY:
                    nd->sensor_value_float = raw / 10.0f;
                    nd->is_light_node = false;
                    ESP_LOGI(TAG, "Node %u (Humi): %.1f%%", node_id, nd->sensor_value_float);
                    break;

                case SENSOR_TYPE_LIGHT:
                    nd->sensor_value_lux = raw;
                    nd->sensor_value_float = NAN;
                    nd->is_light_node = true;
                    ESP_LOGI(TAG, "Node %u (Light): %u lux", node_id, raw);
                    break;

                default:
                    break;
            }
        }

        xSemaphoreGive(g_data_store_mutex);
    }

    check_offline_fallback();
}

/* ============================================================
 * SECTION 16 — JSON BUILDER
 * ============================================================ */
#define JSON_BUF_SIZE 2048

static void build_json(char *buf, size_t buf_size, uint32_t packet_id)
{
    uint32_t ts = (uint32_t)(esp_timer_get_time() / 1000ULL);

    int offset = snprintf(buf, buf_size,
        "{\"packet_id\":%lu,\"timestamp\":%lu,\"nodes\":[",
        (unsigned long)packet_id, (unsigned long)ts);

    xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);

    for (int i = 0; i < 6; i++) {
        node_data_t *nd = &g_data_store[i];

        if (i > 0) {
            offset += snprintf(buf + offset, buf_size - offset, ",");
        }

        offset += snprintf(buf + offset, buf_size - offset,
            "{\"node_id\":%u,\"gateway_id\":%u,\"online\":%s",
            nd->node_id, nd->gateway_id,
            nd->online ? "true" : "false");

        if (nd->online) {
            offset += snprintf(buf + offset, buf_size - offset,
                ",\"timestamp\":%lu,\"gw_timestamp\":%lu",
                (unsigned long)nd->timestamp,
                (unsigned long)nd->gw_timestamp);

            offset += snprintf(buf + offset, buf_size - offset,
                ",\"sensor_error\":%s",
                nd->sensor_error ? "true" : "false");

            if (nd->sensor_error) {
                offset += snprintf(buf + offset, buf_size - offset,
                    ",\"sensor_value\":null");
            } else if (nd->is_light_node) {
                offset += snprintf(buf + offset, buf_size - offset,
                    ",\"sensor_value\":%u", nd->sensor_value_lux);
            } else if (!isnan(nd->sensor_value_float)) {
                offset += snprintf(buf + offset, buf_size - offset,
                    ",\"sensor_value\":%.1f", nd->sensor_value_float);
            } else {
                offset += snprintf(buf + offset, buf_size - offset,
                    ",\"sensor_value\":null");
            }
        } else {
            offset += snprintf(buf + offset, buf_size - offset,
                ",\"timestamp\":null,\"gw_timestamp\":null,\"sensor_value\":null,\"sensor_error\":false");
        }

        offset += snprintf(buf + offset, buf_size - offset, "}");
    }

    xSemaphoreGive(g_data_store_mutex);

    snprintf(buf + offset, buf_size - offset, "]}");
}

/* ============================================================
 * SECTION 17 — WEBSOCKET FUNCTIONS
 * ============================================================ */
static bool is_dashboard_connected(void)
{
    xSemaphoreTake(ws_clients_mutex, portMAX_DELAY);
    bool connected = false;
    for (int i = 0; i < WIFI_MAX_CONN; i++) {
        if (ws_client_fds[i] >= 0) {
            connected = true;
            break;
        }
    }
    xSemaphoreGive(ws_clients_mutex);
    return connected;
}

static void ws_broadcast(const char *json_str)
{
    if (s_server == NULL) return;

    xSemaphoreTake(ws_clients_mutex, portMAX_DELAY);

    for (int i = 0; i < WIFI_MAX_CONN; i++) {
        if (ws_client_fds[i] < 0) continue;

        httpd_ws_frame_t frame = {
            .type = HTTPD_WS_TYPE_TEXT,
            .payload = (uint8_t *)json_str,
            .len = strlen(json_str),
            .final = true,
        };

        esp_err_t err = httpd_ws_send_frame_async(s_server,
                                                   ws_client_fds[i],
                                                   &frame);
        if (err != ESP_OK) {
            ws_client_fds[i] = -1;
        }
    }

    xSemaphoreGive(ws_clients_mutex);

    bool now_connected = is_dashboard_connected();
    if (s_dashboard_connected && !now_connected) {
        ESP_LOGW(TAG, "DASHBOARD DISCONNECTED");
        s_dashboard_connected = false;
    } else if (!s_dashboard_connected && now_connected) {
        ESP_LOGI(TAG, "DASHBOARD RECONNECTED");
        s_dashboard_connected = true;
    }
}

/* ============================================================
 * SECTION 18 — HTTP HANDLERS
 * ============================================================ */
static esp_err_t ws_handler(httpd_req_t *req)
{
    if (req->method == HTTP_GET) {
        ESP_LOGI(TAG, "WebSocket handshake");
        return ESP_OK;
    }

    httpd_ws_frame_t ws_pkt;
    memset(&ws_pkt, 0, sizeof(httpd_ws_frame_t));
    ws_pkt.type = HTTPD_WS_TYPE_TEXT;

    esp_err_t ret = httpd_ws_recv_frame(req, &ws_pkt, 0);
    if (ret != ESP_OK) return ret;

    int fd = httpd_req_to_sockfd(req);
    xSemaphoreTake(ws_clients_mutex, portMAX_DELAY);
    bool found = false;
    for (int i = 0; i < WIFI_MAX_CONN; i++) {
        if (ws_client_fds[i] == fd) {
            found = true;
            break;
        }
    }
    if (!found) {
        for (int i = 0; i < WIFI_MAX_CONN; i++) {
            if (ws_client_fds[i] < 0) {
                ws_client_fds[i] = fd;
                ESP_LOGI(TAG, "WS client connected fd=%d", fd);
                break;
            }
        }
    }
    xSemaphoreGive(ws_clients_mutex);

    return ESP_OK;
}

static esp_err_t root_handler(httpd_req_t *req)
{
    httpd_resp_send(req, dashboard_html, strlen(dashboard_html));
    return ESP_OK;
}

static esp_err_t api_nodes_handler(httpd_req_t *req)
{
    char json[JSON_BUF_SIZE];
    
    xSemaphoreTake(s_l3_packet_id_mutex, portMAX_DELAY);
    uint32_t current_id = s_l3_packet_id;
    xSemaphoreGive(s_l3_packet_id_mutex);
    
    build_json(json, sizeof(json), current_id);
    
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, json, strlen(json));
    return ESP_OK;
}

static void start_webserver(void)
{
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.max_open_sockets = 7;

    if (httpd_start(&s_server, &config) != ESP_OK) {
        ESP_LOGE(TAG, "Failed to start HTTP server");
        return;
    }

    httpd_uri_t root = {
        .uri = "/",
        .method = HTTP_GET,
        .handler = root_handler,
    };
    httpd_register_uri_handler(s_server, &root);

    httpd_uri_t api_nodes = {
        .uri = "/api/nodes",
        .method = HTTP_GET,
        .handler = api_nodes_handler,
    };
    httpd_register_uri_handler(s_server, &api_nodes);

    httpd_uri_t ws = {
        .uri = "/ws",
        .method = HTTP_GET,
        .handler = ws_handler,
        .is_websocket = true,
    };
    httpd_register_uri_handler(s_server, &ws);

    ESP_LOGI(TAG, "HTTP server started — http://192.168.4.1");
}

/* ============================================================
 * SECTION 19 — WiFi INITIALIZATION
 * ============================================================ */
static void wifi_event_handler(void *arg, esp_event_base_t event_base,
                                 int32_t event_id, void *event_data)
{
    if (event_base == WIFI_EVENT) {
        if (event_id == WIFI_EVENT_AP_STACONNECTED) {
            ESP_LOGI(TAG, "Station connected to AP");
        } else if (event_id == WIFI_EVENT_AP_STADISCONNECTED) {
            ESP_LOGI(TAG, "Station disconnected from AP");
        }
    }
}

static void init_wifi_ap(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_ap();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID,
                                                 &wifi_event_handler, NULL));

    wifi_config_t wifi_config = {
        .ap = {
            .ssid = WIFI_SSID,
            .ssid_len = strlen(WIFI_SSID),
            .password = WIFI_PASS,
            .max_connection = WIFI_MAX_CONN,
            .authmode = WIFI_AUTH_WPA2_PSK,
        },
    };

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "WiFi AP started — SSID: %s  IP: 192.168.4.1", WIFI_SSID);
}

/* ============================================================
 * SECTION 20 — TASKS
 * ============================================================ */
static void task_lora_rx(void *arg)
{
    ESP_LOGI(TAG, "Task A (LoRa RX) started");

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    uint8_t raw_buf[L2_PACKET_SIZE];

    while (1) {
        if (s_tx_active) {
            uint32_t tx_elapsed = (uint32_t)(esp_timer_get_time() / 1000ULL) - s_tx_start_time;
            if (tx_elapsed > TX_WATCHDOG_TIMEOUT_MS) {
                ESP_LOGE(TAG, "TX watchdog timeout");
                s_tx_active = false;
            } else {
                vTaskDelay(pdMS_TO_TICKS(50));
                continue;
            }
        }

        uint8_t irq = read_reg(REG_IRQ_FLAGS);

        if (!(irq & IRQ_RX_DONE_MASK)) {
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        xSemaphoreTake(s_lora_mutex, portMAX_DELAY);

        if (irq & IRQ_PAYLOAD_CRC_ERR) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "HW CRC error");
            continue;
        }

        uint8_t nb_bytes = read_reg(REG_RX_NB_BYTES);

        if (nb_bytes != L2_PACKET_SIZE) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "Wrong size %u", nb_bytes);
            continue;
        }

        uint8_t rx_addr = read_reg(REG_FIFO_RX_CURR_ADDR);
        write_reg(REG_FIFO_ADDR_PTR, rx_addr);
        read_fifo(raw_buf, L2_PACKET_SIZE);
        write_reg(REG_IRQ_FLAGS, 0xFF);

        xSemaphoreGive(s_lora_mutex);

        if (xQueueSend(s_raw_rx_queue, raw_buf, pdMS_TO_TICKS(50)) != pdTRUE) {
            ESP_LOGE(TAG, "raw_rx_queue full");
        }

        vTaskDelay(pdMS_TO_TICKS(20));
    }
}

static void task_validation_ack(void *arg)
{
    ESP_LOGI(TAG, "Task B (Validation + ACK) started");

    uint8_t raw_buf[L2_PACKET_SIZE];
    validated_l2_t vl2;

    while (1) {
        if (xQueueReceive(s_raw_rx_queue, raw_buf, portMAX_DELAY) == pdTRUE) {
            if (validate_l2_packet(raw_buf, &vl2)) {
                xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
                send_ack_l2();
                xSemaphoreGive(s_lora_mutex);

                if (xQueueSend(s_validated_queue, &vl2, portMAX_DELAY) != pdTRUE) {
                    ESP_LOGW(TAG, "validated_queue full");
                }
            }
        }
    }
}

static void task_ram_store_update(void *arg)
{
    ESP_LOGI(TAG, "Task D (RAM Store Update) started");

    validated_l2_t vl2;

    while (1) {
        if (xQueueReceive(s_validated_queue, &vl2, portMAX_DELAY) == pdTRUE) {
            update_data_store(&vl2);

            uint8_t gw_id = vl2.gw_id;
            xQueueSend(s_json_trigger, &gw_id, 0);
        }
    }
}

static void task_web_prep(void *arg)
{
    ESP_LOGI(TAG, "Task E (Web Prep + Broadcast) started");

    uint8_t gw_id;
    char json_buf[JSON_BUF_SIZE];

    while (1) {
        if (xQueueReceive(s_json_trigger, &gw_id, portMAX_DELAY) == pdTRUE) {
            xSemaphoreTake(s_l3_packet_id_mutex, portMAX_DELAY);
            s_l3_packet_id++;
            uint32_t current_packet_id = s_l3_packet_id;
            xSemaphoreGive(s_l3_packet_id_mutex);

            build_json(json_buf, sizeof(json_buf), current_packet_id);

            if (is_dashboard_connected()) {
                ws_broadcast(json_buf);
            }
        }
    }
}

/* ============================================================
 * SECTION 21 — APP_MAIN
 * ============================================================ */
void app_main(void)
{
    ESP_LOGI(TAG, "===========================================");
    ESP_LOGI(TAG, " Main Gateway — SENSOR-COMPATIBLE (INLINE HTML)");
    ESP_LOGI(TAG, " ESP32-WROOM-32 | ESP-IDF v5.5.2");
    ESP_LOGI(TAG, " RX: 434 MHz | WiFi AP: 192.168.4.1");
    ESP_LOGI(TAG, " Sentinel: Temp=0x8000 Humi=0x0000");
    ESP_LOGI(TAG, "===========================================");

    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    memset(g_data_store, 0, sizeof(g_data_store));
    for (int i = 0; i < 6; i++) {
        g_data_store[i].node_id = i + 1;
        g_data_store[i].gateway_id = (i < 3) ? 1 : 2;
        g_data_store[i].online = false;
        g_data_store[i].sensor_value_float = NAN;
        g_data_store[i].sensor_error = false;
    }

    sx1278_init_spi();
    gpio_set_direction(PIN_DIO0, GPIO_MODE_INPUT);
    sx1278_reset();
    sx1278_sanity_check();
    sx1278_configure_radio();

    s_lora_mutex          = xSemaphoreCreateBinary();
    g_data_store_mutex    = xSemaphoreCreateBinary();
    ws_clients_mutex      = xSemaphoreCreateBinary();
    s_l3_packet_id_mutex  = xSemaphoreCreateBinary();

    xSemaphoreGive(s_lora_mutex);
    xSemaphoreGive(g_data_store_mutex);
    xSemaphoreGive(ws_clients_mutex);
    xSemaphoreGive(s_l3_packet_id_mutex);

    s_raw_rx_queue    = xQueueCreate(8, L2_PACKET_SIZE);
    s_validated_queue = xQueueCreate(8, sizeof(validated_l2_t));
    s_json_trigger    = xQueueCreate(4, sizeof(uint8_t));

    if (!s_lora_mutex || !g_data_store_mutex || !ws_clients_mutex ||
        !s_raw_rx_queue || !s_validated_queue || !s_json_trigger) {
        ESP_LOGE(TAG, "FATAL: FreeRTOS object creation failed");
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }

    init_wifi_ap();
    start_webserver();

    xTaskCreatePinnedToCore(task_lora_rx,          "TaskA_RX",  4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_validation_ack,   "TaskB_Val", 4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_ram_store_update, "TaskD_RAM", 4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_web_prep,         "TaskE_Web", 4096, NULL, 3, NULL, 1);

    ESP_LOGI(TAG, "All tasks created — Main Gateway operational");
    ESP_LOGI(TAG, "Dashboard: http://192.168.4.1");
}