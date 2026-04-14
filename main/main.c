/*
 * =============================================================================
 * Estrotech — Centralized Private LoRa Sensor Network
 * Main Gateway (GW_ID = 0x00) — R&D Phase Firmware
 * COMBINED: LoRa firmware + WiFi AP + WebSocket dashboard server
 * =============================================================================
 *
 * Owner        : Midhlaj
 * Framework    : ESP-IDF v5.5.2
 * Board        : ESP32 DevKit V1 38-pin (ESP32-WROOM-32)
 * LoRa Module  : AI-Thinker Ra-02 SX1278
 *
 * SPI Pin Assignment (ESP32 DevKit V1 38-pin):
 *   SCK=18  MISO=19  MOSI=23  NSS=5  RST=14  DIO0=26
 *
 * =============================================================================
 * INTEGRATION NOTES (vs. separate test code):
 *
 *   REMOVED — send_mock() task. All data now comes from g_data_store[] which
 *              is populated by the real LoRa pipeline (Task A → B → D).
 *
 *   ADDED   — wifi_init()       : starts WiFi AP (LoRa-Gateway / estrotech)
 *   ADDED   — start_server()    : HTTP + WebSocket server on port 80
 *   ADDED   — ws_broadcast()    : pushes JSON to ALL connected WS clients
 *   ADDED   — /api/nodes        : REST snapshot endpoint (GET)
 *   ADDED   — /api/export       : CSV download endpoint (GET)
 *   WIRED   — Task E now calls ws_broadcast(json_buf) after building L3 JSON
 *
 *   MULTI-CLIENT: up to WS_MAX_CLIENTS simultaneous WebSocket connections
 *                 are tracked in ws_client_fds[]. Single-fd approach from the
 *                 test code is replaced with a tracked array + broadcast loop.
 *
 * =============================================================================
 * FIXES APPLIED vs PREVIOUS REVISION (LoRa side, unchanged):
 *
 *   FIX 1 — SPI_DMA_CH_AUTO replaced with SPI_DMA_DISABLED.
 *   FIX 2 — spi_device_acquire_bus() / spi_device_release_bus() added.
 *   FIX 3 — Task A poll interval increased from 5ms to 50ms (taskYIELD added).
 *
 * =============================================================================
 * ARCHITECTURE:
 *
 * Core 0:
 *   Task A — LoRa RX      (Priority 5) — continuous RX on 434 MHz
 *   Task B — Validation   (Priority 5) — CRC, preamble, dedup → ACK → queue
 *   Task C — Sync Beacon  (Priority 3) — beacon every 30s on 434 MHz
 *
 * Core 1:
 *   Task D — RAM Store    (Priority 5) — unpack NodeBlocks, update data_store[]
 *   Task E — Web Prep     (Priority 3) — build L3 JSON → ws_broadcast()
 *   ESP-IDF HTTP server   (built-in)   — serves dashboard HTML + WS + REST
 *
 * WEB TEAM HANDOFF (internal, now consumed here):
 *   extern node_data_t           g_data_store[6];
 *   extern SemaphoreHandle_t     g_data_store_mutex;
 *   void mgw_prepare_layer3_json(char *buf, size_t buf_len);
 * =============================================================================
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <math.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

#include "driver/spi_master.h"
#include "driver/gpio.h"

#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_http_server.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "nvs_flash.h"
#include "sdkconfig.h"

/* ============================================================
 * SECTION 1 — PIN DEFINITIONS (ESP32 DevKit V1 38-pin)
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
#define REG_PREAMBLE_MSB        0x20
#define REG_PREAMBLE_LSB        0x21
#define REG_PAYLOAD_LENGTH      0x22
#define REG_MODEM_CONFIG3       0x26
#define REG_SYNC_WORD           0x39
#define REG_DIO_MAPPING1        0x40
#define REG_VERSION             0x42

/* SX1278 operating modes */
#define MODE_LONG_RANGE_MODE    0x80
#define MODE_SLEEP              0x00
#define MODE_STDBY              0x01
#define MODE_TX                 0x03
#define MODE_RX_CONTINUOUS      0x05

/* IRQ flag masks */
#define IRQ_TX_DONE_MASK        0x08
#define IRQ_RX_DONE_MASK        0x40
#define IRQ_PAYLOAD_CRC_ERR     0x20

/* ============================================================
 * SECTION 3 — NETWORK CONSTANTS
 * ============================================================ */
#define FREQ_L2_MSB             0x6C
#define FREQ_L2_MID             0x80
#define FREQ_L2_LSB             0x00

#define LAYER2_PREAMBLE         0xBB
#define LAYER2_ACK_BYTE         0xAA
#define SYNC_BEACON_ID          0xBC
#define SYNC_WORD               0x12

#define L2_PACKET_SIZE          35
#define L2_ACK_SIZE             1
#define BEACON_SIZE             2

#define CYCLE_DURATION_MS       30000
#define BEACON_PERIOD_US        30000000ULL

#define MISSED_CYCLES_OFFLINE   3
#define OFFLINE_TIMEOUT_MS      45000

#define SX1278_VERSION_EXPECTED 0x12

#define SENTINEL_PACKET_ID      0xFFFF
#define SENTINEL_SENSOR_VALUE   0xFFFF

#define TOTAL_NODES             6
#define NODES_PER_GW            3

#define GW1_NODE_START          0x01
#define GW1_NODE_END            0x03
#define GW2_NODE_START          0x04
#define GW2_NODE_END            0x06

#define JSON_BUF_SIZE           1024

/* ============================================================
 * SECTION 4 — WEBSOCKET / SERVER CONSTANTS
 * ============================================================ */

/*
 * Maximum simultaneous WebSocket clients.
 * Increase if you need more concurrent browser tabs connected.
 */
#define WS_MAX_CLIENTS          4

/*
 * WiFi AP credentials — match the dashboard connection instructions.
 */
#define WIFI_SSID               "LoRa-Gateway"
#define WIFI_PASS               "estrotech"
#define WIFI_MAX_CONN           4

/* ============================================================
 * SECTION 5 — DATA STRUCTURES
 * ============================================================ */
typedef struct {
    uint8_t  node_id;
    uint8_t  gateway_id;
    bool     online;
    uint32_t timestamp;
    uint32_t gw_timestamp;
    float    sensor_value_float;
    uint16_t sensor_value_lux;
    bool     is_light_node;
    uint8_t  missed_cycles;
    uint32_t last_packet_time_ms;
} node_data_t;

typedef struct {
    uint8_t  gw_id;
    uint32_t gw_timestamp;
    struct {
        uint8_t  node_id;
        uint16_t packet_id;
        uint32_t timestamp;
        uint16_t sensor_value;
        bool     is_sentinel;
    } blocks[NODES_PER_GW];
} validated_l2_t;

/* ============================================================
 * SECTION 6 — GLOBAL STATE
 * ============================================================ */
static const char *TAG = "MGW";

static spi_device_handle_t s_spi_handle;

static SemaphoreHandle_t s_lora_mutex;
static volatile bool     s_tx_active = false;

static QueueHandle_t s_raw_rx_queue;
static QueueHandle_t s_validated_queue;
static QueueHandle_t s_json_trigger;
static QueueHandle_t s_beacon_trigger;

node_data_t           g_data_store[TOTAL_NODES];
SemaphoreHandle_t     g_data_store_mutex;

static uint32_t s_last_gw_packet_ms[2]  = {0, 0};
static uint32_t s_last_gw_timestamp[2]  = {0xFFFFFFFF, 0xFFFFFFFF};

static esp_timer_handle_t s_beacon_timer;

/*
 * WebSocket client fd tracking.
 * ws_client_fds[] holds the socket fd of each connected client (-1 = empty slot).
 * ws_clients_mutex protects the array across the HTTP server task and Task E.
 */
static int               ws_client_fds[WS_MAX_CLIENTS];
static SemaphoreHandle_t ws_clients_mutex;
static httpd_handle_t    s_server = NULL;

/* ============================================================
 * SECTION 7 — SX1278 SPI DRIVER
 * ============================================================ */
static void write_reg(uint8_t addr, uint8_t value)
{
    uint8_t tx[2] = { (uint8_t)(addr | 0x80), value };
    uint8_t rx[2] = { 0 };
    spi_transaction_t t = {
        .length    = 16,
        .tx_buffer = tx,
        .rx_buffer = rx,
    };
    spi_device_transmit(s_spi_handle, &t);
}

static uint8_t read_reg(uint8_t addr)
{
    uint8_t tx[2] = { (uint8_t)(addr & 0x7F), 0x00 };
    uint8_t rx[2] = { 0 };
    spi_transaction_t t = {
        .length    = 16,
        .tx_buffer = tx,
        .rx_buffer = rx,
    };
    spi_device_transmit(s_spi_handle, &t);
    return rx[1];
}

static void write_fifo(const uint8_t *data, uint8_t len)
{
    uint8_t tx[L2_PACKET_SIZE + 1];
    uint8_t rx[L2_PACKET_SIZE + 1];
    tx[0] = REG_FIFO | 0x80;
    memcpy(&tx[1], data, len);
    spi_transaction_t t = {
        .length    = (size_t)(len + 1) * 8,
        .tx_buffer = tx,
        .rx_buffer = rx,
    };
    spi_device_transmit(s_spi_handle, &t);
}

static void read_fifo(uint8_t *data, uint8_t len)
{
    uint8_t tx[L2_PACKET_SIZE + 1];
    uint8_t rx[L2_PACKET_SIZE + 1];
    memset(tx, 0, sizeof(tx));
    tx[0] = REG_FIFO & 0x7F;
    spi_transaction_t t = {
        .length    = (size_t)(len + 1) * 8,
        .tx_buffer = tx,
        .rx_buffer = rx,
    };
    spi_device_transmit(s_spi_handle, &t);
    memcpy(data, &rx[1], len);
}

/* ============================================================
 * SECTION 8 — SX1278 INITIALISATION
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
    /* FIX 1: SPI_DMA_DISABLED — eliminates WDT crash from DMA bg lock */
    ESP_ERROR_CHECK(spi_bus_initialize(HSPI_HOST, &bus_cfg, SPI_DMA_DISABLED));

    spi_device_interface_config_t dev_cfg = {
        .clock_speed_hz = 1000000,
        .mode           = 0,
        .spics_io_num   = PIN_NSS,
        .queue_size     = 4,
    };
    ESP_ERROR_CHECK(spi_bus_add_device(HSPI_HOST, &dev_cfg, &s_spi_handle));
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
        ESP_LOGE(TAG, "FATAL: SX1278 reg 0x42 = 0x%02X, expected 0x12. "
                      "Check wiring: SCK=%d MISO=%d MOSI=%d NSS=%d",
                 version, PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }
    ESP_LOGI(TAG, "SX1278 sanity check PASSED — reg 0x42 = 0x12");
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
    write_reg(REG_PREAMBLE_MSB,   0x00);
    write_reg(REG_PREAMBLE_LSB,   0x08);
    write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1,   0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_TX_BASE_ADDR, 0x80);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);

    ESP_LOGI(TAG, "SX1278 configured: SF7/BW125/CR4_5/10dBm/Sync=0x12 on 434.0 MHz");
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
 * SECTION 10 — PACKET PARSING HELPERS
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
 * SECTION 11 — LAYER 2 PACKET VALIDATION
 * ============================================================ */
static bool validate_l2_packet(const uint8_t *raw, validated_l2_t *out)
{
    if (raw[0] != LAYER2_PREAMBLE) {
        ESP_LOGW(TAG, "L2 preamble FAIL — got 0x%02X expected 0xBB", raw[0]);
        return false;
    }

    uint16_t computed_crc = crc16_ccitt(raw, 33);
    uint16_t received_crc = parse_u16_be(raw, 33);
    if (computed_crc != received_crc) {
        ESP_LOGW(TAG, "L2 CRC FAIL — computed 0x%04X received 0x%04X",
                 computed_crc, received_crc);
        return false;
    }

    uint8_t  gw_id        = raw[1];
    uint32_t gw_timestamp = parse_u32_be(raw, 2);

    if (gw_id < 0x01 || gw_id > 0x02) {
        ESP_LOGW(TAG, "L2 GW_ID 0x%02X invalid", gw_id);
        return false;
    }

    uint8_t gw_idx = gw_id - 1;
    if (gw_timestamp == s_last_gw_timestamp[gw_idx]) {
        ESP_LOGW(TAG, "L2 GW 0x%02X: duplicate GW_TS=%lu — discarded",
                 gw_id, (unsigned long)gw_timestamp);
        return false;
    }
    s_last_gw_timestamp[gw_idx] = gw_timestamp;

    out->gw_id        = gw_id;
    out->gw_timestamp = gw_timestamp;

    const uint8_t nb_offsets[NODES_PER_GW] = { 6, 15, 24 };
    for (int i = 0; i < NODES_PER_GW; i++) {
        uint8_t off = nb_offsets[i];
        out->blocks[i].node_id      = raw[off + 0];
        out->blocks[i].packet_id    = parse_u16_be(raw, off + 1);
        out->blocks[i].timestamp    = parse_u32_be(raw, off + 3);
        out->blocks[i].sensor_value = parse_u16_be(raw, off + 7);
        out->blocks[i].is_sentinel  = (out->blocks[i].packet_id == SENTINEL_PACKET_ID);
    }

    ESP_LOGI(TAG, "L2 VALID from GW 0x%02X — GW_TS=%lu ms CRC=0x%04X",
             gw_id, (unsigned long)gw_timestamp, received_crc);
    return true;
}

/* ============================================================
 * SECTION 12 — ACK TRANSMIT (FIX 2: acquire/release bus)
 * ============================================================ */
static void send_ack_l2(void)
{
    spi_device_acquire_bus(s_spi_handle, portMAX_DELAY);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    vTaskDelay(pdMS_TO_TICKS(5));

    write_reg(REG_PAYLOAD_LENGTH, L2_ACK_SIZE);
    write_reg(REG_DIO_MAPPING1, 0x40);
    write_reg(REG_IRQ_FLAGS, 0xFF);
    write_reg(REG_FIFO_ADDR_PTR, 0x80);
    uint8_t ack = LAYER2_ACK_BYTE;
    write_fifo(&ack, 1);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_TX);

    uint32_t start = (uint32_t)(esp_timer_get_time() / 1000ULL);
    while (!(read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK)) {
        uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
        if ((now - start) > 500) { ESP_LOGE(TAG, "ACK TX timeout"); break; }
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);

    write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1, 0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_ADDR_PTR, 0x00);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    spi_device_release_bus(s_spi_handle);
    ESP_LOGI(TAG, "ACK 0xAA sent");
}

/* ============================================================
 * SECTION 13 — SYNC BEACON TRANSMIT (FIX 2: acquire/release bus)
 * ============================================================ */
static void send_sync_beacon(void)
{
    spi_device_acquire_bus(s_spi_handle, portMAX_DELAY);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    vTaskDelay(pdMS_TO_TICKS(5));

    uint8_t beacon[BEACON_SIZE] = { SYNC_BEACON_ID, 0x00 };
    write_reg(REG_PAYLOAD_LENGTH, BEACON_SIZE);
    write_reg(REG_DIO_MAPPING1, 0x40);
    write_reg(REG_IRQ_FLAGS, 0xFF);
    write_reg(REG_FIFO_ADDR_PTR, 0x80);
    write_fifo(beacon, BEACON_SIZE);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_TX);

    uint32_t start = (uint32_t)(esp_timer_get_time() / 1000ULL);
    while (!(read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK)) {
        uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
        if ((now - start) > 500) { ESP_LOGE(TAG, "Beacon TX timeout"); break; }
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);

    write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1, 0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_ADDR_PTR, 0x00);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    spi_device_release_bus(s_spi_handle);
    ESP_LOGI(TAG, "Sync beacon [0xBC 0x00] sent on 434.0 MHz");
}

/* ============================================================
 * SECTION 14 — ONLINE/OFFLINE LOGIC & DATA STORE UPDATE
 * ============================================================ */
static void update_node(uint8_t node_id, uint8_t gw_id, uint32_t gw_timestamp,
                         uint32_t node_timestamp, uint16_t sensor_raw,
                         bool is_sentinel)
{
    if (node_id < 1 || node_id > TOTAL_NODES) return;
    int idx = node_id - 1;

    xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);

    node_data_t *nd = &g_data_store[idx];
    nd->node_id    = node_id;
    nd->gateway_id = gw_id;

    if (is_sentinel) {
        nd->missed_cycles++;
        if (nd->missed_cycles >= MISSED_CYCLES_OFFLINE) {
            nd->online             = false;
            nd->sensor_value_float = NAN;
            nd->sensor_value_lux   = 0;
        }
        ESP_LOGD(TAG, "Node %u: absent — missed=%u online=%s",
                 node_id, nd->missed_cycles, nd->online ? "true" : "false");
    } else {
        nd->missed_cycles         = 0;
        nd->online                = true;
        nd->timestamp             = node_timestamp;
        nd->gw_timestamp          = gw_timestamp;
        nd->last_packet_time_ms   = (uint32_t)(esp_timer_get_time() / 1000ULL);

        if (node_id == 1 || node_id == 4) {
            nd->sensor_value_float = (float)(int16_t)sensor_raw / 10.0f;
            nd->is_light_node      = false;
            ESP_LOGI(TAG, "Node %u (Temp): %.1f C  raw=0x%04X",
                     node_id, nd->sensor_value_float, sensor_raw);
        } else if (node_id == 2 || node_id == 5) {
            nd->sensor_value_float = (float)sensor_raw / 10.0f;
            nd->is_light_node      = false;
            ESP_LOGI(TAG, "Node %u (Humidity): %.1f%%  raw=0x%04X",
                     node_id, nd->sensor_value_float, sensor_raw);
        } else if (node_id == 3 || node_id == 6) {
            nd->sensor_value_lux   = sensor_raw;
            nd->sensor_value_float = NAN;
            nd->is_light_node      = true;
            ESP_LOGI(TAG, "Node %u (Light): %u lux  raw=0x%04X",
                     node_id, sensor_raw, sensor_raw);
        }
    }

    xSemaphoreGive(g_data_store_mutex);
}

static void check_offline_fallback(void)
{
    uint32_t now_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);

    for (int gw_idx = 0; gw_idx < 2; gw_idx++) {
        if (s_last_gw_packet_ms[gw_idx] == 0) continue;
        if ((now_ms - s_last_gw_packet_ms[gw_idx]) > OFFLINE_TIMEOUT_MS) {
            uint8_t node_start = (gw_idx == 0) ? GW1_NODE_START : GW2_NODE_START;
            uint8_t node_end   = (gw_idx == 0) ? GW1_NODE_END   : GW2_NODE_END;
            xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);
            for (uint8_t nid = node_start; nid <= node_end; nid++) {
                node_data_t *nd = &g_data_store[nid - 1];
                if (nd->online) {
                    nd->online             = false;
                    nd->sensor_value_float = NAN;
                    nd->sensor_value_lux   = 0;
                    ESP_LOGW(TAG, "Node %u: 45s silence — forced offline", nid);
                }
            }
            xSemaphoreGive(g_data_store_mutex);
        }
    }
}

/* ============================================================
 * SECTION 15 — LAYER 3 JSON BUILDER
 * ============================================================ */
void mgw_prepare_layer3_json(char *buf, size_t buf_len)
{
    xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);

    int pos = 0;
    pos += snprintf(buf + pos, buf_len - pos, "[");

    for (int i = 0; i < TOTAL_NODES; i++) {
        node_data_t *nd = &g_data_store[i];

        pos += snprintf(buf + pos, buf_len - pos,
                        "%s{\"node_id\":%u,\"gateway_id\":%u,\"online\":%s,",
                        (i > 0) ? "," : "",
                        nd->node_id,
                        nd->gateway_id,
                        nd->online ? "true" : "false");

        if (nd->online) {
            pos += snprintf(buf + pos, buf_len - pos,
                            "\"timestamp\":%lu,\"gw_timestamp\":%lu,",
                            (unsigned long)nd->timestamp,
                            (unsigned long)nd->gw_timestamp);
            if (nd->is_light_node) {
                pos += snprintf(buf + pos, buf_len - pos,
                                "\"sensor_value\":%u}", nd->sensor_value_lux);
            } else {
                pos += snprintf(buf + pos, buf_len - pos,
                                "\"sensor_value\":%.1f}", nd->sensor_value_float);
            }
        } else {
            pos += snprintf(buf + pos, buf_len - pos,
                            "\"timestamp\":null,\"gw_timestamp\":null,"
                            "\"sensor_value\":null}");
        }
    }
    pos += snprintf(buf + pos, buf_len - pos, "]");

    xSemaphoreGive(g_data_store_mutex);
}

/* ============================================================
 * SECTION 16 — WEBSOCKET BROADCAST
 * ============================================================
 *
 * ws_broadcast() sends a text frame to every connected client.
 * Dead/closed clients are detected by the send error and their slot
 * is cleared so it can be reused by the next connecting client.
 *
 * Called from Task E (Core 1) after JSON is built.
 * ws_clients_mutex protects ws_client_fds[] against concurrent access
 * from the HTTP server task (ws_handler, running on Core 1 as well).
 */
void ws_broadcast(const char *json_str)
{
    if (s_server == NULL) return;

    xSemaphoreTake(ws_clients_mutex, portMAX_DELAY);

    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        if (ws_client_fds[i] < 0) continue;

        httpd_ws_frame_t frame = {
            .type       = HTTPD_WS_TYPE_TEXT,
            .payload    = (uint8_t *)json_str,
            .len        = strlen(json_str),
            .final      = true,
        };

        esp_err_t err = httpd_ws_send_frame_async(s_server,
                                                   ws_client_fds[i],
                                                   &frame);
        if (err != ESP_OK) {
            /*
             * Client disconnected or send failed.
             * Clear the slot — the next handshake will fill it again.
             */
            ESP_LOGW(TAG, "WS send failed fd=%d err=0x%x — removing client",
                     ws_client_fds[i], err);
            ws_client_fds[i] = -1;
        } else {
            ESP_LOGD(TAG, "WS broadcast → fd=%d (%d bytes)",
                     ws_client_fds[i], (int)strlen(json_str));
        }
    }

    xSemaphoreGive(ws_clients_mutex);
}

/* ============================================================
 * SECTION 17 — HTTP HANDLERS
 * ============================================================ */

/*
 * GET /
 * Returns a minimal HTML page that connects via WebSocket and renders live
 * sensor data. The actual full dashboard HTML is served from SPIFFS in
 * production — replace html_page below with your SPIFFS read if needed.
 */
static const char *html_page =
    "<!DOCTYPE html>"
    "<html><head><meta charset='utf-8'>"
    "<title>Estrotech Gateway</title></head>"
    "<body>"
    "<h2>Estrotech LoRa Dashboard</h2>"
    "<pre id='data'>Waiting for data...</pre>"
    "<script>"
    "const ws = new WebSocket('ws://192.168.4.1/ws');"
    "ws.onopen  = () => fetch('/api/nodes')"
    "               .then(r => r.json())"
    "               .then(d => document.getElementById('data').textContent"
    "                        = JSON.stringify(d, null, 2));"
    "ws.onmessage = e => {"
    "  document.getElementById('data').textContent"
    "    = JSON.stringify(JSON.parse(e.data), null, 2);"
    "};"
    "ws.onclose = () => setTimeout(() => location.reload(), 3000);"
    "</script>"
    "</body></html>";

static esp_err_t handler_root(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, html_page, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

/*
 * GET /api/nodes
 * Returns the current JSON snapshot of all 6 nodes.
 * The dashboard calls this once on page load so the screen is not blank
 * while waiting for the first WebSocket push.
 */
static esp_err_t handler_api_nodes(httpd_req_t *req)
{
    char buf[JSON_BUF_SIZE];
    mgw_prepare_layer3_json(buf, sizeof(buf));
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req,  "Access-Control-Allow-Origin", "*");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

/*
 * GET /api/export
 * Downloads a CSV of all current node readings.
 * One row per node: node_id, gateway_id, online, sensor_value, timestamp
 */
static esp_err_t handler_api_export(httpd_req_t *req)
{
    char csv[512];
    int  pos = 0;

    pos += snprintf(csv + pos, sizeof(csv) - pos,
                    "node_id,gateway_id,online,sensor_value,timestamp\r\n");

    xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);
    for (int i = 0; i < TOTAL_NODES; i++) {
        node_data_t *nd = &g_data_store[i];
        if (nd->online) {
            if (nd->is_light_node) {
                pos += snprintf(csv + pos, sizeof(csv) - pos,
                                "%u,%u,true,%u,%lu\r\n",
                                nd->node_id, nd->gateway_id,
                                nd->sensor_value_lux,
                                (unsigned long)nd->timestamp);
            } else {
                pos += snprintf(csv + pos, sizeof(csv) - pos,
                                "%u,%u,true,%.1f,%lu\r\n",
                                nd->node_id, nd->gateway_id,
                                nd->sensor_value_float,
                                (unsigned long)nd->timestamp);
            }
        } else {
            pos += snprintf(csv + pos, sizeof(csv) - pos,
                            "%u,%u,false,,\r\n",
                            nd->node_id, nd->gateway_id);
        }
    }
    xSemaphoreGive(g_data_store_mutex);

    httpd_resp_set_type(req, "text/csv");
    httpd_resp_set_hdr(req, "Content-Disposition",
                       "attachment; filename=\"nodes_export.csv\"");
    httpd_resp_send(req, csv, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

/*
 * WS /ws
 * Handles WebSocket handshake and incoming frames.
 *
 * On HTTP_GET (handshake): registers the client fd in ws_client_fds[].
 * On data frame: reads and discards (dashboard is receive-only for now).
 */
static esp_err_t handler_ws(httpd_req_t *req)
{
    if (req->method == HTTP_GET) {
        int fd = httpd_req_to_sockfd(req);

        xSemaphoreTake(ws_clients_mutex, portMAX_DELAY);
        bool registered = false;
        for (int i = 0; i < WS_MAX_CLIENTS; i++) {
            if (ws_client_fds[i] < 0) {
                ws_client_fds[i] = fd;
                registered = true;
                ESP_LOGI(TAG, "WS client connected fd=%d slot=%d", fd, i);
                break;
            }
        }
        xSemaphoreGive(ws_clients_mutex);

        if (!registered) {
            ESP_LOGW(TAG, "WS: no free slots — rejecting fd=%d", fd);
            return ESP_FAIL;   /* httpd will close the connection */
        }
        return ESP_OK;
    }

    /* Receive and discard any incoming frames (ping / browser messages) */
    httpd_ws_frame_t frame = { .type = HTTPD_WS_TYPE_TEXT };
    esp_err_t ret = httpd_ws_recv_frame(req, &frame, 0);
    if (ret == ESP_OK && frame.len > 0) {
        uint8_t *buf = malloc(frame.len + 1);
        if (buf) {
            frame.payload = buf;
            httpd_ws_recv_frame(req, &frame, frame.len);
            free(buf);
        }
    }
    return ESP_OK;
}

/* ============================================================
 * SECTION 18 — WIFI INIT
 * ============================================================ */
static void wifi_init(void)
{
    /*
     * NVS must be initialized before esp_wifi_init().
     * nvs_flash_erase() is called only when the NVS partition is full or
     * has no free pages — this is the standard ESP-IDF pattern.
     */
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    }

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_ap();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    wifi_config_t ap_config = {
        .ap = {
            .ssid           = WIFI_SSID,
            .password       = WIFI_PASS,
            .max_connection = WIFI_MAX_CONN,
            .authmode       = WIFI_AUTH_WPA_WPA2_PSK,
        }
    };

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "WiFi AP started — SSID: %s  IP: 192.168.4.1", WIFI_SSID);
}

/* ============================================================
 * SECTION 19 — HTTP SERVER INIT
 * ============================================================ */
static void start_server(void)
{
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    /*
     * Increase max open sockets to handle WS_MAX_CLIENTS WebSocket
     * connections plus a few simultaneous HTTP requests.
     */
    config.max_open_sockets = WS_MAX_CLIENTS + 3;

    if (httpd_start(&s_server, &config) != ESP_OK) {
        ESP_LOGE(TAG, "FATAL: HTTP server failed to start");
        return;
    }

    /* GET / — dashboard HTML */
    httpd_uri_t uri_root = {
        .uri     = "/",
        .method  = HTTP_GET,
        .handler = handler_root,
    };
    httpd_register_uri_handler(s_server, &uri_root);

    /* GET /api/nodes — JSON snapshot */
    httpd_uri_t uri_nodes = {
        .uri     = "/api/nodes",
        .method  = HTTP_GET,
        .handler = handler_api_nodes,
    };
    httpd_register_uri_handler(s_server, &uri_nodes);

    /* GET /api/export — CSV download */
    httpd_uri_t uri_export = {
        .uri     = "/api/export",
        .method  = HTTP_GET,
        .handler = handler_api_export,
    };
    httpd_register_uri_handler(s_server, &uri_export);

    /* WS /ws — live WebSocket push */
    httpd_uri_t uri_ws = {
        .uri          = "/ws",
        .method       = HTTP_GET,
        .handler      = handler_ws,
        .is_websocket = true,
    };
    httpd_register_uri_handler(s_server, &uri_ws);

    ESP_LOGI(TAG, "HTTP server started — endpoints: / /api/nodes /api/export /ws");
}

/* ============================================================
 * SECTION 20 — ESP_TIMER CALLBACK (SYNC BEACON)
 * ============================================================ */
static void beacon_timer_cb(void *arg)
{
    uint8_t sig = 1;
    BaseType_t hp = pdFALSE;
    xQueueSendFromISR(s_beacon_trigger, &sig, &hp);
    portYIELD_FROM_ISR(hp);
}

/* ============================================================
 * SECTION 21 — TASK A: LoRa RX (Core 1, Priority 5)
 * ============================================================ */
static void task_lora_rx(void *arg)
{
    ESP_LOGI(TAG, "Task A (LoRa RX) started on Core %d", xPortGetCoreID());

    spi_device_acquire_bus(s_spi_handle, portMAX_DELAY);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);
    spi_device_release_bus(s_spi_handle);

    ESP_LOGI(TAG, "Task A: listening on 434.0 MHz for L2 packets");

    uint8_t raw_buf[L2_PACKET_SIZE];

    while (1) {

        if (s_tx_active) {
            vTaskDelay(pdMS_TO_TICKS(50));
            continue;
        }

        uint8_t irq = read_reg(REG_IRQ_FLAGS);
        taskYIELD();

        if (!(irq & IRQ_RX_DONE_MASK)) {
            vTaskDelay(pdMS_TO_TICKS(50));
            continue;
        }

        xSemaphoreTake(s_lora_mutex, portMAX_DELAY);

        if (s_tx_active) {
            xSemaphoreGive(s_lora_mutex);
            vTaskDelay(pdMS_TO_TICKS(50));
            continue;
        }

        spi_device_acquire_bus(s_spi_handle, portMAX_DELAY);

        irq = read_reg(REG_IRQ_FLAGS);
        taskYIELD();

        if (irq & IRQ_PAYLOAD_CRC_ERR) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            spi_device_release_bus(s_spi_handle);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "Task A: HW CRC error — discarded");
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        if (!(irq & IRQ_RX_DONE_MASK)) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            spi_device_release_bus(s_spi_handle);
            xSemaphoreGive(s_lora_mutex);
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        uint8_t nb_bytes = read_reg(REG_RX_NB_BYTES);
        int8_t  rssi     = (int8_t)read_reg(REG_PKT_RSSI_VALUE) - 157;

        if (nb_bytes != L2_PACKET_SIZE) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            spi_device_release_bus(s_spi_handle);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "Task A: wrong size %u", nb_bytes);
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        uint8_t rx_addr = read_reg(REG_FIFO_RX_CURR_ADDR);
        write_reg(REG_FIFO_ADDR_PTR, rx_addr);
        read_fifo(raw_buf, L2_PACKET_SIZE);
        write_reg(REG_IRQ_FLAGS, 0xFF);

        spi_device_release_bus(s_spi_handle);
        xSemaphoreGive(s_lora_mutex);

        ESP_LOGD(TAG, "Task A: packet received RSSI=%ddBm", rssi);

        if (xQueueSend(s_raw_rx_queue, raw_buf, pdMS_TO_TICKS(10)) != pdTRUE) {
            ESP_LOGW(TAG, "Task A: queue full");
        }

        vTaskDelay(pdMS_TO_TICKS(20));
    }
}

/* ============================================================
 * SECTION 22 — TASK B: VALIDATION + ACK (Core 1, Priority 5)
 * ============================================================ */
static void task_validation(void *arg)
{
    ESP_LOGI(TAG, "Task B (Validation + ACK) started on Core %d", xPortGetCoreID());

    uint8_t        raw_buf[L2_PACKET_SIZE];
    validated_l2_t vl2;

    while (1) {
        if (xQueueReceive(s_raw_rx_queue, raw_buf, portMAX_DELAY) == pdTRUE) {
            if (validate_l2_packet(raw_buf, &vl2)) {
                s_last_gw_packet_ms[vl2.gw_id - 1] =
                    (uint32_t)(esp_timer_get_time() / 1000ULL);

                xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
                send_ack_l2();
                xSemaphoreGive(s_lora_mutex);

                if (xQueueSend(s_validated_queue, &vl2, pdMS_TO_TICKS(50)) != pdTRUE) {
                    ESP_LOGW(TAG, "Task B: validated queue full — data dropped");
                }
            }
        }
    }
}

/* ============================================================
 * SECTION 23 — TASK C: SYNC BEACON (Core 0, Priority 3)
 * ============================================================ */
static void task_sync_beacon(void *arg)
{
    ESP_LOGI(TAG, "Task C (Sync Beacon) started on Core %d", xPortGetCoreID());

    uint8_t sig;

    while (1) {
        if (xQueueReceive(s_beacon_trigger, &sig, portMAX_DELAY) == pdTRUE) {
            ESP_LOGI(TAG, "Task C: sending sync beacon");
            s_tx_active = true;
            vTaskDelay(pdMS_TO_TICKS(15));

            xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
            send_sync_beacon();
            xSemaphoreGive(s_lora_mutex);

            s_tx_active = false;
            ESP_LOGI(TAG, "Task C: beacon done — Task A resumes RX");
        }
    }
}

/* ============================================================
 * SECTION 24 — TASK D: RAM STORE UPDATE (Core 1, Priority 5)
 * ============================================================ */
static void task_ram_store(void *arg)
{
    ESP_LOGI(TAG, "Task D (RAM Store) started on Core %d", xPortGetCoreID());

    validated_l2_t vl2;

    while (1) {
        if (xQueueReceive(s_validated_queue, &vl2, portMAX_DELAY) == pdTRUE) {
            ESP_LOGI(TAG, "Task D: processing GW 0x%02X — 3 NodeBlocks", vl2.gw_id);

            for (int i = 0; i < NODES_PER_GW; i++) {
                update_node(
                    vl2.blocks[i].node_id,
                    vl2.gw_id,
                    vl2.gw_timestamp,
                    vl2.blocks[i].timestamp,
                    vl2.blocks[i].sensor_value,
                    vl2.blocks[i].is_sentinel
                );
            }

            check_offline_fallback();

            uint8_t gw_id = vl2.gw_id;
            if (xQueueSend(s_json_trigger, &gw_id, pdMS_TO_TICKS(10)) != pdTRUE) {
                ESP_LOGW(TAG, "Task D: json_trigger queue full");
            }
        }
    }
}

/* ============================================================
 * SECTION 25 — TASK E: WEB PREP + WS BROADCAST (Core 1, Priority 3)
 * ============================================================
 *
 * This is where the gateway firmware meets the dashboard.
 *
 * Every time a valid L2 packet is processed by Task D, it drops a
 * gateway ID into s_json_trigger. Task E wakes up, calls
 * mgw_prepare_layer3_json() to snapshot g_data_store[] into JSON,
 * then calls ws_broadcast() to push it to all connected browsers.
 *
 * No mock data. No timer. Pushes only on real LoRa data.
 */
static void task_web_prep(void *arg)
{
    ESP_LOGI(TAG, "Task E (Web Prep + WS Broadcast) started on Core %d",
             xPortGetCoreID());

    uint8_t gw_id;
    char    json_buf[JSON_BUF_SIZE];

    while (1) {
        if (xQueueReceive(s_json_trigger, &gw_id, portMAX_DELAY) == pdTRUE) {
            ESP_LOGI(TAG, "Task E: GW 0x%02X updated — building L3 JSON", gw_id);

            mgw_prepare_layer3_json(json_buf, sizeof(json_buf));

            ESP_LOGI(TAG, "L3 JSON: %s", json_buf);

            /* ── THE INTEGRATION POINT ──────────────────────────────────
             * ws_broadcast() replaces the old send_mock() task entirely.
             * Real sensor JSON is pushed to every connected dashboard tab.
             * ─────────────────────────────────────────────────────────── */
            ws_broadcast(json_buf);
        }
    }
}

/* ============================================================
 * SECTION 26 — RAM DATA STORE INITIALISATION
 * ============================================================ */
static void init_data_store(void)
{
    const uint8_t gw_assignment[TOTAL_NODES] = { 1, 1, 1, 2, 2, 2 };
    for (int i = 0; i < TOTAL_NODES; i++) {
        g_data_store[i].node_id             = (uint8_t)(i + 1);
        g_data_store[i].gateway_id          = gw_assignment[i];
        g_data_store[i].online              = false;
        g_data_store[i].timestamp           = 0;
        g_data_store[i].gw_timestamp        = 0;
        g_data_store[i].sensor_value_float  = NAN;
        g_data_store[i].sensor_value_lux    = 0;
        g_data_store[i].is_light_node       = (i == 2 || i == 5);
        g_data_store[i].missed_cycles       = 0;
        g_data_store[i].last_packet_time_ms = 0;
    }
}

/* ============================================================
 * SECTION 27 — APP_MAIN
 * ============================================================ */
void app_main(void)
{
    ESP_LOGI(TAG, "========================================");
    ESP_LOGI(TAG, " Estrotech Main Gateway — R&D Phase");
    ESP_LOGI(TAG, " ESP32 DevKit V1 38-pin | ESP-IDF v5.5.2");
    ESP_LOGI(TAG, " LoRa RX: 434.0 MHz  |  WiFi AP: 192.168.4.1");
    ESP_LOGI(TAG, " ACK=0xAA  Beacon=0xBC  SPI=No-DMA");
    ESP_LOGI(TAG, "========================================");

    /* ── 1. LoRa radio ── */
    sx1278_init_spi();
    ESP_LOGI(TAG, "SPI (HSPI, no-DMA) initialized: SCK=%d MISO=%d MOSI=%d NSS=%d",
             PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);

    gpio_set_direction(PIN_DIO0, GPIO_MODE_INPUT);
    sx1278_reset();
    ESP_LOGI(TAG, "SX1278 hardware reset complete");
    sx1278_sanity_check();
    sx1278_configure_radio();

    /* ── 2. Sensor data store ── */
    init_data_store();
    ESP_LOGI(TAG, "RAM data store initialized — 6 nodes, all offline at start");

    /* ── 3. FreeRTOS synchronisation objects ── */
    s_lora_mutex       = xSemaphoreCreateBinary();
    xSemaphoreGive(s_lora_mutex);
    g_data_store_mutex = xSemaphoreCreateMutex();
    ws_clients_mutex   = xSemaphoreCreateMutex();
    s_raw_rx_queue     = xQueueCreate(8, L2_PACKET_SIZE);
    s_validated_queue  = xQueueCreate(4, sizeof(validated_l2_t));
    s_json_trigger     = xQueueCreate(2, sizeof(uint8_t));
    s_beacon_trigger   = xQueueCreate(1, sizeof(uint8_t));

    if (!s_lora_mutex || !g_data_store_mutex || !ws_clients_mutex ||
        !s_raw_rx_queue || !s_validated_queue ||
        !s_json_trigger || !s_beacon_trigger) {
        ESP_LOGE(TAG, "FATAL: FreeRTOS object creation failed");
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }

    /* ── 4. Init WS client slot array ── */
    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        ws_client_fds[i] = -1;
    }

    /* ── 5. WiFi AP ── */
    wifi_init();

    /* ── 6. HTTP + WebSocket server ── */
    start_server();

    /* ── 7. Sync beacon timer — fires every 30s ── */
    esp_timer_create_args_t beacon_args = {
        .callback = beacon_timer_cb,
        .name     = "beacon_timer",
    };
    ESP_ERROR_CHECK(esp_timer_create(&beacon_args, &s_beacon_timer));
    ESP_ERROR_CHECK(esp_timer_start_periodic(s_beacon_timer, BEACON_PERIOD_US));
    ESP_LOGI(TAG, "Sync beacon timer armed — fires every 30s on 434.0 MHz");

    /* ── 8. FreeRTOS tasks ── */
    xTaskCreatePinnedToCore(task_lora_rx,     "TaskA_RX",    4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_validation,  "TaskB_Val",   4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_sync_beacon, "TaskC_Bcn",   3072, NULL, 3, NULL, 0);
    xTaskCreatePinnedToCore(task_ram_store,   "TaskD_Store", 4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_web_prep,    "TaskE_Web",   4096, NULL, 3, NULL, 1);

    ESP_LOGI(TAG, "All tasks created — Main Gateway fully operational");
    ESP_LOGI(TAG, "Dashboard: http://192.168.4.1   WebSocket: ws://192.168.4.1/ws");
    ESP_LOGI(TAG, "REST:      GET /api/nodes        GET /api/export");
}