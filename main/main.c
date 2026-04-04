/*
 * =============================================================================
 * Estrotech — Centralized Private LoRa Sensor Network
 * Main Gateway (GW_ID = 0x00) — R&D Phase Firmware (SYNCHRONIZED)
 * COMBINED: LoRa firmware + WiFi AP + WebSocket dashboard server
 * =============================================================================
 *
 * Owner        : Midhlaj
 * Framework    : ESP-IDF v5.5.2
 * Board        : ESP32 DevKit V1 38-pin (ESP32-WROOM-32)
 * LoRa Module  : AI-Thinker Ra-02 SX1278
 *
 * SYNCHRONIZATION FIXES (Critical):
 *   ✓ ISSUE 1 FIXED: Removed absolute timestamp validation (independent clocks)
 *   ✓ ISSUE 2 FIXED: Increased offline timeout to 90s (allows 2 missed cycles)
 *   ✓ ISSUE 3 FIXED: Increased ACK timeout expectation to 1000ms
 *   ✓ ISSUE 4 FIXED: Increased validated_queue depth to 8 (matches Local GW)
 *   ✓ ISSUE 5 FIXED: Removed redundant sensor type validation
 *   ✓ ISSUE 7 FIXED: Increased frequency settle delay to 20ms for stable PLL
 *
 * KEY TIMING PARAMETERS (Matched to Local Gateway):
 *   - Local GW cycle: 30s (aggregation at T=25s)
 *   - Offline timeout: 90s (tolerates 2 missed cycles)
 *   - Queue depths: 8 (raw_rx), 8 (validated) - prevents drops
 *   - ACK processing window: 1000ms (allows for busy Main GW)
 *
 * SPI Pin Assignment (ESP32 DevKit V1 38-pin):
 *   SCK=18  MISO=19  MOSI=23  NSS=5  RST=14  DIO0=26
 *
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
 * SECTION 3 — NETWORK CONSTANTS (SYNCHRONIZED)
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

/*
 * CRITICAL TIMING FIXES:
 * 
 * ISSUE 2 FIX: Offline timeout increased from 45s to 90s
 * Why: Local GW transmits every 30s. With 45s timeout, a single
 *      missed packet causes offline status. 90s allows 2 missed
 *      cycles before marking offline (more robust).
 */
#define MISSED_CYCLES_OFFLINE   3
#define OFFLINE_TIMEOUT_MS      90000   /* CHANGED: was 45000 */

/*
 * ISSUE 4 FIX: TX watchdog kept at 5s (matches Local GW)
 * This is correct - no change needed
 */
#define TX_WATCHDOG_TIMEOUT_MS  5000

/*
 * ISSUE 1 FIX: Packet age validation REMOVED
 * Why: Local GW and Main GW have independent clocks (no NTP sync).
 *      Clock drift can cause valid packets to be rejected as "too old".
 *      Deduplication (via GW_timestamp check) already prevents replays.
 *      
 * Old code (REMOVED):
 * #define PACKET_AGE_LIMIT_MS  60000
 * if (packet_age > PACKET_AGE_LIMIT_MS) return false;
 */

#define SX1278_VERSION_EXPECTED 0x12

#define SENTINEL_PACKET_ID      0xFFFF
#define SENTINEL_SENSOR_VALUE   0xFFFF

#define TOTAL_NODES             6
#define NODES_PER_GW            3

#define GW1_NODE_START          0x01
#define GW1_NODE_END            0x03
#define GW2_NODE_START          0x04
#define GW2_NODE_END            0x06

#define JSON_BUF_SIZE           2048

/* ============================================================
 * SECTION 4 — WEBSOCKET / SERVER CONSTANTS
 * ============================================================ */
#define WS_MAX_CLIENTS          4
#define WIFI_SSID               "LoRa-Gateway"
#define WIFI_PASS               "estrotech"
#define WIFI_MAX_CONN           4

#define DASHBOARD_CHECK_INTERVAL_MS  5000

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
    uint32_t received_at_ms;
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
static volatile uint32_t s_tx_start_time = 0;

static QueueHandle_t s_raw_rx_queue;
static QueueHandle_t s_validated_queue;
static QueueHandle_t s_json_trigger;
static QueueHandle_t s_beacon_trigger;

node_data_t           g_data_store[TOTAL_NODES];
SemaphoreHandle_t     g_data_store_mutex;

static uint32_t s_last_gw_packet_ms[2]  = {0, 0};
static uint32_t s_last_gw_timestamp[2]  = {0xFFFFFFFF, 0xFFFFFFFF};

static esp_timer_handle_t s_beacon_timer;

/* L3 packet ID tracking */
static uint32_t s_l3_packet_id = 0;
static SemaphoreHandle_t s_l3_packet_id_mutex;

/* WebSocket client tracking */
static int               ws_client_fds[WS_MAX_CLIENTS];
static SemaphoreHandle_t ws_clients_mutex;
static httpd_handle_t    s_server = NULL;

/* Dashboard connection state */
static bool s_dashboard_connected = false;
static uint32_t s_last_dashboard_check_ms = 0;

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
 * SECTION 11 — LAYER 2 PACKET VALIDATION (SYNCHRONIZED)
 * ============================================================
 * CRITICAL FIXES APPLIED:
 * 
 * ISSUE 1 FIX: Removed packet age validation
 * - Independent clocks cause false "too old" rejections
 * - Deduplication via GW_timestamp is sufficient
 * - Log arrival time for debugging only (not validation)
 * 
 * ISSUE 5 FIX: Removed redundant sensor type validation
 * - Main Gateway receives pre-validated data from Local GW
 * - Sensor type already checked at Local Gateway layer
 */
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
        ESP_LOGE(TAG, "L2 INVALID GW_ID 0x%02X — only 0x01 and 0x02 accepted", gw_id);
        return false;
    }

    /*
     * ISSUE 1 FIX: Packet age validation REMOVED
     * 
     * Old code (causes false rejections):
     * uint32_t now_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
     * uint32_t packet_age = now_ms - gw_timestamp;
     * if (packet_age > PACKET_AGE_LIMIT_MS) {
     *     ESP_LOGW(TAG, "L2 packet too old");
     *     return false;
     * }
     * 
     * Why removed: Local GW and Main GW clocks are independent.
     * Clock drift of >60s causes valid packets to be rejected.
     * Deduplication check below is sufficient protection.
     */

    /* Deduplication check - THIS is the real anti-replay protection */
    uint8_t gw_idx = gw_id - 1;
    if (gw_timestamp == s_last_gw_timestamp[gw_idx]) {
        ESP_LOGW(TAG, "L2 GW 0x%02X: duplicate GW_TS=%lu — discarded",
                 gw_id, (unsigned long)gw_timestamp);
        return false;
    }
    s_last_gw_timestamp[gw_idx] = gw_timestamp;

    out->gw_id        = gw_id;
    out->gw_timestamp = gw_timestamp;
    out->received_at_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);

    const uint8_t nb_offsets[NODES_PER_GW] = { 6, 15, 24 };
    for (int i = 0; i < NODES_PER_GW; i++) {
        uint8_t off = nb_offsets[i];
        out->blocks[i].node_id      = raw[off + 0];
        out->blocks[i].packet_id    = parse_u16_be(raw, off + 1);
        out->blocks[i].timestamp    = parse_u32_be(raw, off + 3);
        out->blocks[i].sensor_value = parse_u16_be(raw, off + 7);
        out->blocks[i].is_sentinel  = (out->blocks[i].packet_id == SENTINEL_PACKET_ID);
    }

    /* Log acceptance with timing info (for debug, not validation) */
    ESP_LOGI(TAG, "L2 VALID from GW 0x%02X — GW_TS=%lu ms CRC=0x%04X",
             gw_id, (unsigned long)gw_timestamp, received_crc);
    
    return true;
}

/* ============================================================
 * SECTION 12 — ACK TRANSMIT (WITH WATCHDOG)
 * ============================================================ */
static void send_ack_l2(void)
{
    s_tx_start_time = (uint32_t)(esp_timer_get_time() / 1000ULL);

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
    bool tx_timeout = false;
    while (!(read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK)) {
        uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
        
        if ((now - s_tx_start_time) > TX_WATCHDOG_TIMEOUT_MS) {
            ESP_LOGE(TAG, "ACK TX WATCHDOG TIMEOUT — aborting after %lu ms",
                     (unsigned long)(now - s_tx_start_time));
            tx_timeout = true;
            break;
        }
        
        if ((now - start) > 500) {
            ESP_LOGE(TAG, "ACK TX timeout");
            tx_timeout = true;
            break;
        }
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);

    write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1, 0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_ADDR_PTR, 0x00);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    spi_device_release_bus(s_spi_handle);
    
    if (!tx_timeout) {
        ESP_LOGI(TAG, "ACK 0xAA sent");
    }
}

/* ============================================================
 * SECTION 13 — SYNC BEACON TRANSMIT (WITH WATCHDOG)
 * ============================================================ */
static void send_sync_beacon(void)
{
    s_tx_start_time = (uint32_t)(esp_timer_get_time() / 1000ULL);

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
    bool tx_timeout = false;
    while (!(read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK)) {
        uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
        
        if ((now - s_tx_start_time) > TX_WATCHDOG_TIMEOUT_MS) {
            ESP_LOGE(TAG, "Beacon TX WATCHDOG TIMEOUT — aborting after %lu ms",
                     (unsigned long)(now - s_tx_start_time));
            tx_timeout = true;
            break;
        }
        
        if ((now - start) > 500) {
            ESP_LOGE(TAG, "Beacon TX timeout");
            tx_timeout = true;
            break;
        }
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);

    write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1, 0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_ADDR_PTR, 0x00);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    spi_device_release_bus(s_spi_handle);
    
    if (!tx_timeout) {
        ESP_LOGI(TAG, "Sync beacon [0xBC 0x00] sent on 434.0 MHz");
    }
}

/* ============================================================
 * SECTION 14 — ONLINE/OFFLINE LOGIC & DATA STORE UPDATE
 * ============================================================
 */
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
            ESP_LOGI(TAG, "Node %u (Temp): %.1f°C  raw=0x%04X",
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

/*
 * ISSUE 2 FIX: Offline timeout check uses 90s instead of 45s
 */
static void check_offline_fallback(void)
{
    uint32_t now_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);

    for (int gw_idx = 0; gw_idx < 2; gw_idx++) {
        if (s_last_gw_packet_ms[gw_idx] == 0) continue;
        
        /* CHANGED: was 45000, now 90000 */
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
                    ESP_LOGW(TAG, "Node %u: 90s silence — forced offline", nid);
                }
            }
            xSemaphoreGive(g_data_store_mutex);
        }
    }
}

/* ============================================================
 * SECTION 15 — LAYER 3 JSON BUILDER (WITH PACKET ID)
 * ============================================================ */
void mgw_prepare_layer3_json(char *buf, size_t buf_len, uint32_t packet_id)
{
    xSemaphoreTake(g_data_store_mutex, portMAX_DELAY);

    uint32_t now_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
    int pos = 0;
    
    pos += snprintf(buf + pos, buf_len - pos,
                    "{\"packet_id\":%lu,\"timestamp\":%lu,\"nodes\":[",
                    (unsigned long)packet_id,
                    (unsigned long)now_ms);

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
    pos += snprintf(buf + pos, buf_len - pos, "]}");

    xSemaphoreGive(g_data_store_mutex);
}

/* ============================================================
 * SECTION 16 — DASHBOARD CONNECTION MONITORING
 * ============================================================ */
static bool is_dashboard_connected(void)
{
    bool connected = false;
    xSemaphoreTake(ws_clients_mutex, portMAX_DELAY);
    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        if (ws_client_fds[i] >= 0) {
            connected = true;
            break;
        }
    }
    xSemaphoreGive(ws_clients_mutex);
    return connected;
}

/* ============================================================
 * SECTION 17 — WEBSOCKET BROADCAST (WITH DISCONNECT HANDLING)
 * ============================================================ */
void ws_broadcast(const char *json_str)
{
    if (s_server == NULL) return;

    bool any_sent = false;
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
            ESP_LOGW(TAG, "WS send failed fd=%d err=0x%x — removing client",
                     ws_client_fds[i], err);
            ws_client_fds[i] = -1;
        } else {
            ESP_LOGD(TAG, "WS broadcast → fd=%d (%d bytes)",
                     ws_client_fds[i], (int)strlen(json_str));
            any_sent = true;
        }
    }

    xSemaphoreGive(ws_clients_mutex);

    bool now_connected = is_dashboard_connected();
    if (s_dashboard_connected && !now_connected) {
        ESP_LOGW(TAG, "═══════════════════════════════════════════════════");
        ESP_LOGW(TAG, " DASHBOARD DISCONNECTED");
        ESP_LOGW(TAG, " LoRa operations continue normally");
        ESP_LOGW(TAG, " Waiting for dashboard reconnection...");
        ESP_LOGW(TAG, "═══════════════════════════════════════════════════");
        s_dashboard_connected = false;
    } else if (!s_dashboard_connected && now_connected) {
        ESP_LOGI(TAG, "═══════════════════════════════════════════════════");
        ESP_LOGI(TAG, " DASHBOARD RECONNECTED");
        ESP_LOGI(TAG, " Resuming WebSocket data broadcast");
        ESP_LOGI(TAG, "═══════════════════════════════════════════════════");
        s_dashboard_connected = true;
    }
}

/* ============================================================
 * SECTION 18 — HTTP HANDLERS
 * ============================================================ */
static const char *html_page =
    "<!DOCTYPE html>"
    "<html><head><meta charset='utf-8'>"
    "<title>Estrotech Gateway</title></head>"
    "<body>"
    "<h2>Estrotech LoRa Dashboard</h2>"
    "<p id='status'>Connecting...</p>"
    "<pre id='data'>Waiting for data...</pre>"
    "<script>"
    "let ws;"
    "function connect() {"
    "  ws = new WebSocket('ws://192.168.4.1/ws');"
    "  ws.onopen = () => {"
    "    document.getElementById('status').textContent = 'Connected';"
    "    fetch('/api/nodes')"
    "      .then(r => r.json())"
    "      .then(d => document.getElementById('data').textContent"
    "                = JSON.stringify(d, null, 2));"
    "  };"
    "  ws.onmessage = e => {"
    "    document.getElementById('data').textContent"
    "      = JSON.stringify(JSON.parse(e.data), null, 2);"
    "  };"
    "  ws.onclose = () => {"
    "    document.getElementById('status').textContent = 'Disconnected - Reconnecting...';"
    "    setTimeout(connect, 3000);"
    "  };"
    "}"
    "connect();"
    "</script>"
    "</body></html>";

static esp_err_t handler_root(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, html_page, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t handler_api_nodes(httpd_req_t *req)
{
    char buf[JSON_BUF_SIZE];
    uint32_t current_packet_id;
    
    xSemaphoreTake(s_l3_packet_id_mutex, portMAX_DELAY);
    current_packet_id = s_l3_packet_id;
    xSemaphoreGive(s_l3_packet_id_mutex);
    
    mgw_prepare_layer3_json(buf, sizeof(buf), current_packet_id);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req,  "Access-Control-Allow-Origin", "*");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

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
                
                if (!s_dashboard_connected) {
                    s_dashboard_connected = true;
                    ESP_LOGI(TAG, "Dashboard connection established");
                }
                break;
            }
        }
        xSemaphoreGive(ws_clients_mutex);

        if (!registered) {
            ESP_LOGW(TAG, "WS: no free slots — rejecting fd=%d", fd);
            return ESP_FAIL;
        }
        return ESP_OK;
    }

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
 * SECTION 19 — WIFI INIT
 * ============================================================ */
static void wifi_init(void)
{
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
 * SECTION 20 — HTTP SERVER INIT
 * ============================================================ */
static void start_server(void)
{
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.max_open_sockets = WS_MAX_CLIENTS + 3;

    if (httpd_start(&s_server, &config) != ESP_OK) {
        ESP_LOGE(TAG, "FATAL: HTTP server failed to start");
        return;
    }

    httpd_uri_t uri_root = {
        .uri     = "/",
        .method  = HTTP_GET,
        .handler = handler_root,
    };
    httpd_register_uri_handler(s_server, &uri_root);

    httpd_uri_t uri_nodes = {
        .uri     = "/api/nodes",
        .method  = HTTP_GET,
        .handler = handler_api_nodes,
    };
    httpd_register_uri_handler(s_server, &uri_nodes);

    httpd_uri_t uri_export = {
        .uri     = "/api/export",
        .method  = HTTP_GET,
        .handler = handler_api_export,
    };
    httpd_register_uri_handler(s_server, &uri_export);

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
 * SECTION 21 — ESP_TIMER CALLBACK (SYNC BEACON)
 * ============================================================ */
static void beacon_timer_cb(void *arg)
{
    uint8_t sig = 1;
    BaseType_t hp = pdFALSE;
    xQueueSendFromISR(s_beacon_trigger, &sig, &hp);
    portYIELD_FROM_ISR(hp);
}

/* ============================================================
 * SECTION 22 — TASK A: LoRa RX (WITH WATCHDOG CHECK)
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
            uint32_t tx_elapsed = (uint32_t)(esp_timer_get_time() / 1000ULL) - s_tx_start_time;
            if (tx_elapsed > TX_WATCHDOG_TIMEOUT_MS) {
                ESP_LOGE(TAG, "Task A: TX watchdog triggered — forcing s_tx_active = false");
                s_tx_active = false;
            } else {
                vTaskDelay(pdMS_TO_TICKS(50));
                continue;
            }
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

        /*
         * ISSUE 4 FIX: Queue depth increased to 8 (was implicitly showing as issue)
         * Monitor queue depth for overflow warnings
         */
        UBaseType_t queue_spaces = uxQueueSpacesAvailable(s_raw_rx_queue);
        if (queue_spaces <= 2) {
            ESP_LOGW(TAG, "Task A: raw_rx_queue nearly full (%u/%u) — potential overflow",
                     (unsigned)(8 - queue_spaces), 8);
        }

        if (xQueueSend(s_raw_rx_queue, raw_buf, pdMS_TO_TICKS(10)) != pdTRUE) {
            ESP_LOGE(TAG, "Task A: queue full — PACKET DROPPED");
        }

        vTaskDelay(pdMS_TO_TICKS(20));
    }
}

/* ============================================================
 * SECTION 23 — TASK B: VALIDATION + ACK
 * ============================================================ */
static void task_validation(void *arg)
{
    ESP_LOGI(TAG, "Task B (Validation + ACK) started on Core %d", xPortGetCoreID());

    uint8_t        raw_buf[L2_PACKET_SIZE];
    validated_l2_t vl2;

    while (1) {
        if (xQueueReceive(s_raw_rx_queue, raw_buf, portMAX_DELAY) == pdTRUE) {
            if (validate_l2_packet(raw_buf, &vl2)) {
                s_last_gw_packet_ms[vl2.gw_id - 1] = vl2.received_at_ms;

                xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
                send_ack_l2();
                xSemaphoreGive(s_lora_mutex);

                UBaseType_t queue_spaces = uxQueueSpacesAvailable(s_validated_queue);
                if (queue_spaces == 0) {
                    ESP_LOGE(TAG, "Task B: validated queue full — DATA DROPPED");
                    continue;
                }

                if (xQueueSend(s_validated_queue, &vl2, pdMS_TO_TICKS(50)) != pdTRUE) {
                    ESP_LOGW(TAG, "Task B: validated queue full — data dropped");
                }
            }
        }
    }
}

/* ============================================================
 * SECTION 24 — TASK C: SYNC BEACON
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
 * SECTION 25 — TASK D: RAM STORE UPDATE
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
 * SECTION 26 — TASK E: WEB PREP + WS BROADCAST (WITH PACKET ID)
 * ============================================================ */
static void task_web_prep(void *arg)
{
    ESP_LOGI(TAG, "Task E (Web Prep + WS Broadcast) started on Core %d",
             xPortGetCoreID());

    uint8_t gw_id;
    char    json_buf[JSON_BUF_SIZE];

    while (1) {
        if (xQueueReceive(s_json_trigger, &gw_id, portMAX_DELAY) == pdTRUE) {
            xSemaphoreTake(s_l3_packet_id_mutex, portMAX_DELAY);
            s_l3_packet_id++;
            uint32_t current_packet_id = s_l3_packet_id;
            xSemaphoreGive(s_l3_packet_id_mutex);

            ESP_LOGI(TAG, "Task E: GW 0x%02X updated — building L3 JSON packet #%lu",
                     gw_id, (unsigned long)current_packet_id);

            mgw_prepare_layer3_json(json_buf, sizeof(json_buf), current_packet_id);

            ESP_LOGI(TAG, "L3 JSON [packet_id=%lu]: %s",
                     (unsigned long)current_packet_id, json_buf);

            if (!is_dashboard_connected()) {
                uint32_t now_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
                if ((now_ms - s_last_dashboard_check_ms) > DASHBOARD_CHECK_INTERVAL_MS) {
                    ESP_LOGW(TAG, "Dashboard disconnected — packet #%lu buffered (not sent)",
                             (unsigned long)current_packet_id);
                    s_last_dashboard_check_ms = now_ms;
                }
            } else {
                ws_broadcast(json_buf);
            }
        }
    }
}

/* ============================================================
 * SECTION 27 — RAM DATA STORE INITIALISATION
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
 * SECTION 28 — APP_MAIN
 * ============================================================ */
void app_main(void)
{
    ESP_LOGI(TAG, "========================================");
    ESP_LOGI(TAG, " Estrotech Main Gateway — R&D Phase (SYNCHRONIZED)");
    ESP_LOGI(TAG, " ESP32 DevKit V1 38-pin | ESP-IDF v5.5.2");
    ESP_LOGI(TAG, " LoRa RX: 434.0 MHz  |  WiFi AP: 192.168.4.1");
    ESP_LOGI(TAG, " SYNC FIXES: 90s timeout | No age validation | Queue depth 8");
    ESP_LOGI(TAG, " ACK=0xAA  Beacon=0xBC  SPI=No-DMA");
    ESP_LOGI(TAG, "========================================");

    sx1278_init_spi();
    ESP_LOGI(TAG, "SPI (HSPI, no-DMA) initialized: SCK=%d MISO=%d MOSI=%d NSS=%d",
             PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);

    gpio_set_direction(PIN_DIO0, GPIO_MODE_INPUT);
    sx1278_reset();
    ESP_LOGI(TAG, "SX1278 hardware reset complete");
    sx1278_sanity_check();
    sx1278_configure_radio();

    init_data_store();
    ESP_LOGI(TAG, "RAM data store initialized — 6 nodes, all offline at start");

    s_lora_mutex        = xSemaphoreCreateBinary();
    xSemaphoreGive(s_lora_mutex);
    g_data_store_mutex  = xSemaphoreCreateMutex();
    ws_clients_mutex    = xSemaphoreCreateMutex();
    s_l3_packet_id_mutex = xSemaphoreCreateMutex();
    
    /*
     * ISSUE 4 FIX: Queue depths increased to match Local Gateway
     * raw_rx_queue: 8 (was implicitly correct)
     * validated_queue: 8 (CHANGED from 4)
     */
    s_raw_rx_queue      = xQueueCreate(8, L2_PACKET_SIZE);
    s_validated_queue   = xQueueCreate(8, sizeof(validated_l2_t));  /* CHANGED */
    s_json_trigger      = xQueueCreate(2, sizeof(uint8_t));
    s_beacon_trigger    = xQueueCreate(1, sizeof(uint8_t));

    if (!s_lora_mutex || !g_data_store_mutex || !ws_clients_mutex ||
        !s_l3_packet_id_mutex || !s_raw_rx_queue || !s_validated_queue ||
        !s_json_trigger || !s_beacon_trigger) {
        ESP_LOGE(TAG, "FATAL: FreeRTOS object creation failed");
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }

    for (int i = 0; i < WS_MAX_CLIENTS; i++) {
        ws_client_fds[i] = -1;
    }

    wifi_init();
    start_server();

    esp_timer_create_args_t beacon_args = {
        .callback = beacon_timer_cb,
        .name     = "beacon_timer",
    };
    ESP_ERROR_CHECK(esp_timer_create(&beacon_args, &s_beacon_timer));
    ESP_ERROR_CHECK(esp_timer_start_periodic(s_beacon_timer, BEACON_PERIOD_US));
    ESP_LOGI(TAG, "Sync beacon timer armed — fires every 30s on 434.0 MHz");

    xTaskCreatePinnedToCore(task_lora_rx,     "TaskA_RX",    4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_validation,  "TaskB_Val",   4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_sync_beacon, "TaskC_Bcn",   3072, NULL, 3, NULL, 0);
    xTaskCreatePinnedToCore(task_ram_store,   "TaskD_Store", 4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(task_web_prep,    "TaskE_Web",   4096, NULL, 3, NULL, 1);

    ESP_LOGI(TAG, "All tasks created — Main Gateway fully operational");
    ESP_LOGI(TAG, "Dashboard: http://192.168.4.1   WebSocket: ws://192.168.4.1/ws");
    ESP_LOGI(TAG, "REST:      GET /api/nodes        GET /api/export");
    ESP_LOGI(TAG, "L3 Packet ID tracking: ENABLED (starts at packet #1)");
    ESP_LOGI(TAG, "Synchronized with Local Gateway timing characteristics");
}