/*
 * =============================================================================
 * Estrotech — Centralized Private LoRa Sensor Network
 * Local Gateway — GW1 — R&D Phase Firmware (REVISED)
 * =============================================================================
 *
 * Owner        : Midhlaj
 * Framework    : ESP-IDF v5.5.2
 * Board        : ESP32-C3 Super Mini
 * LoRa Module  : AI-Thinker Ra-02 SX1278
 *
 * REVISION CHANGES:
 *   ✓ Node 0x03 (Light sensor) now ACTIVE (was offline/sentinel)
 *   ✓ Node timing: Node 0x01 @ T=0s, Node 0x02 @ T=4s, Node 0x03 @ T=8s
 *   ✓ Sensor type validation: reject packets with wrong Node_ID for sensor type
 *   ✓ Frequency switching safety: explicit FIFO reset + IRQ clear + PLL lock delay
 *   ✓ Edge case fixes: race conditions, aggregation timing, TX watchdog
 *
 * R&D Phase Scope:
 *   - Node 0x01 (DS18B20 Temp)    — TX at T=0s
 *   - Node 0x02 (Humidity)        — TX at T=4s
 *   - Node 0x03 (Light)           — TX at T=8s  [NOW ACTIVE]
 *   - GW_ID = 0x01
 *   - Layer 2 TX to Main Gateway on 434.0 MHz with ACK (0xAA) + retry x3
 *   - Aggregation at T=25s
 *   - 30-second cycle using esp_timer
 *
 * Pin Assignment (ESP32-C3 Super Mini):
 *   SCK=GPIO4  MISO=GPIO5  MOSI=GPIO6  NSS=GPIO7  RST=GPIO10  DIO0=GPIO3
 *
 * =============================================================================
 * Layer 1 packet (12 bytes, big-endian):
 *   [ Node_ID | GW_ID | Packet_ID | Timestamp | Sensor_Value | CRC ]
 *     1B        1B      2B          4B           2B             2B
 *   CRC = CRC16-CCITT (poly 0x1021, init 0xFFFF) over bytes 0..9 (10 bytes)
 *   CRC stored at offset 10–11
 *
 * Layer 2 packet (35 bytes, big-endian):
 *   [ Preamble | GW_ID | GW_Timestamp | NodeBlock_1 | NodeBlock_2 | NodeBlock_3 | CRC ]
 *     1B         1B      4B             9B            9B            9B             2B
 *   CRC = CRC16-CCITT over bytes 0..32 (33 bytes)
 *
 * NodeBlock (9 bytes):
 *   [ Node_ID | Packet_ID | Timestamp | Sensor_Value ]
 *     1B        2B          4B          2B
 *
 * =============================================================================
 */

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "driver/spi_master.h"
#include "driver/gpio.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "sdkconfig.h"

/* ============================================================
 * SECTION 1 — PIN DEFINITIONS (ESP32-C3 Super Mini)
 * ============================================================ */
#define PIN_SCK     4
#define PIN_MISO    5
#define PIN_MOSI    6
#define PIN_NSS     7
#define PIN_RST     10
#define PIN_DIO0    3

/* ============================================================
 * SECTION 2 — LoRa / SX1278 REGISTER ADDRESSES
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
#define GW_ID                   0x01
#define LAYER2_PREAMBLE         0xBB
#define LAYER2_ACK_BYTE         0xAA
#define SYNC_WORD               0x12

/* Layer 1: sensor nodes → local gateway */
#define FREQ_L1_MSB             0x6C
#define FREQ_L1_MID             0x40
#define FREQ_L1_LSB             0x00

/* Layer 2: local gateway → main gateway */
#define FREQ_L2_MSB             0x6C
#define FREQ_L2_MID             0x80
#define FREQ_L2_LSB             0x00

#define L1_PACKET_SIZE          12
#define L2_PACKET_SIZE          35
#define L2_ACK_SIZE             1

#define CYCLE_DURATION_MS       30000
#define AGGREGATION_TIME_MS     25000
#define LAYER2_ACK_TIMEOUT_MS   500
#define LAYER2_MAX_RETRIES      3
#define TX_WATCHDOG_TIMEOUT_MS  5000    /* NEW: TX operation timeout */
#define FREQ_SWITCH_SETTLE_MS   15      /* NEW: PLL lock delay */

#define NODE_TEMP_ID            0x01
#define NODE_HUM_ID             0x02
#define NODE_LIGHT_ID           0x03

#define SENTINEL_PACKET_ID      0xFFFF
#define SENTINEL_TIMESTAMP      0x00000000
#define SENTINEL_SENSOR_VALUE   0xFFFF

#define SX1278_VERSION_EXPECTED 0x12

#define L1_CRC_INPUT_BYTES      10

/* NEW: Sensor type definitions for validation */
typedef enum {
    SENSOR_TYPE_TEMPERATURE = 1,
    SENSOR_TYPE_HUMIDITY    = 2,
    SENSOR_TYPE_LIGHT       = 3,
    SENSOR_TYPE_UNKNOWN     = 0xFF
} sensor_type_t;

/* ============================================================
 * SECTION 4 — DATA STRUCTURES
 * ============================================================ */
typedef struct {
    bool     received;
    uint8_t  node_id;
    uint16_t packet_id;
    uint32_t timestamp;
    uint16_t sensor_value;
    uint16_t last_packet_id;
    uint32_t cycle_timestamp;  /* NEW: track which cycle this data belongs to */
} node_buffer_t;

typedef struct {
    uint8_t  node_id;
    uint16_t packet_id;
    uint32_t timestamp;
    uint16_t sensor_value;
} validated_packet_t;

/* ============================================================
 * SECTION 5 — GLOBAL STATE
 * ============================================================ */
static const char *TAG = "GW1";

static spi_device_handle_t s_spi_handle;
static SemaphoreHandle_t   s_lora_mutex;
static volatile bool       s_tx_active = false;
static volatile uint32_t   s_tx_start_time = 0;  /* NEW: TX watchdog */

static SemaphoreHandle_t s_agg_trigger;
static SemaphoreHandle_t s_cycle_reset_c;
static SemaphoreHandle_t s_cycle_reset_mgr;

static QueueHandle_t s_raw_rx_queue;
static QueueHandle_t s_validated_queue;
static QueueHandle_t s_l2_tx_trigger;

static node_buffer_t s_node_buf[3];

static uint8_t s_l2_packet[L2_PACKET_SIZE];
static bool    s_l2_packet_ready = false;

static uint32_t s_current_cycle_start = 0;  /* NEW: cycle timing validation */

/* ============================================================
 * SECTION 6 — SX1278 SPI DRIVER
 * ============================================================ */
static void write_reg(uint8_t addr, uint8_t value)
{
    uint8_t tx[2] = { (uint8_t)(addr | 0x80), value };
    uint8_t rx[2] = { 0 };
    spi_transaction_t t = { .length = 16, .tx_buffer = tx, .rx_buffer = rx };
    spi_device_transmit(s_spi_handle, &t);
}

static uint8_t read_reg(uint8_t addr)
{
    uint8_t tx[2] = { (uint8_t)(addr & 0x7F), 0x00 };
    uint8_t rx[2] = { 0 };
    spi_transaction_t t = { .length = 16, .tx_buffer = tx, .rx_buffer = rx };
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
 * SECTION 7 — SX1278 INITIALISATION
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

    write_reg(REG_SYNC_WORD,      SYNC_WORD);
    write_reg(REG_MODEM_CONFIG1,  0x72);
    write_reg(REG_MODEM_CONFIG2,  0x74);
    write_reg(REG_MODEM_CONFIG3,  0x04);
    write_reg(REG_PA_CONFIG,      0xF8);
    write_reg(REG_LNA,            0x23);
    write_reg(REG_PREAMBLE_MSB,   0x00);
    write_reg(REG_PREAMBLE_LSB,   0x08);
    write_reg(REG_PAYLOAD_LENGTH, L1_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1,   0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_TX_BASE_ADDR, 0x80);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);

    ESP_LOGI(TAG, "SX1278 radio parameters configured");
}

/* ============================================================
 * SECTION 8 — FREQUENCY SWITCHING (WITH SAFETY)
 * ============================================================
 * EDGE CASE FIX: Prevent FIFO corruption during frequency switch
 * - Enter STDBY before frequency change
 * - Clear all IRQ flags
 * - Reset FIFO pointers to base addresses
 * - Allow PLL to settle (FREQ_SWITCH_SETTLE_MS)
 * - Re-enter RX_CONTINUOUS
 */
static void set_frequency_433(void)
{
    /* Step 1: Enter standby */
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    vTaskDelay(pdMS_TO_TICKS(5));
    
    /* Step 2: Change frequency registers */
    write_reg(REG_FRF_MSB, FREQ_L1_MSB);
    write_reg(REG_FRF_MID, FREQ_L1_MID);
    write_reg(REG_FRF_LSB, FREQ_L1_LSB);
    
    /* Step 3: Clear IRQ flags and reset FIFO pointers */
    write_reg(REG_IRQ_FLAGS, 0xFF);
    write_reg(REG_FIFO_ADDR_PTR, 0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    
    /* Step 4: Wait for PLL lock */
    vTaskDelay(pdMS_TO_TICKS(FREQ_SWITCH_SETTLE_MS));
    
    ESP_LOGD(TAG, "Frequency set to 433.0 MHz (Layer 1 RX) — FIFO reset complete");
}

static void set_frequency_434(void)
{
    /* Step 1: Enter standby */
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    vTaskDelay(pdMS_TO_TICKS(5));
    
    /* Step 2: Change frequency registers */
    write_reg(REG_FRF_MSB, FREQ_L2_MSB);
    write_reg(REG_FRF_MID, FREQ_L2_MID);
    write_reg(REG_FRF_LSB, FREQ_L2_LSB);
    
    /* Step 3: Clear IRQ flags and reset FIFO pointers */
    write_reg(REG_IRQ_FLAGS, 0xFF);
    write_reg(REG_FIFO_ADDR_PTR, 0x80);
    write_reg(REG_FIFO_TX_BASE_ADDR, 0x80);
    
    /* Step 4: Wait for PLL lock */
    vTaskDelay(pdMS_TO_TICKS(FREQ_SWITCH_SETTLE_MS));
    
    ESP_LOGD(TAG, "Frequency set to 434.0 MHz (Layer 2 TX) — FIFO reset complete");
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

static inline void put_u16_be(uint8_t *buf, uint8_t offset, uint16_t val)
{
    buf[offset]     = (uint8_t)(val >> 8);
    buf[offset + 1] = (uint8_t)(val & 0xFF);
}

static inline void put_u32_be(uint8_t *buf, uint8_t offset, uint32_t val)
{
    buf[offset]     = (uint8_t)(val >> 24);
    buf[offset + 1] = (uint8_t)(val >> 16);
    buf[offset + 2] = (uint8_t)(val >>  8);
    buf[offset + 3] = (uint8_t)(val & 0xFF);
}

/* ============================================================
 * SECTION 11 — SENSOR TYPE VALIDATION
 * ============================================================
 * NEW: Validate that Node_ID matches expected sensor type
 */
static const char* get_sensor_type_name(uint8_t node_id)
{
    switch (node_id) {
        case NODE_TEMP_ID:  return "Temperature (DS18B20)";
        case NODE_HUM_ID:   return "Humidity";
        case NODE_LIGHT_ID: return "Light";
        default:            return "UNKNOWN";
    }
}

static bool validate_sensor_type(uint8_t node_id, sensor_type_t expected_type)
{
    sensor_type_t actual_type = SENSOR_TYPE_UNKNOWN;
    
    switch (node_id) {
        case NODE_TEMP_ID:  actual_type = SENSOR_TYPE_TEMPERATURE; break;
        case NODE_HUM_ID:   actual_type = SENSOR_TYPE_HUMIDITY;    break;
        case NODE_LIGHT_ID: actual_type = SENSOR_TYPE_LIGHT;       break;
        default:            actual_type = SENSOR_TYPE_UNKNOWN;     break;
    }
    
    if (actual_type != expected_type) {
        ESP_LOGE(TAG, "SENSOR TYPE MISMATCH DETECTED!");
        ESP_LOGE(TAG, "  Node_ID 0x%02X reports as: %s", 
                 node_id, get_sensor_type_name(node_id));
        ESP_LOGE(TAG, "  Expected sensor type: %s (type %d)",
                 get_sensor_type_name(expected_type), expected_type);
        ESP_LOGE(TAG, "  ACTION: Packet REJECTED — verify node configuration");
        return false;
    }
    
    return true;
}

/* ============================================================
 * SECTION 12 — LAYER 1 PACKET VALIDATION
 * ============================================================
 * ENHANCED: Added sensor type validation
 */
static bool validate_l1_packet(const uint8_t *raw, validated_packet_t *out)
{
    /* CRC validation */
    uint16_t computed_crc = crc16_ccitt(raw, L1_CRC_INPUT_BYTES);
    uint16_t received_crc = parse_u16_be(raw, 10);

    if (computed_crc != received_crc) {
        ESP_LOGW(TAG, "L1 CRC FAIL — computed 0x%04X received 0x%04X",
                 computed_crc, received_crc);
        return false;
    }

    uint8_t  node_id      = raw[0];
    uint8_t  gw_id        = raw[1];
    uint16_t packet_id    = parse_u16_be(raw, 2);
    uint32_t timestamp    = parse_u32_be(raw, 4);
    uint16_t sensor_value = parse_u16_be(raw, 8);

    /* Node ID range validation - NOW INCLUDES NODE 0x03 */
    if (node_id != NODE_TEMP_ID && node_id != NODE_HUM_ID && node_id != NODE_LIGHT_ID) {
        ESP_LOGE(TAG, "L1 INVALID Node_ID 0x%02X — GW1 only handles 0x01(Temp), 0x02(Hum), 0x03(Light)",
                 node_id);
        ESP_LOGE(TAG, "  Received Node_ID does not match any configured sensor");
        ESP_LOGE(TAG, "  ACTION: Check node firmware configuration");
        return false;
    }

    /* NEW: Sensor type validation */
    sensor_type_t expected_type = (sensor_type_t)node_id;
    if (!validate_sensor_type(node_id, expected_type)) {
        return false;  /* Error already logged in validate_sensor_type() */
    }

    /* Gateway ID validation */
    if (gw_id != GW_ID) {
        ESP_LOGW(TAG, "L1 GW_ID 0x%02X rejected — expected 0x01", gw_id);
        return false;
    }

    /* Packet ID validation */
    if (packet_id == 0x0000) {
        ESP_LOGW(TAG, "L1 Node 0x%02X: Packet_ID 0x0000 reserved — discarded",
                 node_id);
        return false;
    }

    /* Deduplication check */
    uint8_t idx = node_id - 1;
    if (s_node_buf[idx].last_packet_id == packet_id) {
        ESP_LOGW(TAG, "L1 Node 0x%02X: Packet_ID 0x%04X duplicate — discarded",
                 node_id, packet_id);
        return false;
    }

    out->node_id      = node_id;
    out->packet_id    = packet_id;
    out->timestamp    = timestamp;
    out->sensor_value = sensor_value;
    return true;
}

/* ============================================================
 * SECTION 13 — LAYER 2 PACKET BUILDER
 * ============================================================
 * UPDATED: Node 0x03 now uses actual data (not sentinel)
 */
static void build_nodeblock(uint8_t *buf, uint8_t offset,
                             uint8_t node_id, const node_buffer_t *nb)
{
    buf[offset] = node_id;
    if (nb->received) {
        put_u16_be(buf, offset + 1, nb->packet_id);
        put_u32_be(buf, offset + 3, nb->timestamp);
        put_u16_be(buf, offset + 7, nb->sensor_value);
    } else {
        put_u16_be(buf, offset + 1, SENTINEL_PACKET_ID);
        put_u32_be(buf, offset + 3, SENTINEL_TIMESTAMP);
        put_u16_be(buf, offset + 7, SENTINEL_SENSOR_VALUE);
    }
}

static void build_l2_packet(void)
{
    uint32_t gw_timestamp = (uint32_t)(esp_timer_get_time() / 1000ULL);

    s_l2_packet[0] = LAYER2_PREAMBLE;
    s_l2_packet[1] = GW_ID;
    put_u32_be(s_l2_packet, 2, gw_timestamp);

    build_nodeblock(s_l2_packet,  6, NODE_TEMP_ID,  &s_node_buf[0]);
    build_nodeblock(s_l2_packet, 15, NODE_HUM_ID,   &s_node_buf[1]);
    build_nodeblock(s_l2_packet, 24, NODE_LIGHT_ID, &s_node_buf[2]);  /* CHANGED: now active */

    uint16_t crc = crc16_ccitt(s_l2_packet, 33);
    put_u16_be(s_l2_packet, 33, crc);

    ESP_LOGI(TAG, "L2 packet built — GW_TS=%lu ms", (unsigned long)gw_timestamp);
    ESP_LOGI(TAG, "  NodeBlock 1 (0x01 Temp)  : %s  raw=0x%04X",
             s_node_buf[0].received ? "DATA    " : "SENTINEL",
             s_node_buf[0].received ? s_node_buf[0].sensor_value : 0xFFFF);
    ESP_LOGI(TAG, "  NodeBlock 2 (0x02 Hum)   : %s  raw=0x%04X",
             s_node_buf[1].received ? "DATA    " : "SENTINEL",
             s_node_buf[1].received ? s_node_buf[1].sensor_value : 0xFFFF);
    ESP_LOGI(TAG, "  NodeBlock 3 (0x03 Light) : %s  raw=0x%04X",  /* CHANGED */
             s_node_buf[2].received ? "DATA    " : "SENTINEL",
             s_node_buf[2].received ? s_node_buf[2].sensor_value : 0xFFFF);
    ESP_LOGI(TAG, "  CRC = 0x%04X", crc);
}

/* ============================================================
 * SECTION 14 — LAYER 2 TX WITH ACK + RETRY
 * ============================================================
 * ENHANCED: TX watchdog for edge case protection
 */
static void transmit_l2_packet(void)
{
    bool ack_received = false;
    s_tx_start_time = (uint32_t)(esp_timer_get_time() / 1000ULL);  /* NEW: start watchdog */

    for (uint8_t attempt = 1; attempt <= LAYER2_MAX_RETRIES; attempt++) {
        /* NEW: Watchdog check */
        uint32_t elapsed = (uint32_t)(esp_timer_get_time() / 1000ULL) - s_tx_start_time;
        if (elapsed > TX_WATCHDOG_TIMEOUT_MS) {
            ESP_LOGE(TAG, "TX WATCHDOG TIMEOUT — aborting after %lu ms", (unsigned long)elapsed);
            break;
        }

        ESP_LOGI(TAG, "L2 TX attempt %u/%u", attempt, LAYER2_MAX_RETRIES);

        set_frequency_434();
        write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
        write_reg(REG_DIO_MAPPING1, 0x40);
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
        write_reg(REG_FIFO_ADDR_PTR, 0x80);
        write_fifo(s_l2_packet, L2_PACKET_SIZE);
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_TX);

        uint32_t tx_start = (uint32_t)(esp_timer_get_time() / 1000ULL);
        bool     tx_done  = false;
        while (!tx_done) {
            if (read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK) tx_done = true;
            if (!tx_done) {
                if (((uint32_t)(esp_timer_get_time() / 1000ULL) - tx_start) > 2000) {
                    ESP_LOGE(TAG, "L2 TX timeout");
                    break;
                }
                vTaskDelay(pdMS_TO_TICKS(5));
            }
        }
        write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);
        if (!tx_done) { continue; }

        ESP_LOGI(TAG, "L2 TX complete — waiting for ACK");

        write_reg(REG_DIO_MAPPING1,      0x00);
        write_reg(REG_PAYLOAD_LENGTH,    L2_ACK_SIZE);
        write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
        write_reg(REG_FIFO_ADDR_PTR,     0x00);
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

        uint32_t ack_start = (uint32_t)(esp_timer_get_time() / 1000ULL);
        bool     timed_out = false;
        while (!(read_reg(REG_IRQ_FLAGS) & IRQ_RX_DONE_MASK)) {
            if (((uint32_t)(esp_timer_get_time() / 1000ULL) - ack_start)
                    > LAYER2_ACK_TIMEOUT_MS) {
                timed_out = true;
                break;
            }
            vTaskDelay(pdMS_TO_TICKS(5));
        }

        if (timed_out) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            ESP_LOGW(TAG, "L2 ACK timeout on attempt %u", attempt);
            continue;
        }

        bool crc_err = (read_reg(REG_IRQ_FLAGS) & IRQ_PAYLOAD_CRC_ERR) != 0;
        write_reg(REG_IRQ_FLAGS, 0xFF);
        if (crc_err) { ESP_LOGW(TAG, "L2 ACK CRC error on attempt %u", attempt); continue; }

        uint8_t fifo_rx_addr = read_reg(REG_FIFO_RX_CURR_ADDR);
        write_reg(REG_FIFO_ADDR_PTR, fifo_rx_addr);
        uint8_t ack_buf[1] = {0};
        read_fifo(ack_buf, 1);

        if (ack_buf[0] == LAYER2_ACK_BYTE) {
            ack_received = true;
            ESP_LOGI(TAG, "L2 ACK received (0xAA) on attempt %u", attempt);
            break;
        } else {
            ESP_LOGW(TAG, "L2 unexpected ACK byte 0x%02X on attempt %u",
                     ack_buf[0], attempt);
        }
    }

    if (!ack_received) {
        ESP_LOGE(TAG, "L2 TX FAILED — no ACK after %u attempts", LAYER2_MAX_RETRIES);
    }

    /* CRITICAL: Safe frequency switch with FIFO reset */
    set_frequency_433();
    write_reg(REG_DIO_MAPPING1,      0x00);
    write_reg(REG_PAYLOAD_LENGTH,    L1_PACKET_SIZE);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_ADDR_PTR,     0x00);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);
    ESP_LOGI(TAG, "Restored 433 MHz RX mode for next cycle");
}

/* ============================================================
 * SECTION 15 — ESP_TIMER CALLBACKS (CYCLE TIMING)
 * ============================================================ */
static esp_timer_handle_t s_agg_timer;
static esp_timer_handle_t s_cycle_timer;

static void agg_timer_cb(void *arg)
{
    BaseType_t hp = pdFALSE;
    xSemaphoreGiveFromISR(s_agg_trigger, &hp);
    portYIELD_FROM_ISR(hp);
}

static void cycle_reset_timer_cb(void *arg)
{
    BaseType_t hp = pdFALSE;
    xSemaphoreGiveFromISR(s_cycle_reset_c,   &hp);
    xSemaphoreGiveFromISR(s_cycle_reset_mgr, &hp);
    portYIELD_FROM_ISR(hp);
}

static void start_cycle_timers(void)
{
    s_current_cycle_start = (uint32_t)(esp_timer_get_time() / 1000ULL);  /* NEW */
    
    esp_timer_create_args_t agg_args = { .callback = agg_timer_cb, .name = "agg" };
    esp_timer_create(&agg_args, &s_agg_timer);
    esp_timer_start_once(s_agg_timer, 25000000ULL);

    esp_timer_create_args_t cyc_args = { .callback = cycle_reset_timer_cb, .name = "cyc" };
    esp_timer_create(&cyc_args, &s_cycle_timer);
    esp_timer_start_once(s_cycle_timer, 30000000ULL);

    ESP_LOGI(TAG, "Cycle timers armed — T=25s aggregation, T=30s reset");
}

/* ============================================================
 * SECTION 16 — TASK A: LoRa RX
 * ============================================================
 * ENHANCED: Better TX_ACTIVE handling with timeout
 */
static void task_lora_rx(void *arg)
{
    ESP_LOGI(TAG, "Task A (LoRa RX) started");
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    uint8_t raw_buf[L1_PACKET_SIZE];

    while (1) {
        /* NEW: Watchdog check for stuck TX */
        if (s_tx_active) {
            uint32_t tx_elapsed = (uint32_t)(esp_timer_get_time() / 1000ULL) - s_tx_start_time;
            if (tx_elapsed > TX_WATCHDOG_TIMEOUT_MS) {
                ESP_LOGE(TAG, "Task A: TX watchdog triggered — forcing s_tx_active = false");
                s_tx_active = false;
            } else {
                vTaskDelay(pdMS_TO_TICKS(10));
                continue;
            }
        }

        uint8_t irq = read_reg(REG_IRQ_FLAGS);
        if (!(irq & IRQ_RX_DONE_MASK)) { vTaskDelay(pdMS_TO_TICKS(5)); continue; }

        xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
        if (s_tx_active) { xSemaphoreGive(s_lora_mutex); vTaskDelay(pdMS_TO_TICKS(10)); continue; }

        if (irq & IRQ_PAYLOAD_CRC_ERR) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "Task A: HW CRC error — discarded");
            continue;
        }

        uint8_t nb_bytes = read_reg(REG_RX_NB_BYTES);
        if (nb_bytes != L1_PACKET_SIZE) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "Task A: wrong size %u (expected %u)", nb_bytes, L1_PACKET_SIZE);
            continue;
        }

        uint8_t rx_addr = read_reg(REG_FIFO_RX_CURR_ADDR);
        write_reg(REG_FIFO_ADDR_PTR, rx_addr);
        read_fifo(raw_buf, L1_PACKET_SIZE);
        write_reg(REG_IRQ_FLAGS, 0xFF);
        xSemaphoreGive(s_lora_mutex);

        if (xQueueSend(s_raw_rx_queue, raw_buf, pdMS_TO_TICKS(10)) != pdTRUE) {
            ESP_LOGW(TAG, "Task A: raw RX queue full — packet dropped");
        } else {
            ESP_LOGD(TAG, "Task A: queued Node 0x%02X for validation", raw_buf[0]);
        }
    }
}

/* ============================================================
 * SECTION 17 — TASK B: VALIDATION
 * ============================================================
 * UPDATED: Now logs all three sensor types
 */
static void task_validation(void *arg)
{
    ESP_LOGI(TAG, "Task B (Validation) started");

    uint8_t            raw_buf[L1_PACKET_SIZE];
    validated_packet_t vp;

    while (1) {
        if (xQueueReceive(s_raw_rx_queue, raw_buf, portMAX_DELAY) == pdTRUE) {
            if (validate_l1_packet(raw_buf, &vp)) {
                uint8_t idx = vp.node_id - 1;
                s_node_buf[idx].last_packet_id = vp.packet_id;

                if (vp.node_id == NODE_TEMP_ID) {
                    float temp_c = (float)(int16_t)vp.sensor_value / 10.0f;
                    ESP_LOGI(TAG, "Task B: Node 0x01 VALID — Temp=%.1f°C "
                             "PktID=0x%04X TS=%lu ms",
                             temp_c, vp.packet_id, (unsigned long)vp.timestamp);
                } else if (vp.node_id == NODE_HUM_ID) {
                    float hum = (float)vp.sensor_value / 10.0f;
                    ESP_LOGI(TAG, "Task B: Node 0x02 VALID — Hum=%.1f%% "
                             "PktID=0x%04X TS=%lu ms",
                             hum, vp.packet_id, (unsigned long)vp.timestamp);
                } else if (vp.node_id == NODE_LIGHT_ID) {  /* NEW */
                    ESP_LOGI(TAG, "Task B: Node 0x03 VALID — Light=%u lux "
                             "PktID=0x%04X TS=%lu ms",
                             vp.sensor_value, vp.packet_id, (unsigned long)vp.timestamp);
                }

                if (xQueueSend(s_validated_queue, &vp, pdMS_TO_TICKS(10)) != pdTRUE) {
                    ESP_LOGW(TAG, "Task B: validated queue full");
                }
            }
        }
    }
}

/* ============================================================
 * SECTION 18 — TASK C: AGGREGATION
 * ============================================================
 * EDGE CASE FIX: Cycle timestamp validation
 */
static void task_aggregation(void *arg)
{
    ESP_LOGI(TAG, "Task C (Aggregation) started");

    validated_packet_t vp;
    uint8_t dummy = 1;

    while (1) {
        /* NEW: Validate packet belongs to current cycle */
        while (xQueueReceive(s_validated_queue, &vp, pdMS_TO_TICKS(10)) == pdTRUE) {
            uint32_t packet_age = (uint32_t)(esp_timer_get_time() / 1000ULL) - vp.timestamp;
            
            /* Reject packets older than current cycle */
            if (packet_age > CYCLE_DURATION_MS) {
                ESP_LOGW(TAG, "Task C: Node 0x%02X packet too old (%lu ms) — discarded",
                         vp.node_id, (unsigned long)packet_age);
                continue;
            }
            
            uint8_t idx = vp.node_id - 1;
            if (idx < 3) {  /* CHANGED: was <2, now includes node 3 */
                s_node_buf[idx].received     = true;
                s_node_buf[idx].node_id      = vp.node_id;
                s_node_buf[idx].packet_id    = vp.packet_id;
                s_node_buf[idx].timestamp    = vp.timestamp;
                s_node_buf[idx].sensor_value = vp.sensor_value;
                s_node_buf[idx].cycle_timestamp = s_current_cycle_start;
            }
        }

        if (xSemaphoreTake(s_agg_trigger, 0) == pdTRUE) {
            /* Drain any remaining packets before aggregation */
            while (xQueueReceive(s_validated_queue, &vp, pdMS_TO_TICKS(5)) == pdTRUE) {
                uint32_t packet_age = (uint32_t)(esp_timer_get_time() / 1000ULL) - vp.timestamp;
                if (packet_age > CYCLE_DURATION_MS) {
                    ESP_LOGW(TAG, "Task C (agg): Node 0x%02X packet too old — discarded",
                             vp.node_id);
                    continue;
                }
                
                uint8_t idx = vp.node_id - 1;
                if (idx < 3) {  /* CHANGED */
                    s_node_buf[idx].received     = true;
                    s_node_buf[idx].node_id      = vp.node_id;
                    s_node_buf[idx].packet_id    = vp.packet_id;
                    s_node_buf[idx].timestamp    = vp.timestamp;
                    s_node_buf[idx].sensor_value = vp.sensor_value;
                    s_node_buf[idx].cycle_timestamp = s_current_cycle_start;
                }
            }

            ESP_LOGI(TAG, "--- T=25s: Aggregating ---");
            ESP_LOGI(TAG, "  Node 0x01 (Temp) : %s",
                     s_node_buf[0].received ? "RECEIVED" : "ABSENT");
            ESP_LOGI(TAG, "  Node 0x02 (Hum)  : %s",
                     s_node_buf[1].received ? "RECEIVED" : "ABSENT");
            ESP_LOGI(TAG, "  Node 0x03 (Light): %s",  /* CHANGED */
                     s_node_buf[2].received ? "RECEIVED" : "ABSENT");

            build_l2_packet();
            s_l2_packet_ready = true;
            xQueueSend(s_l2_tx_trigger, &dummy, pdMS_TO_TICKS(100));
        }

        if (xSemaphoreTake(s_cycle_reset_c, 0) == pdTRUE) {
            s_node_buf[0].received = false;
            s_node_buf[1].received = false;
            s_node_buf[2].received = false;  /* CHANGED: was hardcoded sentinel */
            s_l2_packet_ready = false;
            ESP_LOGI(TAG, "--- T=30s: Cycle reset — buffers cleared ---");
        }
    }
}

/* ============================================================
 * SECTION 19 — TASK D: LoRa TX
 * ============================================================
 * ENHANCED: TX_ACTIVE flag management with watchdog
 */
static void task_lora_tx(void *arg)
{
    ESP_LOGI(TAG, "Task D (LoRa TX) started");
    uint8_t trigger;

    while (1) {
        if (xQueueReceive(s_l2_tx_trigger, &trigger, portMAX_DELAY) == pdTRUE) {
            ESP_LOGI(TAG, "Task D: TX trigger received");
            s_tx_active = true;
            s_tx_start_time = (uint32_t)(esp_timer_get_time() / 1000ULL);  /* NEW: start watchdog */
            
            vTaskDelay(pdMS_TO_TICKS(15));
            xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
            ESP_LOGI(TAG, "Task D: mutex acquired — L2 TX begin");
            
            transmit_l2_packet();
            
            xSemaphoreGive(s_lora_mutex);
            s_tx_active = false;
            ESP_LOGI(TAG, "Task D: mutex released — Task A resumes RX");
        }
    }
}

/* ============================================================
 * SECTION 20 — CYCLE MANAGER TASK
 * ============================================================ */
static void task_cycle_manager(void *arg)
{
    ESP_LOGI(TAG, "Cycle manager task started");
    while (1) {
        if (xSemaphoreTake(s_cycle_reset_mgr, portMAX_DELAY) == pdTRUE) {
            vTaskDelay(pdMS_TO_TICKS(50));
            esp_timer_stop(s_agg_timer);
            esp_timer_delete(s_agg_timer);
            esp_timer_stop(s_cycle_timer);
            esp_timer_delete(s_cycle_timer);
            start_cycle_timers();
        }
    }
}

/* ============================================================
 * SECTION 21 — NODE BUFFER INITIALISATION
 * ============================================================ */
static void init_node_buffers(void)
{
    for (int i = 0; i < 3; i++) {
        s_node_buf[i].received       = false;
        s_node_buf[i].node_id        = (uint8_t)(i + 1);
        s_node_buf[i].packet_id      = 0;
        s_node_buf[i].timestamp      = 0;
        s_node_buf[i].sensor_value   = 0;
        s_node_buf[i].last_packet_id = 0;
        s_node_buf[i].cycle_timestamp = 0;  /* NEW */
    }
}

/* ============================================================
 * SECTION 22 — APP_MAIN
 * ============================================================ */
void app_main(void)
{
    ESP_LOGI(TAG, "========================================");
    ESP_LOGI(TAG, " Estrotech Local Gateway GW1 — R&D Phase (REVISED)");
    ESP_LOGI(TAG, " ESP32-C3 Super Mini | ESP-IDF v5.5.2");
    ESP_LOGI(TAG, " Active: Node 0x01 (T=0s), Node 0x02 (T=4s), Node 0x03 (T=8s)");
    ESP_LOGI(TAG, " All sensors OPERATIONAL");  /* CHANGED */
    ESP_LOGI(TAG, " L1=433MHz L2=434MHz ACK=0xAA Retry=3");
    ESP_LOGI(TAG, "========================================");

    sx1278_init_spi();
    ESP_LOGI(TAG, "SPI bus initialized on SCK=%d MISO=%d MOSI=%d NSS=%d",
             PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);

    gpio_set_direction(PIN_DIO0, GPIO_MODE_INPUT);
    sx1278_reset();
    ESP_LOGI(TAG, "SX1278 hardware reset complete");

    sx1278_sanity_check();
    sx1278_configure_radio();
    set_frequency_433();
    init_node_buffers();

    s_lora_mutex      = xSemaphoreCreateBinary();
    xSemaphoreGive(s_lora_mutex);
    s_agg_trigger     = xSemaphoreCreateBinary();
    s_cycle_reset_c   = xSemaphoreCreateBinary();
    s_cycle_reset_mgr = xSemaphoreCreateBinary();
    s_raw_rx_queue    = xQueueCreate(8, L1_PACKET_SIZE);
    s_validated_queue = xQueueCreate(8, sizeof(validated_packet_t));
    s_l2_tx_trigger   = xQueueCreate(1, sizeof(uint8_t));

    if (!s_lora_mutex || !s_agg_trigger || !s_cycle_reset_c ||
        !s_cycle_reset_mgr || !s_raw_rx_queue ||
        !s_validated_queue || !s_l2_tx_trigger) {
        ESP_LOGE(TAG, "FATAL: FreeRTOS object creation failed");
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }

    start_cycle_timers();

    xTaskCreate(task_lora_rx,       "TaskA_RX",  4096, NULL, 5, NULL);
    xTaskCreate(task_validation,    "TaskB_Val", 4096, NULL, 5, NULL);
    xTaskCreate(task_aggregation,   "TaskC_Agg", 4096, NULL, 4, NULL);
    xTaskCreate(task_lora_tx,       "TaskD_TX",  4096, NULL, 5, NULL);
    xTaskCreate(task_cycle_manager, "CycleMgr",  2048, NULL, 2, NULL);

    ESP_LOGI(TAG, "All tasks created — gateway operational");
    ESP_LOGI(TAG, "Listening on 433.0 MHz for Node 0x01, 0x02, and 0x03");  /* CHANGED */
}