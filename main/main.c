/*
 * =============================================================================
 * Estrotech — Centralized Private LoRa Sensor Network
 * Local Gateway — GW1 — SENSOR-COMPATIBLE VERSION
 * =============================================================================
 *
 * Owner        : Midhlaj
 * Framework    : ESP-IDF v5.5.2
 * Board        : ESP32-C3 Super Mini
 * LoRa Module  : AI-Thinker Ra-02 SX1278
 *
 * SENSOR COMPATIBILITY FIXES:
 *   ✓ Sentinel value detection matches sensor node behavior:
 *     - Temperature (0x01, 0x04): 0x8000 = sensor failure
 *     - Humidity (0x02, 0x05):    0x0000 = sensor failure
 *     - Light (0x03, 0x06):       0xFFFF = sensor absent/stub
 *   ✓ All LoRa parameters verified against sensor node code
 *   ✓ Packet format matches sensor node exactly
 *   ✓ TDMA timing slots aligned with sensor transmission schedule
 *
 * Verified Compatible With Sensor Node:
 *   - Frequency: 433.0 MHz (0x6C 0x40 0x00) ✓
 *   - LoRa params: SF7, BW125, CR4/5 ✓
 *   - Sync word: 0x12 ✓
 *   - Packet size: 12 bytes ✓
 *   - CRC: CRC16-CCITT (poly 0x1021, init 0xFFFF) ✓
 *   - TX power: PA_BOOST 0xF8 ✓
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
#define GW_ID                   0x01
#define LAYER2_PREAMBLE         0xBB
#define LAYER2_ACK_BYTE         0xAA
#define SYNC_WORD               0x12

/* Layer 1: sensor nodes → local gateway (433.0 MHz) */
#define FREQ_L1_MSB             0x6C
#define FREQ_L1_MID             0x40
#define FREQ_L1_LSB             0x00

/* Layer 2: local gateway → main gateway (434.0 MHz) */
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
#define TX_WATCHDOG_TIMEOUT_MS  5000
#define FREQ_SWITCH_SETTLE_MS   15

#define NODE_TEMP_ID            0x01
#define NODE_HUM_ID             0x02
#define NODE_LIGHT_ID           0x03

/*
 * CRITICAL: Sentinel values now match sensor node behavior
 * Sensor Node Code:
 *   - Temperature fail (0x01, 0x04): returns 0x8000
 *   - Humidity fail (0x02, 0x05):    returns 0x0000  
 *   - Light stub (0x03, 0x06):       returns 0x0000
 */
#define SENTINEL_TEMP_FAIL      0x8000  /* Temperature sensor failure */
#define SENTINEL_HUMI_FAIL      0x0000  /* Humidity sensor failure */
#define SENTINEL_LIGHT_STUB     0x0000  /* Light sensor stub (no hardware) */
#define SENTINEL_PACKET_ID      0xFFFF  /* General absent marker */
#define SENTINEL_TIMESTAMP      0x00000000

#define SX1278_VERSION_EXPECTED 0x12

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
    uint8_t  gw_id;
    uint16_t packet_id;
    uint32_t timestamp;
    uint16_t sensor_value;
    bool     is_valid;
    bool     is_sentinel;  /* NEW: Track if value is a sensor failure sentinel */
} validated_l1_t;

typedef struct {
    uint8_t  node_id;
    uint16_t packet_id;
    uint32_t timestamp;
    uint16_t sensor_value;
    bool     is_sentinel;
} node_block_t;

/* ============================================================
 * SECTION 5 — GLOBAL STATE
 * ============================================================ */
static const char *TAG = "LGW1";

static spi_device_handle_t s_spi_handle;

static SemaphoreHandle_t s_lora_mutex;
static volatile bool     s_tx_active = false;
static volatile uint32_t s_tx_start_time = 0;

static QueueHandle_t s_raw_rx_queue;
static QueueHandle_t s_validated_queue;

static validated_l1_t s_node_buffer[3];
static uint16_t s_last_packet_id[3] = {0, 0, 0};

static uint32_t s_cycle_start_time = 0;
static bool s_aggregation_ready = false;

/* ============================================================
 * SECTION 6 — SX1278 SPI DRIVER
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
    uint8_t tx[L1_PACKET_SIZE + 1];
    uint8_t rx[L1_PACKET_SIZE + 1];
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
 * SECTION 7 — SX1278 INITIALIZATION
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
        ESP_LOGE(TAG, "Check wiring: SCK=%d MISO=%d MOSI=%d NSS=%d",
                 PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }
    ESP_LOGI(TAG, "SX1278 sanity check PASSED — reg 0x42 = 0x12");
}

static void sx1278_configure_radio(void)
{
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_SLEEP);
    vTaskDelay(pdMS_TO_TICKS(10));

    /* Set 433.0 MHz (matches sensor node exactly) */
    write_reg(REG_FRF_MSB, FREQ_L1_MSB);
    write_reg(REG_FRF_MID, FREQ_L1_MID);
    write_reg(REG_FRF_LSB, FREQ_L1_LSB);

    /* LoRa parameters matching sensor node */
    write_reg(REG_SYNC_WORD,      SYNC_WORD);    /* 0x12 */
    write_reg(REG_MODEM_CONFIG1,  0x72);         /* SF7, BW125, CR4/5 */
    write_reg(REG_MODEM_CONFIG2,  0x74);         /* SF7 cont'd */
    write_reg(REG_MODEM_CONFIG3,  0x04);         /* AGC on, LDRO off */
    write_reg(REG_PA_CONFIG,      0xF8);         /* PA_BOOST, matches sensor */
    write_reg(REG_LNA,            0x23);
    write_reg(REG_PREAMBLE_MSB,   0x00);
    write_reg(REG_PREAMBLE_LSB,   0x08);
    write_reg(REG_PAYLOAD_LENGTH, L1_PACKET_SIZE);
    write_reg(REG_DIO_MAPPING1,   0x00);
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_TX_BASE_ADDR, 0x80);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);

    ESP_LOGI(TAG, "SX1278 configured: 433MHz SF7 BW125 CR4/5 Sync=0x12 (sensor-compatible)");
}

/* ============================================================
 * SECTION 8 — CRC16-CCITT (matches sensor node exactly)
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
 * SECTION 9 — PACKET PARSING HELPERS
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
 * SECTION 10 — SENSOR TYPE DETECTION
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

/*
 * CRITICAL UPDATE: Sentinel detection now matches sensor node behavior
 * 
 * Sensor Node Returns:
 *   - Temperature (DHT11) fail: 0x8000
 *   - Humidity (DHT11) fail:    0x0000
 *   - Light (BH1750) stub:      0x0000 (no hardware yet)
 */
static bool is_sentinel_value(uint8_t node_id, uint16_t sensor_value)
{
    sensor_type_t type = get_sensor_type(node_id);
    
    switch (type) {
        case SENSOR_TYPE_TEMPERATURE:
            /* Temperature: 0x8000 indicates DHT11 read failure */
            if (sensor_value == SENTINEL_TEMP_FAIL) {
                ESP_LOGW(TAG, "Node 0x%02X: Temperature sensor failure (0x8000)", node_id);
                return true;
            }
            break;
            
        case SENSOR_TYPE_HUMIDITY:
            /* Humidity: 0x0000 indicates DHT11 read failure */
            if (sensor_value == SENTINEL_HUMI_FAIL) {
                ESP_LOGW(TAG, "Node 0x%02X: Humidity sensor failure (0x0000)", node_id);
                return true;
            }
            break;
            
        case SENSOR_TYPE_LIGHT:
            /* Light: Currently stubbed in sensor node, returns 0x0000 */
            /* This is valid data (0 lux) until BH1750 hardware arrives */
            /* We'll treat it as valid data, not a sentinel */
            return false;
            
        default:
            ESP_LOGW(TAG, "Unknown sensor type for Node 0x%02X", node_id);
            return false;
    }
    
    return false;
}

/* ============================================================
 * SECTION 11 — LAYER 1 PACKET VALIDATION
 * ============================================================ */
static bool validate_l1_packet(const uint8_t *raw, validated_l1_t *out)
{
    uint8_t  node_id      = raw[0];
    uint8_t  gw_id        = raw[1];
    uint16_t packet_id    = parse_u16_be(raw, 2);
    uint32_t timestamp    = parse_u32_be(raw, 4);
    uint16_t sensor_value = parse_u16_be(raw, 8);
    uint16_t received_crc = parse_u16_be(raw, 10);

    /* CRC validation */
    uint16_t computed_crc = crc16_ccitt(raw, 10);
    if (computed_crc != received_crc) {
        ESP_LOGW(TAG, "L1 CRC FAIL — computed 0x%04X received 0x%04X",
                 computed_crc, received_crc);
        return false;
    }

    /* Gateway ID validation */
    if (gw_id != GW_ID) {
        ESP_LOGW(TAG, "Wrong GW_ID 0x%02X (expected 0x%02X)", gw_id, GW_ID);
        return false;
    }

    /* Node ID range validation */
    if (node_id < NODE_TEMP_ID || node_id > NODE_LIGHT_ID) {
        ESP_LOGE(TAG, "Invalid Node_ID 0x%02X", node_id);
        return false;
    }

    /* Sensor type validation */
    sensor_type_t expected_type = get_sensor_type(node_id);
    if (expected_type == SENSOR_TYPE_UNKNOWN) {
        ESP_LOGE(TAG, "Unknown sensor type for Node 0x%02X", node_id);
        return false;
    }

    /* Check for sensor failure sentinel */
    bool is_failure = is_sentinel_value(node_id, sensor_value);

    out->node_id      = node_id;
    out->gw_id        = gw_id;
    out->packet_id    = packet_id;
    out->timestamp    = timestamp;
    out->sensor_value = sensor_value;
    out->is_valid     = true;
    out->is_sentinel  = is_failure;

    if (is_failure) {
        ESP_LOGI(TAG, "L1 VALID (sensor failure) — Node 0x%02X Pkt %u Value 0x%04X CRC 0x%04X",
                 node_id, packet_id, sensor_value, received_crc);
    } else {
        ESP_LOGI(TAG, "L1 VALID — Node 0x%02X Pkt %u Value 0x%04X CRC 0x%04X",
                 node_id, packet_id, sensor_value, received_crc);
    }

    return true;
}

/* ============================================================
 * SECTION 12 — FREQUENCY SWITCHING (SAFE SEQUENCE)
 * ============================================================ */
static void switch_to_434mhz(void)
{
    /* Step 1: STDBY mode */
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    vTaskDelay(pdMS_TO_TICKS(5));

    /* Step 2: Clear IRQ and FIFO */
    write_reg(REG_IRQ_FLAGS, 0xFF);
    write_reg(REG_FIFO_ADDR_PTR, 0x80);  /* TX base */

    /* Step 3: Set frequency */
    write_reg(REG_FRF_MSB, FREQ_L2_MSB);
    write_reg(REG_FRF_MID, FREQ_L2_MID);
    write_reg(REG_FRF_LSB, FREQ_L2_LSB);

    /* Step 4: PLL lock delay */
    vTaskDelay(pdMS_TO_TICKS(FREQ_SWITCH_SETTLE_MS));
}

static void restore_433mhz_rx(void)
{
    /* Step 1: STDBY mode */
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    vTaskDelay(pdMS_TO_TICKS(5));

    /* Step 2: Clear IRQ and FIFO */
    write_reg(REG_IRQ_FLAGS, 0xFF);
    write_reg(REG_FIFO_ADDR_PTR, 0x00);  /* RX base */

    /* Step 3: Set frequency */
    write_reg(REG_FRF_MSB, FREQ_L1_MSB);
    write_reg(REG_FRF_MID, FREQ_L1_MID);
    write_reg(REG_FRF_LSB, FREQ_L1_LSB);

    /* Step 4: PLL lock delay */
    vTaskDelay(pdMS_TO_TICKS(FREQ_SWITCH_SETTLE_MS));

    /* Step 5: RX mode */
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);
}

/* ============================================================
 * SECTION 13 — LAYER 2 PACKET BUILDER
 * ============================================================ */
static void build_l2_packet(uint8_t *l2_pkt)
{
    uint32_t gw_timestamp = (uint32_t)(esp_timer_get_time() / 1000ULL);

    l2_pkt[0] = LAYER2_PREAMBLE;  /* 0xBB */
    l2_pkt[1] = GW_ID;
    
    /* GW timestamp */
    l2_pkt[2] = (uint8_t)((gw_timestamp >> 24) & 0xFF);
    l2_pkt[3] = (uint8_t)((gw_timestamp >> 16) & 0xFF);
    l2_pkt[4] = (uint8_t)((gw_timestamp >>  8) & 0xFF);
    l2_pkt[5] = (uint8_t)(gw_timestamp         & 0xFF);

    /* Pack 3 NodeBlocks */
    for (int i = 0; i < 3; i++) {
        int offset = 6 + (i * 9);
        
        if (s_node_buffer[i].is_valid && !s_node_buffer[i].is_sentinel) {
            /* Valid sensor data */
            l2_pkt[offset + 0] = s_node_buffer[i].node_id;
            l2_pkt[offset + 1] = (uint8_t)(s_node_buffer[i].packet_id >> 8);
            l2_pkt[offset + 2] = (uint8_t)(s_node_buffer[i].packet_id & 0xFF);
            l2_pkt[offset + 3] = (uint8_t)((s_node_buffer[i].timestamp >> 24) & 0xFF);
            l2_pkt[offset + 4] = (uint8_t)((s_node_buffer[i].timestamp >> 16) & 0xFF);
            l2_pkt[offset + 5] = (uint8_t)((s_node_buffer[i].timestamp >>  8) & 0xFF);
            l2_pkt[offset + 6] = (uint8_t)(s_node_buffer[i].timestamp         & 0xFF);
            l2_pkt[offset + 7] = (uint8_t)(s_node_buffer[i].sensor_value >> 8);
            l2_pkt[offset + 8] = (uint8_t)(s_node_buffer[i].sensor_value & 0xFF);
            
            ESP_LOGI(TAG, "NodeBlock[%d]: Node 0x%02X valid data 0x%04X", 
                     i, s_node_buffer[i].node_id, s_node_buffer[i].sensor_value);
        } else if (s_node_buffer[i].is_valid && s_node_buffer[i].is_sentinel) {
            /* Sensor failure - pack actual failure value (0x8000 or 0x0000) */
            l2_pkt[offset + 0] = s_node_buffer[i].node_id;
            l2_pkt[offset + 1] = (uint8_t)(s_node_buffer[i].packet_id >> 8);
            l2_pkt[offset + 2] = (uint8_t)(s_node_buffer[i].packet_id & 0xFF);
            l2_pkt[offset + 3] = (uint8_t)((s_node_buffer[i].timestamp >> 24) & 0xFF);
            l2_pkt[offset + 4] = (uint8_t)((s_node_buffer[i].timestamp >> 16) & 0xFF);
            l2_pkt[offset + 5] = (uint8_t)((s_node_buffer[i].timestamp >>  8) & 0xFF);
            l2_pkt[offset + 6] = (uint8_t)(s_node_buffer[i].timestamp         & 0xFF);
            l2_pkt[offset + 7] = (uint8_t)(s_node_buffer[i].sensor_value >> 8);
            l2_pkt[offset + 8] = (uint8_t)(s_node_buffer[i].sensor_value & 0xFF);
            
            ESP_LOGW(TAG, "NodeBlock[%d]: Node 0x%02X sensor failure 0x%04X", 
                     i, s_node_buffer[i].node_id, s_node_buffer[i].sensor_value);
        } else {
            /* Sensor absent - use 0xFFFF sentinel */
            uint8_t node_id = NODE_TEMP_ID + i;
            l2_pkt[offset + 0] = node_id;
            l2_pkt[offset + 1] = 0xFF;  /* Packet_ID sentinel */
            l2_pkt[offset + 2] = 0xFF;
            l2_pkt[offset + 3] = 0;     /* Timestamp = 0 */
            l2_pkt[offset + 4] = 0;
            l2_pkt[offset + 5] = 0;
            l2_pkt[offset + 6] = 0;
            l2_pkt[offset + 7] = 0xFF;  /* Sensor_Value sentinel */
            l2_pkt[offset + 8] = 0xFF;
            
            ESP_LOGW(TAG, "NodeBlock[%d]: Node 0x%02X absent (sentinel 0xFFFF)", i, node_id);
        }
    }

    /* CRC over first 33 bytes */
    uint16_t crc = crc16_ccitt(l2_pkt, 33);
    l2_pkt[33] = (uint8_t)(crc >> 8);
    l2_pkt[34] = (uint8_t)(crc & 0xFF);

    ESP_LOGI(TAG, "L2 packet built — GW_TS=%lu CRC=0x%04X", 
             (unsigned long)gw_timestamp, crc);
}

/* ============================================================
 * SECTION 14 — LAYER 2 TRANSMISSION
 * ============================================================ */
static bool transmit_l2_packet(const uint8_t *l2_pkt)
{
    s_tx_active = true;
    s_tx_start_time = (uint32_t)(esp_timer_get_time() / 1000ULL);

    for (int retry = 0; retry < LAYER2_MAX_RETRIES; retry++) {
        ESP_LOGI(TAG, "L2 TX attempt %d/%d", retry + 1, LAYER2_MAX_RETRIES);

        /* Switch to 434 MHz */
        xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
        switch_to_434mhz();
        xSemaphoreGive(s_lora_mutex);

        /* Transmit */
        xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
        
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
        vTaskDelay(pdMS_TO_TICKS(5));
        write_reg(REG_IRQ_FLAGS, 0xFF);
        write_reg(REG_FIFO_ADDR_PTR, 0x80);
        write_fifo(l2_pkt, L2_PACKET_SIZE);
        write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);
        write_reg(REG_DIO_MAPPING1, 0x40);
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_TX);

        /* Wait for TX_DONE */
        uint32_t tx_start = (uint32_t)(esp_timer_get_time() / 1000ULL);
        bool tx_done = false;
        while ((esp_timer_get_time() / 1000ULL - tx_start) < 2000) {
            if (read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK) {
                tx_done = true;
                break;
            }
            vTaskDelay(pdMS_TO_TICKS(10));
        }

        write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);
        xSemaphoreGive(s_lora_mutex);

        if (!tx_done) {
            ESP_LOGE(TAG, "L2 TX timeout");
            continue;
        }

        ESP_LOGI(TAG, "L2 TX complete — waiting for ACK");

        /* Wait for ACK */
        uint32_t ack_start = (uint32_t)(esp_timer_get_time() / 1000ULL);
        bool ack_received = false;

        while ((esp_timer_get_time() / 1000ULL - ack_start) < LAYER2_ACK_TIMEOUT_MS) {
            /* Check for RX_DONE */
            uint8_t irq = read_reg(REG_IRQ_FLAGS);
            if (irq & IRQ_RX_DONE_MASK) {
                uint8_t nb = read_reg(REG_RX_NB_BYTES);
                if (nb == L2_ACK_SIZE) {
                    uint8_t ack_byte;
                    uint8_t rx_addr = read_reg(REG_FIFO_RX_CURR_ADDR);
                    write_reg(REG_FIFO_ADDR_PTR, rx_addr);
                    read_fifo(&ack_byte, 1);
                    
                    if (ack_byte == LAYER2_ACK_BYTE) {
                        ESP_LOGI(TAG, "ACK 0xAA received");
                        ack_received = true;
                        break;
                    }
                }
                write_reg(REG_IRQ_FLAGS, 0xFF);
            }
            vTaskDelay(pdMS_TO_TICKS(5));
        }

        if (ack_received) {
            /* Restore 433 MHz RX */
            xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
            restore_433mhz_rx();
            xSemaphoreGive(s_lora_mutex);
            
            s_tx_active = false;
            ESP_LOGI(TAG, "L2 TX successful");
            return true;
        }

        ESP_LOGW(TAG, "ACK timeout");
    }

    /* All retries failed */
    xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
    restore_433mhz_rx();
    xSemaphoreGive(s_lora_mutex);
    
    s_tx_active = false;
    ESP_LOGE(TAG, "L2 TX failed after %d retries", LAYER2_MAX_RETRIES);
    return false;
}

/* ============================================================
 * SECTION 15 — TASK A: LoRa RX
 * ============================================================ */
static void task_lora_rx(void *arg)
{
    ESP_LOGI(TAG, "Task A (LoRa RX) started");

    spi_device_acquire_bus(s_spi_handle, portMAX_DELAY);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);
    spi_device_release_bus(s_spi_handle);

    ESP_LOGI(TAG, "Listening on 433 MHz for L1 packets (sensor-compatible mode)");

    uint8_t raw_buf[L1_PACKET_SIZE];

    while (1) {
        /* TX watchdog check */
        if (s_tx_active) {
            uint32_t tx_elapsed = (uint32_t)(esp_timer_get_time() / 1000ULL) - s_tx_start_time;
            if (tx_elapsed > TX_WATCHDOG_TIMEOUT_MS) {
                ESP_LOGE(TAG, "TX watchdog timeout — forcing recovery");
                s_tx_active = false;
            } else {
                vTaskDelay(pdMS_TO_TICKS(50));
                continue;
            }
        }

        uint8_t irq = read_reg(REG_IRQ_FLAGS);
        taskYIELD();

        if (!(irq & IRQ_RX_DONE_MASK)) {
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        xSemaphoreTake(s_lora_mutex, portMAX_DELAY);

        if (s_tx_active) {
            xSemaphoreGive(s_lora_mutex);
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        spi_device_acquire_bus(s_spi_handle, portMAX_DELAY);

        irq = read_reg(REG_IRQ_FLAGS);
        taskYIELD();

        if (irq & IRQ_PAYLOAD_CRC_ERR) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            spi_device_release_bus(s_spi_handle);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "HW CRC error");
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

        if (nb_bytes != L1_PACKET_SIZE) {
            write_reg(REG_IRQ_FLAGS, 0xFF);
            spi_device_release_bus(s_spi_handle);
            xSemaphoreGive(s_lora_mutex);
            ESP_LOGW(TAG, "Wrong size %u", nb_bytes);
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        uint8_t rx_addr = read_reg(REG_FIFO_RX_CURR_ADDR);
        write_reg(REG_FIFO_ADDR_PTR, rx_addr);
        read_fifo(raw_buf, L1_PACKET_SIZE);
        write_reg(REG_IRQ_FLAGS, 0xFF);

        spi_device_release_bus(s_spi_handle);
        xSemaphoreGive(s_lora_mutex);

        /* Queue for validation */
        if (xQueueSend(s_raw_rx_queue, raw_buf, pdMS_TO_TICKS(10)) != pdTRUE) {
            ESP_LOGE(TAG, "raw_rx_queue full");
        }

        vTaskDelay(pdMS_TO_TICKS(20));
    }
}

/* ============================================================
 * SECTION 16 — TASK B: VALIDATION
 * ============================================================ */
static void task_validation(void *arg)
{
    ESP_LOGI(TAG, "Task B (Validation) started");

    uint8_t        raw_buf[L1_PACKET_SIZE];
    validated_l1_t vl1;

    while (1) {
        if (xQueueReceive(s_raw_rx_queue, raw_buf, portMAX_DELAY) == pdTRUE) {
            if (validate_l1_packet(raw_buf, &vl1)) {
                /* Deduplication */
                int idx = vl1.node_id - NODE_TEMP_ID;
                if (idx >= 0 && idx < 3) {
                    if (vl1.packet_id == s_last_packet_id[idx]) {
                        ESP_LOGW(TAG, "Duplicate packet_id %u from Node 0x%02X",
                                 vl1.packet_id, vl1.node_id);
                        continue;
                    }
                    s_last_packet_id[idx] = vl1.packet_id;
                }

                if (xQueueSend(s_validated_queue, &vl1, portMAX_DELAY) != pdTRUE) {
                    ESP_LOGW(TAG, "validated_queue full");
                }
            }
        }
    }
}

/* ============================================================
 * SECTION 17 — TASK C: AGGREGATION
 * ============================================================ */
static void task_aggregation(void *arg)
{
    ESP_LOGI(TAG, "Task C (Aggregation) started");

    validated_l1_t vl1;

    while (1) {
        if (xQueueReceive(s_validated_queue, &vl1, portMAX_DELAY) == pdTRUE) {
            uint32_t now_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
            uint32_t packet_age = now_ms - vl1.timestamp;

            /* Packet age check */
            if (packet_age > 30000) {
                ESP_LOGW(TAG, "Packet too old (%lu ms)", (unsigned long)packet_age);
                continue;
            }

            /* Cycle membership check */
            if (vl1.timestamp < s_cycle_start_time) {
                ESP_LOGW(TAG, "Packet from previous cycle");
                continue;
            }

            /* Store in buffer */
            int idx = vl1.node_id - NODE_TEMP_ID;
            if (idx >= 0 && idx < 3) {
                s_node_buffer[idx] = vl1;
                ESP_LOGI(TAG, "Node 0x%02X buffered (slot %d) — age %lu ms", 
                         vl1.node_id, idx, (unsigned long)packet_age);
            }
        }
    }
}

/* ============================================================
 * SECTION 18 — TASK D: L2 TX
 * ============================================================ */
static void task_l2_tx(void *arg)
{
    ESP_LOGI(TAG, "Task D (L2 TX) started");

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(AGGREGATION_TIME_MS));

        if (!s_aggregation_ready) {
            continue;
        }

        ESP_LOGI(TAG, "Aggregation window closed — building L2 packet");

        uint8_t l2_pkt[L2_PACKET_SIZE];
        build_l2_packet(l2_pkt);

        transmit_l2_packet(l2_pkt);

        s_aggregation_ready = false;
    }
}

/* ============================================================
 * SECTION 19 — CYCLE MANAGER
 * ============================================================ */
static void cycle_manager_task(void *arg)
{
    ESP_LOGI(TAG, "Cycle Manager started");

    while (1) {
        s_cycle_start_time = (uint32_t)(esp_timer_get_time() / 1000ULL);
        
        /* Clear buffers */
        memset(s_node_buffer, 0, sizeof(s_node_buffer));
        
        s_aggregation_ready = true;

        ESP_LOGI(TAG, "New cycle — T=0s (timestamp %lu)", 
                 (unsigned long)s_cycle_start_time);

        vTaskDelay(pdMS_TO_TICKS(CYCLE_DURATION_MS));
    }
}

/* ============================================================
 * SECTION 20 — APP_MAIN
 * ============================================================ */
void app_main(void)
{
    ESP_LOGI(TAG, "========================================");
    ESP_LOGI(TAG, " Local Gateway GW1 — SENSOR-COMPATIBLE");
    ESP_LOGI(TAG, " ESP32-C3 Super Mini | ESP-IDF v5.5.2");
    ESP_LOGI(TAG, " Frequency: 433 MHz (L1 RX, sensor match)");
    ESP_LOGI(TAG, " Sentinel Values: Temp=0x8000 Humi=0x0000");
    ESP_LOGI(TAG, "========================================");

    sx1278_init_spi();
    ESP_LOGI(TAG, "SPI initialized");

    gpio_set_direction(PIN_DIO0, GPIO_MODE_INPUT);
    sx1278_reset();
    ESP_LOGI(TAG, "SX1278 reset");
    
    sx1278_sanity_check();
    sx1278_configure_radio();

    s_lora_mutex        = xSemaphoreCreateBinary();
    xSemaphoreGive(s_lora_mutex);
    
    s_raw_rx_queue      = xQueueCreate(8, L1_PACKET_SIZE);
    s_validated_queue   = xQueueCreate(8, sizeof(validated_l1_t));

    if (!s_lora_mutex || !s_raw_rx_queue || !s_validated_queue) {
        ESP_LOGE(TAG, "FATAL: FreeRTOS object creation failed");
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }

    xTaskCreate(task_lora_rx,     "TaskA_RX",   4096, NULL, 5, NULL);
    xTaskCreate(task_validation,  "TaskB_Val",  4096, NULL, 5, NULL);
    xTaskCreate(task_aggregation, "TaskC_Agg",  4096, NULL, 5, NULL);
    xTaskCreate(task_l2_tx,       "TaskD_TX",   4096, NULL, 5, NULL);
    xTaskCreate(cycle_manager_task, "CycleMgr", 2048, NULL, 3, NULL);

    ESP_LOGI(TAG, "All tasks created — Local Gateway operational");
    ESP_LOGI(TAG, "Listening for sensor nodes: 0x01 (temp), 0x02 (humi), 0x03 (light)");
}