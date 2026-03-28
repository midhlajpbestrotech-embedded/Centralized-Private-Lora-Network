/*
 * =============================================================================
 * Estrotech — Centralized Private LoRa Sensor Network
 * Local Gateway — GW1 — R&D Phase Firmware
 * =============================================================================
 *
 * Owner        : Midhlaj
 * Framework    : ESP-IDF v5.5.2
 * Board        : ESP32-C3 Super Mini
 * LoRa Module  : AI-Thinker Ra-02 SX1278
 *
 * R&D Phase Scope:
 *   - Node 0x01 (DS18B20 Temp)    — TX at T=0s
 *   - Node 0x02 (Humidity)        — TX at T=4s  [R&D compressed timing]
 *   - Node 0x03 (Light)           — OFFLINE — always sentinel
 *   - GW_ID = 0x01
 *   - Layer 2 TX to Main Gateway on 434.0 MHz with ACK (0xAA) + retry x3
 *   - Aggregation at T=25s
 *   - 30-second cycle using esp_timer
 *
 * Pin Assignment (ESP32-C3 Super Mini):
 *   SCK=GPIO4  MISO=GPIO5  MOSI=GPIO6  NSS=GPIO7  RST=GPIO10  DIO0=GPIO3
 *
 * =============================================================================
 * FIXES APPLIED vs PREVIOUS REVISION:
 *
 *   FIX A — ESP32-C3 is SINGLE CORE (Core 0 only).
 *            xTaskCreatePinnedToCore(..., core=1) crashes with assert.
 *            All tasks now use xTaskCreate() — no core pinning.
 *            This was the direct cause of the firmware crash shown in the log.
 *
 *   FIX B — L1_PACKET_SIZE corrected from 11 to 12.
 *            Spec offset table: Node_ID(1)+GW_ID(1)+Pkt_ID(2)+TS(4)+
 *            Sensor_Value(2)+CRC(2) = 12 bytes total.
 *            CRC sits at offset 10–11, not 9–10.
 *
 *   FIX C — CRC offset in validate_l1_packet corrected.
 *            Was: parse_u16_be(raw, 9)  — reads Sensor_Value[1] + garbage.
 *            Now: parse_u16_be(raw, 10) — reads actual CRC bytes.
 *            Previous code caused CRC mismatch on every valid packet.
 *
 *   FIX D — Mutex usage in Task A corrected.
 *            Previous: mutex acquired/released every 5ms poll tick, allowing
 *            Task D to grab the mutex mid-reception and corrupt FIFO reads.
 *            Now: volatile flag s_tx_active used for soft signalling.
 *            Task A skips all radio access when flag is set.
 *            Task D sets flag before acquiring mutex, clears after release.
 *            Mutex now guards full radio sessions, not individual register reads.
 *
 *   FIX E — Dual-consumer of s_cycle_reset semaphore removed.
 *            Previous: task_cycle_manager gave the semaphore back so Task C
 *            could also see it — fragile double-give on a binary semaphore.
 *            Now: two separate semaphores:
 *              s_cycle_reset_c   — taken by Task C to clear buffers
 *              s_cycle_reset_mgr — taken by CycleMgr to re-arm timers
 *            Both are given independently from the timer ISR callback.
 * =============================================================================
 *
 * Layer 1 packet (12 bytes, big-endian):
 *   [ Node_ID | GW_ID | Packet_ID | Timestamp | Sensor_Value | CRC ]
 *     1B        1B      2B          4B           2B             2B
 *   CRC = CRC16-CCITT over bytes 0..8 (first 9 bytes)
 *   CRC stored at offset 10–11
 *
 * Layer 2 packet (35 bytes, big-endian):
 *   [ Preamble | GW_ID | GW_Timestamp | NodeBlock_1 | NodeBlock_2 | NodeBlock_3 | CRC ]
 *     1B         1B      4B             9B            9B            9B             2B
 *   CRC = CRC16-CCITT over bytes 0..32 (first 33 bytes)
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
 * ============================================================
 * Original spec pins (ESP32-30pin) are NOT usable on C3 Super Mini.
 * Remapped as follows:
 *   Original: SCK=18, MISO=19, MOSI=23, NSS=5,  RST=14, DIO0=26
 *   C3 Mini : SCK=4,  MISO=5,  MOSI=6,  NSS=7,  RST=10, DIO0=3
 *
 * GPIO 20/21 reserved for USB-Serial (idf.py monitor).
 * GPIO 9     reserved for BOOT button.
 */
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

/*
 * FIX B: L1_PACKET_SIZE corrected from 11 to 12.
 * Spec offset table: Node_ID(1) + GW_ID(1) + Packet_ID(2) + Timestamp(4)
 *                  + Sensor_Value(2) + CRC(2) = 12 bytes.
 * CRC at offset 10–11. Previous value of 11 caused CRC to be read from
 * offset 9–10 (Sensor_Value[1] + CRC[0]) — wrong bytes, always failed.
 */
#define L1_PACKET_SIZE          12      /* FIX B: was 11 */

#define L2_PACKET_SIZE          35      /* 1+1+4 + 3×9 + 2 — unchanged */
#define L2_ACK_SIZE             1

#define CYCLE_DURATION_MS       30000
#define AGGREGATION_TIME_MS     25000
#define LAYER2_ACK_TIMEOUT_MS   500
#define LAYER2_MAX_RETRIES      3

/* Node IDs managed by GW1 */
#define NODE_TEMP_ID            0x01    /* DS18B20 Temperature — TX T=0s  */
#define NODE_HUM_ID             0x02    /* Humidity             — TX T=4s  */
#define NODE_LIGHT_ID           0x03    /* Light                — OFFLINE  */

/* Sentinel values for absent nodes */
#define SENTINEL_PACKET_ID      0xFFFF
#define SENTINEL_TIMESTAMP      0x00000000
#define SENTINEL_SENSOR_VALUE   0xFFFF

/* SX1278 version register expected value */
#define SX1278_VERSION_EXPECTED 0x12

/* ============================================================
 * SECTION 4 — DATA STRUCTURES
 * ============================================================ */

/*
 * Per-node receive buffer — holds one cycle's worth of data.
 * Populated by Task C (Aggregation) from validated_packet_t items.
 * Consumed by build_l2_packet() at T=25s.
 */
typedef struct {
    bool     received;
    uint8_t  node_id;
    uint16_t packet_id;
    uint32_t timestamp;
    uint16_t sensor_value;
    uint16_t last_packet_id;    /* for duplicate detection across cycles */
} node_buffer_t;

/*
 * validated_packet_t — passed from Task B to Task C via queue.
 */
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

/*
 * s_lora_mutex — guards full radio sessions (not individual register reads).
 * Task A holds it during a full FIFO-read session.
 * Task D holds it for the entire TX+ACK sequence.
 *
 * FIX D: s_tx_active flag added.
 * Task A checks this flag before every radio access.
 * Task D sets s_tx_active=true BEFORE taking the mutex — this gives Task A
 * time to finish any in-progress IRQ read and exit cleanly before Task D
 * acquires the mutex. Task D clears s_tx_active after releasing the mutex.
 */
static SemaphoreHandle_t     s_lora_mutex;
static volatile bool         s_tx_active = false;  /* FIX D */

/*
 * Aggregation trigger — given by timer ISR at T=25s, taken by Task C.
 */
static SemaphoreHandle_t s_agg_trigger;

/*
 * FIX E: Two separate cycle-reset semaphores replace the single s_cycle_reset.
 *   s_cycle_reset_c   — given by ISR at T=30s, taken by Task C to clear buffers.
 *   s_cycle_reset_mgr — given by ISR at T=30s, taken by CycleMgr to re-arm timers.
 * Previously both tasks consumed the same binary semaphore, requiring a fragile
 * re-give from CycleMgr. Now the ISR gives both independently.
 */
static SemaphoreHandle_t s_cycle_reset_c;    /* FIX E */
static SemaphoreHandle_t s_cycle_reset_mgr;  /* FIX E */

/* Queue: Task A → Task B (raw 12-byte packets) */
static QueueHandle_t s_raw_rx_queue;

/* Queue: Task B → Task C (validated packets) */
static QueueHandle_t s_validated_queue;

/* Queue: Task C → Task D (TX trigger) */
static QueueHandle_t s_l2_tx_trigger;

/* Node receive buffers — index 0=Node0x01, 1=Node0x02, 2=Node0x03 */
static node_buffer_t s_node_buf[3];

/* Assembled Layer 2 packet — written by Task C, read by Task D */
static uint8_t s_l2_packet[L2_PACKET_SIZE];
static bool    s_l2_packet_ready = false;

/* ============================================================
 * SECTION 6 — SX1278 SPI DRIVER (Custom — No External Libraries)
 * ============================================================ */

/*
 * write_reg — write one byte to a SX1278 register.
 * SX1278 SPI: address byte MSB=1 for write, MSB=0 for read.
 * NSS is managed automatically by the SPI driver (spics_io_num=PIN_NSS).
 */
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

/*
 * read_reg — read one byte from a SX1278 register.
 */
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

/*
 * write_fifo — burst-write a byte array into the SX1278 FIFO.
 */
static void write_fifo(const uint8_t *data, uint8_t len)
{
    /* Max buffer: 1 addr byte + L2_PACKET_SIZE data bytes */
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

/*
 * read_fifo — burst-read len bytes from the SX1278 FIFO.
 * Buffer sized to L1_PACKET_SIZE (12 bytes) + 1 addr byte = 13 bytes max.
 */
static void read_fifo(uint8_t *data, uint8_t len)
{
    uint8_t tx[L2_PACKET_SIZE + 1];
    uint8_t rx[L2_PACKET_SIZE + 1];
    memset(tx, 0, sizeof(tx));
    tx[0] = REG_FIFO & 0x7F;    /* read address = 0x00 */
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
        .clock_speed_hz = 1000000,   /* 1 MHz — conservative for R&D */
        .mode           = 0,
        .spics_io_num   = PIN_NSS,   /* driver controls NSS automatically */
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

/*
 * sx1278_sanity_check — reads version register 0x42, expects 0x12.
 * Halts firmware on failure — SPI fault is unrecoverable.
 */
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
    /* Sleep mode required to set LoRa mode bit */
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_SLEEP);
    vTaskDelay(pdMS_TO_TICKS(10));

    /* Sync word 0x12 — private network */
    write_reg(REG_SYNC_WORD, SYNC_WORD);

    /* BW=125kHz, CR=4/5, implicit header off */
    write_reg(REG_MODEM_CONFIG1, 0x72);

    /* SF=7, RxPayloadCrcOn=1 */
    write_reg(REG_MODEM_CONFIG2, 0x74);

    /* LNA gain auto */
    write_reg(REG_MODEM_CONFIG3, 0x04);

    /*
     * PA_CONFIG = 0xF8:
     *   PA_BOOST (bit7)   = 1   — mandatory on Ra-02 (RFO not connected)
     *   MaxPower (bits6:4)= 111 = 7
     *   OutputPower(bits3:0)=1000 = 8
     *   Pout = 17 - (15 - 8) = 10 dBm — matches spec
     */
    write_reg(REG_PA_CONFIG, 0xF8);

    /* LNA max gain, boost on */
    write_reg(REG_LNA, 0x23);

    /* Preamble = 8 symbols */
    write_reg(REG_PREAMBLE_MSB, 0x00);
    write_reg(REG_PREAMBLE_LSB, 0x08);

    /* Fixed payload length for Layer 1 RX — FIX B: 12 bytes */
    write_reg(REG_PAYLOAD_LENGTH, L1_PACKET_SIZE);

    /* DIO0 = RxDone by default */
    write_reg(REG_DIO_MAPPING1, 0x00);

    /* FIFO split: RX at 0x00, TX at 0x80 */
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_TX_BASE_ADDR, 0x80);

    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);

    ESP_LOGI(TAG, "SX1278 radio parameters configured");
}

/* ============================================================
 * SECTION 8 — FREQUENCY SWITCHING
 * ============================================================ */

static void set_frequency_433(void)
{
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    write_reg(REG_FRF_MSB, FREQ_L1_MSB);   /* 0x6C */
    write_reg(REG_FRF_MID, FREQ_L1_MID);   /* 0x40 */
    write_reg(REG_FRF_LSB, FREQ_L1_LSB);   /* 0x00 */
    ESP_LOGD(TAG, "Frequency set to 433.0 MHz (Layer 1 RX)");
}

static void set_frequency_434(void)
{
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
    write_reg(REG_FRF_MSB, FREQ_L2_MSB);   /* 0x6C */
    write_reg(REG_FRF_MID, FREQ_L2_MID);   /* 0x80 */
    write_reg(REG_FRF_LSB, FREQ_L2_LSB);   /* 0x00 */
    ESP_LOGD(TAG, "Frequency set to 434.0 MHz (Layer 2 TX)");
}

/* ============================================================
 * SECTION 9 — CRC16-CCITT
 * ============================================================
 * Polynomial 0x1021, initial value 0xFFFF.
 * Layer 1: computed over bytes 0..8 (9 bytes), stored at offset 10–11.
 * Layer 2: computed over bytes 0..32 (33 bytes), stored at offset 33–34.
 */
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
 * SECTION 11 — LAYER 1 PACKET VALIDATION
 * ============================================================
 *
 * Validates a 12-byte Layer 1 packet. Returns true only if all checks pass.
 *
 * Packet layout (12 bytes, big-endian):
 *   [0]     Node_ID       uint8
 *   [1]     Gateway_ID    uint8
 *   [2-3]   Packet_ID     uint16 BE
 *   [4-7]   Timestamp     uint32 BE
 *   [8-9]   Sensor_Value  uint16 BE
 *   [10-11] CRC           uint16 BE  — CRC16-CCITT over bytes [0..8]
 *
 * Validation checks (in order):
 *   1. CRC16-CCITT over bytes [0..8] must match bytes [10..11]
 *   2. Node_ID must be 0x01 or 0x02 (active nodes in R&D)
 *   3. Gateway_ID must be 0x01 (this gateway)
 *   4. Packet_ID != 0x0000 (reserved)
 *   5. Packet_ID != last seen for that node (deduplication)
 */
static bool validate_l1_packet(const uint8_t *raw, validated_packet_t *out)
{
    /* Check 1: CRC — over first 9 bytes */
    uint16_t computed_crc = crc16_ccitt(raw, 9);
    /*
     * FIX C: CRC read from offset 10, not 9.
     * Offset 9 = Sensor_Value[1] (second byte of sensor value).
     * Offset 10–11 = CRC field per spec offset table.
     */
    uint16_t received_crc = parse_u16_be(raw, 10);  /* FIX C: was offset 9 */

    if (computed_crc != received_crc) {
        ESP_LOGW(TAG, "L1 CRC FAIL — computed 0x%04X received 0x%04X",
                 computed_crc, received_crc);
        return false;
    }

    /* Extract fields */
    uint8_t  node_id      = raw[0];
    uint8_t  gw_id        = raw[1];
    uint16_t packet_id    = parse_u16_be(raw, 2);
    uint32_t timestamp    = parse_u32_be(raw, 4);
    uint16_t sensor_value = parse_u16_be(raw, 8);

    /* Check 2: Node_ID — only active R&D nodes accepted */
    if (node_id != NODE_TEMP_ID && node_id != NODE_HUM_ID) {
        ESP_LOGW(TAG, "L1 Node_ID 0x%02X rejected — not in GW1 R&D active set",
                 node_id);
        return false;
    }

    /* Check 3: Gateway_ID */
    if (gw_id != GW_ID) {
        ESP_LOGW(TAG, "L1 GW_ID 0x%02X rejected — expected 0x01", gw_id);
        return false;
    }

    /* Check 4: Reserved Packet_ID */
    if (packet_id == 0x0000) {
        ESP_LOGW(TAG, "L1 Node 0x%02X: Packet_ID 0x0000 reserved — discarded",
                 node_id);
        return false;
    }

    /* Check 5: Duplicate detection */
    uint8_t idx = node_id - 1;   /* 0x01 → 0, 0x02 → 1 */
    if (s_node_buf[idx].last_packet_id == packet_id) {
        ESP_LOGW(TAG, "L1 Node 0x%02X: Packet_ID 0x%04X duplicate — discarded",
                 node_id, packet_id);
        return false;
    }

    /* All checks passed */
    out->node_id      = node_id;
    out->packet_id    = packet_id;
    out->timestamp    = timestamp;
    out->sensor_value = sensor_value;
    return true;
}

/* ============================================================
 * SECTION 12 — LAYER 2 PACKET BUILDER
 * ============================================================ */

/*
 * build_nodeblock — write one 9-byte NodeBlock into buf at offset.
 * Uses sentinel values if node did not respond this cycle.
 *
 * NodeBlock layout (9 bytes):
 *   [0]   Node_ID       uint8
 *   [1-2] Packet_ID     uint16 BE
 *   [3-6] Timestamp     uint32 BE
 *   [7-8] Sensor_Value  uint16 BE
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

/*
 * build_l2_packet — assemble the 35-byte Layer 2 packet into s_l2_packet.
 *
 * Layout:
 *   [0]      Preamble     0xBB
 *   [1]      GW_ID        0x01
 *   [2-5]    GW_Timestamp uint32 BE  ms since boot
 *   [6-14]   NodeBlock_1  Node 0x01 (Temp)
 *   [15-23]  NodeBlock_2  Node 0x02 (Humidity)
 *   [24-32]  NodeBlock_3  Node 0x03 (Light — always sentinel in R&D)
 *   [33-34]  CRC          CRC16-CCITT over bytes [0..32]
 */
static void build_l2_packet(void)
{
    uint32_t gw_timestamp = (uint32_t)(esp_timer_get_time() / 1000ULL);

    s_l2_packet[0] = LAYER2_PREAMBLE;
    s_l2_packet[1] = GW_ID;
    put_u32_be(s_l2_packet, 2, gw_timestamp);

    build_nodeblock(s_l2_packet,  6, NODE_TEMP_ID,  &s_node_buf[0]);
    build_nodeblock(s_l2_packet, 15, NODE_HUM_ID,   &s_node_buf[1]);
    build_nodeblock(s_l2_packet, 24, NODE_LIGHT_ID, &s_node_buf[2]);

    uint16_t crc = crc16_ccitt(s_l2_packet, 33);
    put_u16_be(s_l2_packet, 33, crc);

    ESP_LOGI(TAG, "L2 packet built — GW_TS=%lu ms", (unsigned long)gw_timestamp);
    ESP_LOGI(TAG, "  NodeBlock 1 (0x01 Temp)  : %s  raw=0x%04X",
             s_node_buf[0].received ? "DATA    " : "SENTINEL",
             s_node_buf[0].received ? s_node_buf[0].sensor_value : 0xFFFF);
    ESP_LOGI(TAG, "  NodeBlock 2 (0x02 Hum)   : %s  raw=0x%04X",
             s_node_buf[1].received ? "DATA    " : "SENTINEL",
             s_node_buf[1].received ? s_node_buf[1].sensor_value : 0xFFFF);
    ESP_LOGI(TAG, "  NodeBlock 3 (0x03 Light) : SENTINEL (offline in R&D)");
    ESP_LOGI(TAG, "  CRC = 0x%04X", crc);
}

/* ============================================================
 * SECTION 13 — LAYER 2 TX WITH ACK + RETRY
 * ============================================================
 *
 * Called from Task D which already holds s_lora_mutex.
 * Switches to 434 MHz, transmits, waits for 0xAA ACK, retries up to 3×.
 * Restores 433 MHz RX on exit.
 */
static void transmit_l2_packet(void)
{
    bool ack_received = false;

    for (uint8_t attempt = 1; attempt <= LAYER2_MAX_RETRIES; attempt++) {
        ESP_LOGI(TAG, "L2 TX attempt %u/%u", attempt, LAYER2_MAX_RETRIES);

        /* Switch to 434 MHz */
        set_frequency_434();

        /* Set payload length to L2 size */
        write_reg(REG_PAYLOAD_LENGTH, L2_PACKET_SIZE);

        /* DIO0 = TX done */
        write_reg(REG_DIO_MAPPING1, 0x40);

        /* Load packet into TX FIFO half */
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_STDBY);
        write_reg(REG_FIFO_ADDR_PTR, 0x80);
        write_fifo(s_l2_packet, L2_PACKET_SIZE);

        /* Start TX */
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_TX);

        /* Wait for TX done — poll IRQ flag (timeout 2s) */
        uint32_t tx_start = (uint32_t)(esp_timer_get_time() / 1000ULL);
        bool     tx_done  = false;
        while (!tx_done) {
            if (read_reg(REG_IRQ_FLAGS) & IRQ_TX_DONE_MASK) {
                tx_done = true;
            }
            if (!tx_done) {
                uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
                if ((now - tx_start) > 2000) {
                    ESP_LOGE(TAG, "L2 TX timeout — IRQ_TX_DONE never set");
                    break;
                }
                vTaskDelay(pdMS_TO_TICKS(5));
            }
        }
        write_reg(REG_IRQ_FLAGS, IRQ_TX_DONE_MASK);

        if (!tx_done) {
            continue;   /* retry — TX hardware failed */
        }

        ESP_LOGI(TAG, "L2 TX complete — switching to RX 434 MHz for ACK");

        /* Switch to RX on 434 MHz to receive 1-byte ACK */
        write_reg(REG_DIO_MAPPING1,   0x00);           /* DIO0 = RxDone */
        write_reg(REG_PAYLOAD_LENGTH, L2_ACK_SIZE);
        write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
        write_reg(REG_FIFO_ADDR_PTR,     0x00);
        write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

        /* Poll for RxDone within ACK timeout */
        uint32_t ack_start = (uint32_t)(esp_timer_get_time() / 1000ULL);
        bool     timed_out = false;

        while (!(read_reg(REG_IRQ_FLAGS) & IRQ_RX_DONE_MASK)) {
            uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
            if ((now - ack_start) > LAYER2_ACK_TIMEOUT_MS) {
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

        /* Check for CRC error on ACK */
        bool crc_err = (read_reg(REG_IRQ_FLAGS) & IRQ_PAYLOAD_CRC_ERR) != 0;
        write_reg(REG_IRQ_FLAGS, 0xFF);

        if (crc_err) {
            ESP_LOGW(TAG, "L2 ACK CRC error on attempt %u", attempt);
            continue;
        }

        /* Read ACK byte */
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

    /* Restore 433 MHz RX */
    set_frequency_433();
    write_reg(REG_DIO_MAPPING1,   0x00);
    write_reg(REG_PAYLOAD_LENGTH, L1_PACKET_SIZE);  /* FIX B: 12 */
    write_reg(REG_FIFO_RX_BASE_ADDR, 0x00);
    write_reg(REG_FIFO_ADDR_PTR,     0x00);
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    ESP_LOGI(TAG, "Restored 433 MHz RX mode for next cycle");
}

/* ============================================================
 * SECTION 14 — ESP_TIMER CALLBACKS (CYCLE TIMING)
 * ============================================================ */

static esp_timer_handle_t s_agg_timer;
static esp_timer_handle_t s_cycle_timer;

/*
 * agg_timer_cb — fires at T=25s. Wakes Task C to aggregate.
 * Runs in timer ISR context — no blocking allowed.
 */
static void agg_timer_cb(void *arg)
{
    BaseType_t hp = pdFALSE;
    xSemaphoreGiveFromISR(s_agg_trigger, &hp);
    portYIELD_FROM_ISR(hp);
}

/*
 * cycle_reset_timer_cb — fires at T=30s.
 *
 * FIX E: Gives BOTH s_cycle_reset_c (for Task C) and
 * s_cycle_reset_mgr (for CycleMgr) independently.
 * Previously only one semaphore existed and was re-given unsafely.
 */
static void cycle_reset_timer_cb(void *arg)
{
    BaseType_t hp = pdFALSE;
    xSemaphoreGiveFromISR(s_cycle_reset_c,   &hp);  /* FIX E */
    xSemaphoreGiveFromISR(s_cycle_reset_mgr, &hp);  /* FIX E */
    portYIELD_FROM_ISR(hp);
}

/*
 * start_cycle_timers — arms both one-shot timers for the current cycle.
 * Called once at boot and then by task_cycle_manager at each T=30s reset.
 */
static void start_cycle_timers(void)
{
    esp_timer_create_args_t agg_args = {
        .callback = agg_timer_cb,
        .name     = "agg_timer",
    };
    esp_timer_create(&agg_args, &s_agg_timer);
    esp_timer_start_once(s_agg_timer, 25000000ULL);   /* 25s */

    esp_timer_create_args_t cycle_args = {
        .callback = cycle_reset_timer_cb,
        .name     = "cycle_timer",
    };
    esp_timer_create(&cycle_args, &s_cycle_timer);
    esp_timer_start_once(s_cycle_timer, 30000000ULL);  /* 30s */

    ESP_LOGI(TAG, "Cycle timers armed — T=25s aggregation, T=30s reset");
}

/* ============================================================
 * SECTION 15 — TASK A: LoRa RX
 * ============================================================
 *
 * Continuously listens on 433.0 MHz for Layer 1 packets.
 *
 * FIX D: Mutex is now held ONLY during the full FIFO-read session,
 * not during the idle poll loop. Between packets, Task A polls the
 * IRQ flag WITHOUT holding the mutex.
 *
 * When s_tx_active is set by Task D, Task A stops all radio access
 * and yields. Task D then acquires the mutex safely for a full TX session.
 *
 * This prevents Task D from acquiring the mutex mid-FIFO-read and
 * corrupting an in-progress packet reception.
 */
static void task_lora_rx(void *arg)
{
    ESP_LOGI(TAG, "Task A (LoRa RX) started");

    /* Start continuous RX */
    write_reg(REG_OP_MODE, MODE_LONG_RANGE_MODE | MODE_RX_CONTINUOUS);

    uint8_t raw_buf[L1_PACKET_SIZE];  /* 12 bytes — FIX B */

    while (1) {
        /*
         * FIX D: If TX is in progress, yield immediately.
         * Do not touch any SPI registers while Task D may be mid-transaction.
         */
        if (s_tx_active) {
            vTaskDelay(pdMS_TO_TICKS(10));
            continue;
        }

        /* Fast poll — no mutex needed for a single register read */
        uint8_t irq = read_reg(REG_IRQ_FLAGS);

        if (!(irq & IRQ_RX_DONE_MASK)) {
            vTaskDelay(pdMS_TO_TICKS(5));
            continue;
        }

        /*
         * RxDone set. Now acquire mutex to protect the FIFO read session.
         * FIX D: mutex guards the full FIFO session, not individual reg reads.
         */
        xSemaphoreTake(s_lora_mutex, portMAX_DELAY);

        /* Re-check s_tx_active — Task D may have set it between our poll and here */
        if (s_tx_active) {
            xSemaphoreGive(s_lora_mutex);
            vTaskDelay(pdMS_TO_TICKS(10));
            continue;
        }

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
            ESP_LOGW(TAG, "Task A: wrong size %u (expected %u)",
                     nb_bytes, L1_PACKET_SIZE);
            continue;
        }

        /* Read packet from FIFO */
        uint8_t rx_addr = read_reg(REG_FIFO_RX_CURR_ADDR);
        write_reg(REG_FIFO_ADDR_PTR, rx_addr);
        read_fifo(raw_buf, L1_PACKET_SIZE);
        write_reg(REG_IRQ_FLAGS, 0xFF);

        xSemaphoreGive(s_lora_mutex);
        /* Mutex released — radio session complete */

        /* Push raw packet to Task B */
        if (xQueueSend(s_raw_rx_queue, raw_buf, pdMS_TO_TICKS(10)) != pdTRUE) {
            ESP_LOGW(TAG, "Task A: raw RX queue full — packet dropped");
        } else {
            ESP_LOGD(TAG, "Task A: queued Node 0x%02X for validation", raw_buf[0]);
        }
    }
}

/* ============================================================
 * SECTION 16 — TASK B: VALIDATION
 * ============================================================
 *
 * Receives raw 12-byte packets from Task A, runs full validation,
 * pushes validated_packet_t to Task C. Updates last_packet_id for
 * deduplication. No radio access — pure data processing.
 */
static void task_validation(void *arg)
{
    ESP_LOGI(TAG, "Task B (Validation) started");

    uint8_t           raw_buf[L1_PACKET_SIZE];   /* 12 bytes — FIX B */
    validated_packet_t vp;

    while (1) {
        if (xQueueReceive(s_raw_rx_queue, raw_buf, portMAX_DELAY) == pdTRUE) {

            if (validate_l1_packet(raw_buf, &vp)) {
                /* Update last_packet_id before pushing */
                uint8_t idx = vp.node_id - 1;
                s_node_buf[idx].last_packet_id = vp.packet_id;

                /* Decode and log sensor value */
                if (vp.node_id == NODE_TEMP_ID) {
                    float temp_c = (float)(int16_t)vp.sensor_value / 10.0f;
                    ESP_LOGI(TAG,
                             "Task B: Node 0x01 VALID — Temp=%.1f C "
                             "PktID=0x%04X TS=%lu ms",
                             temp_c, vp.packet_id, (unsigned long)vp.timestamp);
                } else {
                    float hum = (float)vp.sensor_value / 10.0f;
                    ESP_LOGI(TAG,
                             "Task B: Node 0x02 VALID — Hum=%.1f%% "
                             "PktID=0x%04X TS=%lu ms",
                             hum, vp.packet_id, (unsigned long)vp.timestamp);
                }

                if (xQueueSend(s_validated_queue, &vp, pdMS_TO_TICKS(10)) != pdTRUE) {
                    ESP_LOGW(TAG, "Task B: validated queue full");
                }
            }
        }
    }
}

/* ============================================================
 * SECTION 17 — TASK C: AGGREGATION
 * ============================================================
 *
 * Drains validated packets into node buffers.
 * At T=25s: builds Layer 2 packet, signals Task D.
 *
 * FIX E: Listens on s_cycle_reset_c (dedicated to Task C only).
 * At T=30s: clears buffers for the next cycle.
 */
static void task_aggregation(void *arg)
{
    ESP_LOGI(TAG, "Task C (Aggregation) started");

    validated_packet_t vp;
    uint8_t dummy = 1;

    while (1) {
        /* Drain validated queue — non-blocking, runs between events */
        while (xQueueReceive(s_validated_queue, &vp, pdMS_TO_TICKS(10)) == pdTRUE) {
            uint8_t idx = vp.node_id - 1;
            if (idx < 2) {
                s_node_buf[idx].received     = true;
                s_node_buf[idx].node_id      = vp.node_id;
                s_node_buf[idx].packet_id    = vp.packet_id;
                s_node_buf[idx].timestamp    = vp.timestamp;
                s_node_buf[idx].sensor_value = vp.sensor_value;
                ESP_LOGD(TAG, "Task C: buffered Node 0x%02X", vp.node_id);
            }
        }

        /* T=25s: Aggregation trigger */
        if (xSemaphoreTake(s_agg_trigger, 0) == pdTRUE) {

            /* Drain any last-minute packets arriving just at T=25s */
            while (xQueueReceive(s_validated_queue, &vp, pdMS_TO_TICKS(5)) == pdTRUE) {
                uint8_t idx = vp.node_id - 1;
                if (idx < 2) {
                    s_node_buf[idx].received     = true;
                    s_node_buf[idx].node_id      = vp.node_id;
                    s_node_buf[idx].packet_id    = vp.packet_id;
                    s_node_buf[idx].timestamp    = vp.timestamp;
                    s_node_buf[idx].sensor_value = vp.sensor_value;
                }
            }

            ESP_LOGI(TAG, "--- T=25s: Aggregating ---");
            ESP_LOGI(TAG, "  Node 0x01 (Temp) : %s",
                     s_node_buf[0].received ? "RECEIVED" : "ABSENT");
            ESP_LOGI(TAG, "  Node 0x02 (Hum)  : %s",
                     s_node_buf[1].received ? "RECEIVED" : "ABSENT");
            ESP_LOGI(TAG, "  Node 0x03 (Light): OFFLINE (R&D sentinel)");

            build_l2_packet();
            s_l2_packet_ready = true;

            xQueueSend(s_l2_tx_trigger, &dummy, pdMS_TO_TICKS(100));
        }

        /* FIX E: T=30s — using dedicated semaphore s_cycle_reset_c */
        if (xSemaphoreTake(s_cycle_reset_c, 0) == pdTRUE) {
            s_node_buf[0].received = false;
            s_node_buf[1].received = false;
            s_node_buf[2].received = false;
            s_l2_packet_ready = false;
            ESP_LOGI(TAG, "--- T=30s: Cycle reset — buffers cleared ---");
        }
    }
}

/* ============================================================
 * SECTION 18 — TASK D: LoRa TX
 * ============================================================
 *
 * Waits for TX trigger from Task C, then:
 *
 * FIX D: Sets s_tx_active=true BEFORE taking the mutex.
 * This signals Task A to stop polling and exit any in-progress
 * radio access. Task D then acquires the mutex cleanly.
 * After TX+ACK completes, s_tx_active is cleared and mutex released,
 * allowing Task A to resume RX.
 */
static void task_lora_tx(void *arg)
{
    ESP_LOGI(TAG, "Task D (LoRa TX) started");

    uint8_t trigger;

    while (1) {
        if (xQueueReceive(s_l2_tx_trigger, &trigger, portMAX_DELAY) == pdTRUE) {
            ESP_LOGI(TAG, "Task D: TX trigger received");

            /*
             * FIX D: Signal Task A to stop radio access BEFORE taking mutex.
             * Give Task A a brief moment to finish any current register read.
             */
            s_tx_active = true;
            vTaskDelay(pdMS_TO_TICKS(15));   /* let Task A see the flag and yield */

            xSemaphoreTake(s_lora_mutex, portMAX_DELAY);
            ESP_LOGI(TAG, "Task D: mutex acquired — L2 TX begin");

            transmit_l2_packet();

            xSemaphoreGive(s_lora_mutex);

            /* FIX D: Clear flag — Task A resumes normal RX polling */
            s_tx_active = false;

            ESP_LOGI(TAG, "Task D: mutex released — Task A resumes RX");
        }
    }
}

/* ============================================================
 * SECTION 19 — CYCLE MANAGER TASK
 * ============================================================
 *
 * FIX E: Waits on s_cycle_reset_mgr (dedicated semaphore for this task).
 * No longer needs to re-give a shared semaphore to Task C.
 * Cleanly stops old timers and re-arms for the next cycle.
 */
static void task_cycle_manager(void *arg)
{
    ESP_LOGI(TAG, "Cycle manager task started");

    while (1) {
        /* FIX E: dedicated semaphore — no re-give hack needed */
        if (xSemaphoreTake(s_cycle_reset_mgr, portMAX_DELAY) == pdTRUE) {
            /* Small delay to let Task C process its reset first */
            vTaskDelay(pdMS_TO_TICKS(50));

            /* Stop and delete expired timers */
            esp_timer_stop(s_agg_timer);
            esp_timer_delete(s_agg_timer);
            esp_timer_stop(s_cycle_timer);
            esp_timer_delete(s_cycle_timer);

            /* Arm timers for next cycle */
            start_cycle_timers();
        }
    }
}

/* ============================================================
 * SECTION 20 — NODE BUFFER INITIALISATION
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
    }
    /* Node 0x03 offline in R&D — received stays false permanently */
}

/* ============================================================
 * SECTION 21 — APP_MAIN
 * ============================================================ */

void app_main(void)
{
    ESP_LOGI(TAG, "========================================");
    ESP_LOGI(TAG, " Estrotech Local Gateway GW1 — R&D Phase");
    ESP_LOGI(TAG, " ESP32-C3 Super Mini | ESP-IDF v5.5.2");
    ESP_LOGI(TAG, " Active: Node 0x01 (T=0s), Node 0x02 (T=4s)");
    ESP_LOGI(TAG, " Offline: Node 0x03 (sentinel always)");
    ESP_LOGI(TAG, " L1=433MHz L2=434MHz ACK=0xAA Retry=3");
    ESP_LOGI(TAG, "========================================");

    /* Step 1: SPI init */
    sx1278_init_spi();
    ESP_LOGI(TAG, "SPI bus initialized on SCK=%d MISO=%d MOSI=%d NSS=%d",
             PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);

    /* Step 2: DIO0 as input */
    gpio_set_direction(PIN_DIO0, GPIO_MODE_INPUT);

    /* Step 3: Hardware reset */
    sx1278_reset();
    ESP_LOGI(TAG, "SX1278 hardware reset complete");

    /* Step 4: Sanity check — halts if SPI broken */
    sx1278_sanity_check();

    /* Step 5: Configure radio */
    sx1278_configure_radio();

    /* Step 6: Set 433 MHz for Layer 1 RX */
    set_frequency_433();

    /* Step 7: Init node buffers */
    init_node_buffers();

    /* Step 8: FreeRTOS synchronisation primitives */
    s_lora_mutex     = xSemaphoreCreateBinary();
    xSemaphoreGive(s_lora_mutex);

    s_agg_trigger    = xSemaphoreCreateBinary();

    /* FIX E: two separate reset semaphores */
    s_cycle_reset_c   = xSemaphoreCreateBinary();
    s_cycle_reset_mgr = xSemaphoreCreateBinary();

    s_raw_rx_queue    = xQueueCreate(8, L1_PACKET_SIZE);          /* 12 bytes — FIX B */
    s_validated_queue = xQueueCreate(8, sizeof(validated_packet_t));
    s_l2_tx_trigger   = xQueueCreate(1, sizeof(uint8_t));

    if (!s_lora_mutex || !s_agg_trigger || !s_cycle_reset_c ||
        !s_cycle_reset_mgr || !s_raw_rx_queue ||
        !s_validated_queue || !s_l2_tx_trigger) {
        ESP_LOGE(TAG, "FATAL: FreeRTOS object creation failed");
        while (1) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    }

    /* Step 9: Arm cycle timers */
    start_cycle_timers();

    /*
     * Step 10: Create tasks.
     *
     * FIX A: ESP32-C3 is SINGLE CORE — xTaskCreatePinnedToCore with core=1
     * causes an assert crash (seen in monitor log). All tasks use xTaskCreate()
     * which lets the FreeRTOS scheduler place them on the only available core.
     */
    xTaskCreate(task_lora_rx,       "TaskA_RX",  4096, NULL, 5, NULL);
    xTaskCreate(task_validation,    "TaskB_Val", 4096, NULL, 5, NULL);
    xTaskCreate(task_aggregation,   "TaskC_Agg", 4096, NULL, 4, NULL);
    xTaskCreate(task_lora_tx,       "TaskD_TX",  4096, NULL, 5, NULL);
    xTaskCreate(task_cycle_manager, "CycleMgr",  2048, NULL, 2, NULL);

    ESP_LOGI(TAG, "All tasks created — gateway operational");
    ESP_LOGI(TAG, "Listening on 433.0 MHz for Node 0x01 and Node 0x02");
}