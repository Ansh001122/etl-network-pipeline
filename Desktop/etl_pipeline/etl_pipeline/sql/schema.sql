-- ============================================================
--  ETL Pipeline — MySQL Schema
--  Database : cisco_etl_db
--  Author   : Ansh Raj
-- ============================================================

CREATE DATABASE IF NOT EXISTS cisco_etl_db;
USE cisco_etl_db;

-- ─────────────────────────────────────────────
--  DIMENSION TABLE: devices
--  Stores device metadata (slowly changing)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS devices (
    device_id       VARCHAR(20)     NOT NULL,
    device_name     VARCHAR(100),
    device_type     VARCHAR(50),
    region          VARCHAR(50),
    registered_at   DATETIME        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────
--  FACT TABLE: network_events
--  Stores measurable network activity events
--  Foreign key → devices dimension table
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS network_events (
    id                  BIGINT          NOT NULL AUTO_INCREMENT,
    device_id           VARCHAR(20)     NOT NULL,
    event_timestamp     DATETIME        NOT NULL,
    event_type          VARCHAR(50)     NOT NULL,
    bytes_transferred   BIGINT          DEFAULT 0,
    bytes_kb            DECIMAL(12, 2)  DEFAULT 0.00,
    protocol            VARCHAR(20),
    status_code         SMALLINT,
    region              VARCHAR(50),
    event_date          DATE,
    event_hour          TINYINT,
    load_timestamp      DATETIME        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_device_id         (device_id),
    INDEX idx_event_timestamp   (event_timestamp),
    INDEX idx_event_type        (event_type),
    INDEX idx_event_date        (event_date),
    CONSTRAINT fk_device
        FOREIGN KEY (device_id)
        REFERENCES devices(device_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────
--  PIPELINE AUDIT TABLE
--  Tracks every pipeline run for observability
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          INT             NOT NULL AUTO_INCREMENT,
    run_timestamp   DATETIME        DEFAULT CURRENT_TIMESTAMP,
    source_file     VARCHAR(255),
    rows_extracted  INT,
    rows_loaded     INT,
    rows_rejected   INT,
    status          ENUM('SUCCESS', 'FAILED', 'PARTIAL'),
    error_message   TEXT,
    PRIMARY KEY (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────
--  ANALYTICAL VIEW: hourly device summary
--  Demonstrates data modeling for BI/reporting
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_hourly_device_summary AS
SELECT
    device_id,
    event_date,
    event_hour,
    COUNT(*)                    AS total_events,
    SUM(bytes_transferred)      AS total_bytes,
    ROUND(AVG(bytes_transferred), 2) AS avg_bytes_per_event,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count
FROM network_events
GROUP BY device_id, event_date, event_hour;

-- ─────────────────────────────────────────────
--  SAMPLE COMPLEX QUERY (window functions)
--  Running total + rank per device per day
-- ─────────────────────────────────────────────
/*
SELECT
    device_id,
    event_timestamp,
    bytes_transferred,
    SUM(bytes_transferred) OVER (
        PARTITION BY device_id, event_date
        ORDER BY event_timestamp
    ) AS running_daily_bytes,
    RANK() OVER (
        PARTITION BY event_date
        ORDER BY bytes_transferred DESC
    ) AS daily_transfer_rank
FROM network_events
WHERE event_date = CURDATE();
*/
