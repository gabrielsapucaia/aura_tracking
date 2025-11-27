CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry_env (
    time        TIMESTAMPTZ       NOT NULL,
    device_id   TEXT              NOT NULL,
    temperature DOUBLE PRECISION  NULL,
    humidity    DOUBLE PRECISION  NULL,
    pressure    DOUBLE PRECISION  NULL,
    raw_payload JSONB             NULL
);

SELECT create_hypertable('telemetry_env', 'time', if_not_exists => TRUE);

CREATE TABLE device_status (
    time      TIMESTAMPTZ NOT NULL,
    device_id TEXT        NOT NULL,
    status    TEXT        NOT NULL,
    topic     TEXT        NOT NULL
);

SELECT create_hypertable('device_status', 'time', if_not_exists => TRUE);

CREATE TABLE tablet_telemetry (
    time                       TIMESTAMPTZ     NOT NULL,
    ts_epoch_ms                BIGINT          NOT NULL,
    schema_version             TEXT,
    device_id                  TEXT            NOT NULL,
    operator_id                TEXT,
    equipment_tag              TEXT,
    truck_status               TEXT,
    seq_id                     BIGINT,
    gnss_fix                   TEXT,
    gnss_provider              TEXT,
    gnss_lat                   DOUBLE PRECISION,
    gnss_lon                   DOUBLE PRECISION,
    gnss_alt                   DOUBLE PRECISION,
    gnss_speed                 DOUBLE PRECISION,
    gnss_course                DOUBLE PRECISION,
    gnss_hdop                  DOUBLE PRECISION,
    gnss_vdop                  DOUBLE PRECISION,
    gnss_pdop                  DOUBLE PRECISION,
    gnss_num_sats              INT,
    gnss_sats_visible          INT,
    gnss_cn0_avg               DOUBLE PRECISION,
    gnss_cn0_min               DOUBLE PRECISION,
    gnss_cn0_max               DOUBLE PRECISION,
    gnss_cn0_p25               DOUBLE PRECISION,
    gnss_cn0_p50               DOUBLE PRECISION,
    gnss_cn0_p75               DOUBLE PRECISION,
    gnss_accuracy_m            DOUBLE PRECISION,
    gnss_vert_accuracy_m       DOUBLE PRECISION,
    gnss_speed_accuracy_mps    DOUBLE PRECISION,
    gnss_bearing_accuracy_deg  DOUBLE PRECISION,
    gnss_has_l5                BOOLEAN,
    gnss_raw_supported         BOOLEAN,
    gnss_raw_count             INT,
    gnss_elapsed_realtime_nanos BIGINT,
    gnss_gps_visible           INT,
    gnss_gps_used              INT,
    gnss_glonass_visible       INT,
    gnss_glonass_used          INT,
    gnss_galileo_visible       INT,
    gnss_galileo_used          INT,
    gnss_beidou_visible        INT,
    gnss_beidou_used           INT,
    gnss_qzss_visible          INT,
    gnss_qzss_used             INT,
    gnss_sbas_visible          INT,
    gnss_sbas_used             INT,
    baro_pressure_hpa          DOUBLE PRECISION,
    baro_altitude_m            DOUBLE PRECISION,
    imu_acc_norm_rms           DOUBLE PRECISION,
    imu_jerk_norm_rms          DOUBLE PRECISION,
    imu_gyro_norm_rms          DOUBLE PRECISION,
    imu_yaw_rate_deg_s         DOUBLE PRECISION,
    imu_samples                INT,
    imu_fps_eff                DOUBLE PRECISION,
    imu_pitch_deg              DOUBLE PRECISION,
    imu_roll_deg               DOUBLE PRECISION,
    imu_yaw_deg                DOUBLE PRECISION,
    imu_q_w                    DOUBLE PRECISION,
    imu_q_x                    DOUBLE PRECISION,
    imu_q_y                    DOUBLE PRECISION,
    imu_q_z                    DOUBLE PRECISION,
    imu_acc_accuracy           TEXT,
    imu_gyro_accuracy          TEXT,
    imu_rotation_accuracy      TEXT,
    imu_motion_stationary      BOOLEAN,
    imu_motion_shock_level     TEXT,
    imu_motion_shock_score     DOUBLE PRECISION,
    imu_acc_longitudinal_mps2  DOUBLE PRECISION,
    imu_acc_lateral_mps2       DOUBLE PRECISION,
    imu_acc_vertical_mps2      DOUBLE PRECISION,
    imu_vehicle_tilt_pitch_deg DOUBLE PRECISION,
    imu_vehicle_tilt_roll_deg  DOUBLE PRECISION,
    imu_mag_field_strength_uT  DOUBLE PRECISION,
    payload_json               JSONB,
    gnss_raw                   JSONB
);

SELECT create_hypertable(
    'tablet_telemetry',
    'time',
    'device_id',
    4,
    if_not_exists => TRUE
);
