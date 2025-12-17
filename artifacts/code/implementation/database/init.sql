-- Production Events Table
CREATE TABLE IF NOT EXISTS production_events (
    event_id SERIAL PRIMARY KEY,
    can_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    cycle_time_ms INTEGER NOT NULL,
    fill_level_ml INTEGER NOT NULL,
    seal_verified BOOLEAN NOT NULL,
    quality_result VARCHAR(10) NOT NULL CHECK (quality_result IN ('PASS', 'FAIL')),
    reject_reason VARCHAR(255)
);

CREATE INDEX idx_event_timestamp ON production_events(event_timestamp);
CREATE INDEX idx_quality_result ON production_events(quality_result);

-- Fault Logs Table
CREATE TABLE IF NOT EXISTS fault_logs (
    fault_id VARCHAR(50) PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    fault_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    cleared_at TIMESTAMP,
    diagnostics JSONB
);

CREATE INDEX idx_fault_sensor ON fault_logs(sensor_id);
CREATE INDEX idx_fault_severity ON fault_logs(severity);

-- System Metrics Table
CREATE TABLE IF NOT EXISTS system_metrics (
    metric_id SERIAL PRIMARY KEY,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    cans_processed INTEGER NOT NULL,
    cans_passed INTEGER NOT NULL,
    cans_failed INTEGER NOT NULL,
    avg_cycle_time_ms INTEGER NOT NULL,
    system_mode VARCHAR(20) NOT NULL,
    uptime_seconds INTEGER NOT NULL
);

CREATE INDEX idx_metrics_recorded ON system_metrics(recorded_at);

-- Production Summary View
CREATE OR REPLACE VIEW production_summary AS
SELECT
    DATE(event_timestamp) as production_date,
    COUNT(*) as total_cans,
    SUM(CASE WHEN quality_result = 'PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN quality_result = 'FAIL' THEN 1 ELSE 0 END) as failed,
    ROUND(AVG(cycle_time_ms), 2) as avg_cycle_time_ms,
    ROUND(AVG(fill_level_ml), 2) as avg_fill_level_ml,
    ROUND(100.0 * SUM(CASE WHEN quality_result = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate_pct
FROM production_events
GROUP BY DATE(event_timestamp)
ORDER BY production_date DESC;

-- Insert some initial test data (optional)
INSERT INTO production_events (can_id, cycle_time_ms, fill_level_ml, seal_verified, quality_result)
VALUES 
    ('CAN00001', 892, 328, true, 'PASS'),
    ('CAN00002', 915, 331, true, 'PASS'),
    ('CAN00003', 1123, 318, true, 'FAIL');

COMMIT;
