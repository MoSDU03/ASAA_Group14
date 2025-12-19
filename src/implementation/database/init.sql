-- Can Filling System Database Schema
-- Initialize database for event logging and system monitoring

CREATE TABLE IF NOT EXISTS filling_events (
    event_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50) NOT NULL,
    can_id INTEGER,
    position_mm DECIMAL(6,2),
    fill_level_ml DECIMAL(6,2),
    cycle_time_ms INTEGER,
    fill_duration_ms INTEGER,
    valve_state VARCHAR(20),
    sensor_status VARCHAR(50),
    fault_code VARCHAR(50),
    fault_description TEXT,
    system_state VARCHAR(30) NOT NULL
);

CREATE INDEX idx_timestamp ON filling_events(timestamp);
CREATE INDEX idx_event_type ON filling_events(event_type);
CREATE INDEX idx_system_state ON filling_events(system_state);

CREATE TABLE IF NOT EXISTS system_metrics (
    metric_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(12,4),
    unit VARCHAR(20)
);

CREATE INDEX idx_metric_timestamp ON system_metrics(timestamp);
CREATE INDEX idx_metric_name ON system_metrics(metric_name);

CREATE TABLE IF NOT EXISTS fault_log (
    fault_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fault_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    component VARCHAR(50),
    detection_time_ms INTEGER,
    recovery_time_ms INTEGER,
    can_affected INTEGER,
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT
);

CREATE INDEX idx_fault_timestamp ON fault_log(timestamp);
CREATE INDEX idx_fault_severity ON fault_log(severity);

-- Initial system metrics
INSERT INTO system_metrics (metric_name, metric_value, unit) VALUES
('target_fill_volume', 330.0, 'ml'),
('fill_tolerance', 5.0, 'ml'),
('max_fill_time', 3000.0, 'ms'),
('position_timeout', 200.0, 'ms'),
('sensor_timeout', 200.0, 'ms'),
('emergency_response_max', 50.0, 'ms'),
('cycle_time_min', 600.0, 'ms'),
('cycle_time_max', 1500.0, 'ms');

-- Create view for successful fills
CREATE VIEW successful_fills AS
SELECT 
    event_id,
    timestamp,
    can_id,
    fill_level_ml,
    cycle_time_ms,
    fill_duration_ms
FROM filling_events
WHERE event_type = 'fill_complete'
    AND fill_level_ml BETWEEN 325.0 AND 335.0
    AND cycle_time_ms BETWEEN 600 AND 1500;

-- Create view for performance statistics
CREATE VIEW performance_stats AS
SELECT 
    COUNT(*) as total_fills,
    AVG(cycle_time_ms) as avg_cycle_time,
    STDDEV(cycle_time_ms) as stddev_cycle_time,
    MIN(cycle_time_ms) as min_cycle_time,
    MAX(cycle_time_ms) as max_cycle_time,
    AVG(fill_level_ml) as avg_fill_level,
    STDDEV(fill_level_ml) as stddev_fill_level,
    COUNT(CASE WHEN fill_level_ml BETWEEN 325.0 AND 335.0 THEN 1 END) as fills_in_spec,
    CAST(COUNT(CASE WHEN fill_level_ml BETWEEN 325.0 AND 335.0 THEN 1 END) AS FLOAT) / COUNT(*) * 100 as success_rate
FROM filling_events
WHERE event_type = 'fill_complete';

-- Create view for fault analysis
CREATE VIEW fault_analysis AS
SELECT 
    fault_type,
    severity,
    COUNT(*) as occurrence_count,
    AVG(detection_time_ms) as avg_detection_time,
    AVG(recovery_time_ms) as avg_recovery_time,
    SUM(CASE WHEN resolved = TRUE THEN 1 ELSE 0 END) as resolved_count
FROM fault_log
GROUP BY fault_type, severity
ORDER BY occurrence_count DESC;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO filling_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO filling_user;
