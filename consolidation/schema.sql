-- Daily summary statistics
CREATE TABLE IF NOT EXISTS daily_summary (
    date DATE PRIMARY KEY,
    total_records INTEGER NOT NULL,
    total_routes INTEGER NOT NULL,
    total_vehicles INTEGER NOT NULL,
    avg_speed DECIMAL(5,2),
    delayed_count INTEGER NOT NULL,
    on_time_pct DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily performance by route
CREATE TABLE IF NOT EXISTS daily_route_performance (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    route VARCHAR(10) NOT NULL,
    readings INTEGER NOT NULL,
    delayed INTEGER NOT NULL,
    delay_pct DECIMAL(5,2),
    on_time_pct DECIMAL(5,2),
    avg_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, route)
);

-- Daily segment type performance (ROW vs mixed traffic)
CREATE TABLE IF NOT EXISTS daily_segment_performance (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    segment_type VARCHAR(20) NOT NULL,
    readings INTEGER NOT NULL,
    delayed INTEGER NOT NULL,
    delay_pct DECIMAL(5,2),
    on_time_pct DECIMAL(5,2),
    avg_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, segment_type)
);

-- Hourly breakdown by segment type (for time-of-day analysis)
CREATE TABLE IF NOT EXISTS hourly_segment_performance (
    id SERIAL PRIMARY KEY,
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    segment_type VARCHAR(20) NOT NULL,
    readings INTEGER NOT NULL,
    delayed INTEGER NOT NULL,
    delay_pct DECIMAL(5,2),
    avg_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hour, segment_type)
);

-- Individual segment performance
CREATE TABLE IF NOT EXISTS segment_summary (
    id SERIAL PRIMARY KEY,
    segment_name VARCHAR(100) NOT NULL,
    segment_type VARCHAR(20) NOT NULL,
    readings INTEGER NOT NULL,
    delay_pct DECIMAL(5,2),
    avg_speed DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(segment_name)
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_daily_route_date ON daily_route_performance(date);
CREATE INDEX IF NOT EXISTS idx_daily_segment_date ON daily_segment_performance(date);
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary(date);
