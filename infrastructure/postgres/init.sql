-- Initialize PostgreSQL with sample customer data
-- This represents transactional/operational data that would typically live in a relational DB

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    region VARCHAR(50) NOT NULL,
    tier VARCHAR(20) DEFAULT 'standard',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for common query patterns
CREATE INDEX idx_customers_region ON customers(region);
CREATE INDEX idx_customers_tier ON customers(tier);

-- Insert sample customers across different regions
INSERT INTO customers (name, email, region, tier, created_at) VALUES
    ('Alice Johnson', 'alice.johnson@example.com', 'North America', 'premium', '2024-01-15 10:30:00'),
    ('Bob Smith', 'bob.smith@example.com', 'North America', 'standard', '2024-02-20 14:45:00'),
    ('Carlos Rodriguez', 'carlos.rodriguez@example.com', 'South America', 'premium', '2024-01-10 09:15:00'),
    ('Diana Chen', 'diana.chen@example.com', 'Asia Pacific', 'enterprise', '2023-11-05 16:20:00'),
    ('Erik Larsson', 'erik.larsson@example.com', 'Europe', 'standard', '2024-03-01 11:00:00'),
    ('Fatima Al-Hassan', 'fatima.alhassan@example.com', 'Middle East', 'premium', '2024-02-14 08:30:00'),
    ('George Wilson', 'george.wilson@example.com', 'North America', 'standard', '2024-01-25 13:45:00'),
    ('Hiroko Tanaka', 'hiroko.tanaka@example.com', 'Asia Pacific', 'premium', '2024-02-28 10:00:00'),
    ('Ivan Petrov', 'ivan.petrov@example.com', 'Europe', 'enterprise', '2023-12-10 15:30:00'),
    ('Julia Santos', 'julia.santos@example.com', 'South America', 'standard', '2024-03-05 09:45:00'),
    ('Klaus Weber', 'klaus.weber@example.com', 'Europe', 'premium', '2024-01-20 14:00:00'),
    ('Lena Okonkwo', 'lena.okonkwo@example.com', 'Africa', 'standard', '2024-02-08 11:30:00'),
    ('Michael Brown', 'michael.brown@example.com', 'North America', 'enterprise', '2023-10-15 16:45:00'),
    ('Nina Volkov', 'nina.volkov@example.com', 'Europe', 'standard', '2024-03-10 08:15:00'),
    ('Omar Hassan', 'omar.hassan@example.com', 'Middle East', 'premium', '2024-01-30 12:00:00'),
    ('Priya Sharma', 'priya.sharma@example.com', 'Asia Pacific', 'standard', '2024-02-22 10:30:00'),
    ('Quinn O''Brien', 'quinn.obrien@example.com', 'Europe', 'premium', '2024-03-15 14:15:00'),
    ('Rosa Martinez', 'rosa.martinez@example.com', 'South America', 'enterprise', '2023-09-20 09:00:00'),
    ('Sven Andersson', 'sven.andersson@example.com', 'Europe', 'standard', '2024-02-05 11:45:00'),
    ('Tomoko Yamamoto', 'tomoko.yamamoto@example.com', 'Asia Pacific', 'premium', '2024-01-08 15:00:00');

-- Create a products table for additional join scenarios
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    active BOOLEAN DEFAULT true
);

INSERT INTO products (name, category, price) VALUES
    ('Basic Analytics Package', 'Analytics', 99.99),
    ('Pro Analytics Suite', 'Analytics', 299.99),
    ('Enterprise Data Platform', 'Analytics', 999.99),
    ('Cloud Storage - 100GB', 'Storage', 9.99),
    ('Cloud Storage - 1TB', 'Storage', 49.99),
    ('Cloud Storage - 10TB', 'Storage', 199.99),
    ('API Access - Basic', 'API', 29.99),
    ('API Access - Premium', 'API', 149.99),
    ('Support Package - Standard', 'Support', 199.99),
    ('Support Package - Premium', 'Support', 499.99);

COMMENT ON TABLE customers IS 'Customer master data - transactional source of truth';
COMMENT ON TABLE products IS 'Product catalog - transactional source of truth';
