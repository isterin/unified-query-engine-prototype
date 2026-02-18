-- Initialize PostgreSQL with sample data at scale
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

-- Generate 10,000 customers programmatically
INSERT INTO customers (name, email, region, tier, created_at)
SELECT 
    'Customer ' || i AS name,
    'customer' || i || '@example.com' AS email,
    (ARRAY['North America', 'South America', 'Europe', 'Asia Pacific', 'Middle East', 'Africa'])[1 + (i % 6)] AS region,
    (ARRAY['standard', 'standard', 'standard', 'premium', 'premium', 'enterprise'])[1 + (i % 6)] AS tier,
    TIMESTAMP '2023-01-01' + (random() * INTERVAL '365 days') AS created_at
FROM generate_series(1, 10000) AS i;

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

COMMENT ON TABLE customers IS 'Customer master data - 10,000 customers across 6 regions';
COMMENT ON TABLE products IS 'Product catalog - 10 products across 4 categories';
