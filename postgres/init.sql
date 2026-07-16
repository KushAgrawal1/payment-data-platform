-- ==========================================
-- 1. CLEANUP (For development / clean slate)
-- ==========================================
DROP TABLE IF EXISTS fact_fraud_events CASCADE;
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_currency CASCADE;
DROP TABLE IF EXISTS dim_merchant CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- ==========================================
-- 2. DIMENSION TABLES
-- ==========================================

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,           -- yyyymmdd
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,     -- Surrogate Key
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(100),
    country VARCHAR(50),
    join_date DATE,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_merchant (
    merchant_sk SERIAL PRIMARY KEY,     -- Surrogate Key
    merchant_id VARCHAR(50) UNIQUE NOT NULL,
    merchant_name VARCHAR(100),
    category VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_currency (
    currency_code CHAR(3) PRIMARY KEY,
    currency_name VARCHAR(50) NOT NULL,
    exchange_rate_to_gbp NUMERIC(10,4) NOT NULL   -- Base conversion rate
);

-- ==========================================
-- 3. FACT TABLES
-- ==========================================

CREATE TABLE fact_transactions (
    transaction_sk SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    customer_sk INT REFERENCES dim_customer(customer_sk), -- FIXED: references customer_sk now
    merchant_sk INT REFERENCES dim_merchant(merchant_sk),
    date_key INT REFERENCES dim_date(date_key),
    currency_code CHAR(3) REFERENCES dim_currency(currency_code),
    amount_gbp NUMERIC(15,2) NOT NULL,
    original_amount NUMERIC(15,2) NOT NULL,
    payment_method VARCHAR(20),
    status VARCHAR(20) CHECK (status IN ('SUCCESS', 'FAILED', 'PENDING', 'REFUND', 'CHARGEBACK')), -- Added 'PENDING' to match ingestion status
    fraud_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_fraud_events (
    fraud_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) REFERENCES fact_transactions(transaction_id),
    rule_name VARCHAR(100) NOT NULL,
    risk_score NUMERIC(5,2) NOT NULL,
    action_taken VARCHAR(50),   -- e.g., 'BLOCKED', 'REVIEW'
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 4. PERFORMANCE INDEXES
-- ==========================================
CREATE INDEX idx_fact_tx_date ON fact_transactions(date_key);
CREATE INDEX idx_fact_tx_merchant ON fact_transactions(merchant_sk);
CREATE INDEX idx_fact_tx_status ON fact_transactions(status);

-- ==========================================
-- 5. SEED INITIAL STATIC DIMENSION DATA
-- ==========================================
INSERT INTO dim_currency (currency_code, currency_name, exchange_rate_to_gbp) VALUES
('GBP', 'British Pound Sterling', 1.0000),
('USD', 'United States Dollar', 0.8100),
('EUR', 'Euro', 0.8800),
('CAD', 'Canadian Dollar', 0.6000),
('AUD', 'Australian Dollar', 0.5400)
ON CONFLICT (currency_code) DO UPDATE 
SET exchange_rate_to_gbp = EXCLUDED.exchange_rate_to_gbp;