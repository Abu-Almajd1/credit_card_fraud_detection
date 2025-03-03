-- Drop existing tables to avoid duplication
DROP TABLE IF EXISTS fact_transactions, transactions, customers, merchants, locations;

-- Table for Customers (Dimension Table)
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    cc_num BIGINT UNIQUE NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(10),
    dob DATE,
    job VARCHAR(100),
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code INT,
    city_pop INT
);

-- Table for Merchants (Dimension Table)
CREATE TABLE merchants (
    merchant_id SERIAL PRIMARY KEY,
    merchant_name VARCHAR(255) UNIQUE NOT NULL,
    category VARCHAR(100),
    merch_lat DECIMAL(10, 6),
    merch_long DECIMAL(10, 6)
);

-- Table for Locations (Dimension Table)
CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zip_code INT UNIQUE NOT NULL,
    lat DECIMAL(10, 6),
    long DECIMAL(10, 6),
    city_pop INT
);

-- Table for Transactions (Dimension Table)
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    trans_num VARCHAR(100) UNIQUE NOT NULL,
    trans_date TIMESTAMP NOT NULL,
    cc_num BIGINT REFERENCES customers(cc_num) ON DELETE CASCADE,
    merchant_id INT REFERENCES merchants(merchant_id) ON DELETE CASCADE,
    amt DECIMAL(10, 2) NOT NULL,
    unix_time BIGINT,
    is_fraud BOOLEAN NOT NULL
);

-- Fact Table for Power BI Reporting
CREATE TABLE fact_transactions (
    fact_id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
    merchant_id INT REFERENCES merchants(merchant_id) ON DELETE CASCADE,
    location_id INT REFERENCES locations(location_id) ON DELETE CASCADE,
    trans_date TIMESTAMP NOT NULL,
    amt DECIMAL(10, 2) NOT NULL,
    category VARCHAR(100),
    is_fraud BOOLEAN NOT NULL,
    day_of_week VARCHAR(20),
    hour_of_day INT,
    is_high_risk BOOLEAN GENERATED ALWAYS AS (
        CASE 
            WHEN amt > 500 THEN TRUE
            ELSE FALSE
        END
    ) STORED
);
