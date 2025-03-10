-- branch1.sql
-- Minimal schema for Branch Office One database

-- Drop table if it exists to ensure clean initialization
DROP TABLE IF EXISTS product_sales;

-- Create product_sales table
CREATE TABLE product_sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    product VARCHAR(100) NOT NULL,
    qty INT NOT NULL,
    cost DECIMAL(10, 2) NOT NULL,
    amt DECIMAL(10, 2) NOT NULL,
    tax DECIMAL(10, 2) NOT NULL,
    total DECIMAL(10, 2) NOT NULL
);

-- Insert sample sales data for Branch 1
INSERT INTO product_sales (date, region, product, qty, cost, amt, tax, total) VALUES
('2025-03-01', 'East', 'Paper', 73, 12.05, 545.35, 66.17, 1011.52),
('2025-03-01', 'East', 'Pens', 14, 2.19, 30.66, 2.15, 32.81),
('2025-03-02', 'East', 'Paper', 21, 12.05, 273.95, 19.04, 292.99),
('2025-03-02', 'North-East', 'Pens', 40, 2.19, 87.60, 6.13, 93.73);

-- Create a view for easy reporting
CREATE VIEW sales_summary AS
SELECT 
    DATE_FORMAT(date, '%d-%b') AS formatted_date,
    region,
    product,
    qty,
    cost,
    amt,
    tax,
    total
FROM 
    product_sales
ORDER BY 
    date, region, product;