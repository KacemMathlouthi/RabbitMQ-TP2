-- branch2.sql
-- Minimal schema for Branch Office Two database

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

-- Insert sample sales data for Branch 2
INSERT INTO product_sales (date, region, product, qty, cost, amt, tax, total) VALUES
('2025-03-01', 'West', 'Paper', 33, 12.05, 427.35, 29.91, 457.26),
('2025-03-01', 'West', 'Paper', 10, 12.05, 120.50, 9.07, 138.57),
('2025-03-02', 'South-West', 'Notebooks', 25, 8.50, 212.50, 14.88, 227.38),
('2025-03-02', 'West', 'Desk Lamps', 5, 35.00, 175.00, 12.25, 187.25);

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