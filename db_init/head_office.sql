-- head_office.sql
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
    total DECIMAL(10, 2) NOT NULL,
    last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    source_branch VARCHAR(10) NULL
);

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
    total,
    source_branch
FROM 
    product_sales
ORDER BY 
    date, region, product;