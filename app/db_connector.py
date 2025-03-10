import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG

class DatabaseConnector:
    def __init__(self, db_type):
        """
        Initialize database connection for specified type
        :param db_type: 'head_office', 'branch1', or 'branch2'
        """
        self.db_type = db_type
        self.config = DB_CONFIG[db_type]
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to the database"""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
            
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                return True
        except Error as e:
            print(f"Error connecting to MySQL Database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            if self.cursor:
                self.cursor.close()
            self.connection.close()
    
    def execute_query(self, query, params=None, commit=False):
        """Execute a SQL query with optional parameters"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
                
            self.cursor.execute(query, params or ())
            
            if commit:
                self.connection.commit()
                return True
            
            result = self.cursor.fetchall()
            return result
            
        except Error as e:
            print(f"Error executing query: {e}")
            return None
    
    def get_unsynchronized_sales(self):
        """Get sales records that haven't been sent to head office"""
        if self.db_type in ['branch1', 'branch2']:
            query = """
            SELECT 
                sale_id, date, region, product, qty, cost, amt, tax, total
            FROM 
                product_sales 
            WHERE 
                sale_id NOT IN (
                    SELECT DISTINCT sale_id 
                    FROM product_sales_sync_status 
                    WHERE is_synced = 1
                )
            """
            
            # Check if the sync status table exists, if not create it
            self.ensure_sync_table_exists()
            
            # Get unsynchronized records
            records = self.execute_query(query)
            return records
        else:
            print("This method is only for branch databases")
            return []
    
    def ensure_sync_table_exists(self):
        """Create sync status table if it doesn't exist"""
        if self.db_type in ['branch1', 'branch2']:
            check_query = """
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = 'product_sales_sync_status'
            """
            
            result = self.execute_query(check_query, (self.config['database'],))
            
            if result[0]['count'] == 0:
                create_query = """
                CREATE TABLE product_sales_sync_status (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    sale_id INT NOT NULL,
                    is_synced BOOLEAN DEFAULT FALSE,
                    sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (sale_id) REFERENCES product_sales(sale_id)
                )
                """
                self.execute_query(create_query, commit=True)
    
    def mark_as_synced(self, sale_id):
        """Mark a record as synchronized"""
        if self.db_type in ['branch1', 'branch2']:
            # Ensure sync table exists
            self.ensure_sync_table_exists()
            
            # Check if record exists in sync table
            check_query = """
            SELECT COUNT(*) as count 
            FROM product_sales_sync_status 
            WHERE sale_id = %s
            """
            
            result = self.execute_query(check_query, (sale_id,))
            
            if result[0]['count'] == 0:
                # Insert new record
                insert_query = """
                INSERT INTO product_sales_sync_status 
                (sale_id, is_synced) 
                VALUES (%s, TRUE)
                """
                self.execute_query(insert_query, (sale_id,), commit=True)
            else:
                # Update existing record
                update_query = """
                UPDATE product_sales_sync_status 
                SET is_synced = TRUE, 
                    sync_time = CURRENT_TIMESTAMP 
                WHERE sale_id = %s
                """
                self.execute_query(update_query, (sale_id,), commit=True)
            
            return True
        else:
            print("This method is only for branch databases")
            return False
    
    def add_sale_to_head_office(self, sale_data, source_branch):
        """Add a new sale record to the head office database"""
        if self.db_type == 'head_office':
            insert_query = """
            INSERT INTO product_sales 
            (date, region, product, qty, cost, amt, tax, total, source_branch) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                sale_data['date'],
                sale_data['region'],
                sale_data['product'],
                sale_data['qty'],
                sale_data['cost'],
                sale_data['amt'],
                sale_data['tax'],
                sale_data['total'],
                source_branch
            )
            
            self.execute_query(insert_query, params, commit=True)
            return True
        else:
            print("This method is only for head office database")
            return False
    
    def get_all_sales(self):
        """Get all sales records from a database"""
        query = "SELECT * FROM product_sales"
        return self.execute_query(query)
    
    def get_sales_summary(self):
        """Get sales summary from the database"""
        query = "SELECT * FROM sales_summary"
        return self.execute_query(query)
            
    def add_new_sale(self, sale_data):
        """Add a new sale record to a branch database"""
        if self.db_type in ['branch1', 'branch2']:
            insert_query = """
            INSERT INTO product_sales 
            (date, region, product, qty, cost, amt, tax, total) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                sale_data['date'],
                sale_data['region'],
                sale_data['product'],
                sale_data['qty'],
                sale_data['cost'],
                sale_data['amt'],
                sale_data['tax'],
                sale_data['total']
            )
            
            self.execute_query(insert_query, params, commit=True)
            return True
        else:
            print("This method is only for branch databases")
            return False