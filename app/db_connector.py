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
                print(f"Connected to {self.db_type} database")
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
                if not self.connect():
                    print(f"Failed to connect to {self.db_type} database")
                    return None
                
            self.cursor.execute(query, params or ())
            
            if commit:
                self.connection.commit()
                print(f"Committed changes to {self.db_type} database")
                return True
            
            result = self.cursor.fetchall()
            return result
            
        except Error as e:
            print(f"Error executing query in {self.db_type}: {e}")
            print(f"Query: {query}")
            print(f"Params: {params}")
            if self.connection and self.connection.is_connected():
                self.connection.rollback()
            return None
    
    def get_all_sales_for_sync(self):
        """Get all sales records from branch for syncing to head office"""
        if self.db_type in ['branch1', 'branch2']:
            query = """
            SELECT 
                sale_id, date, region, product, qty, cost, amt, tax, total
            FROM 
                product_sales 
            """
            
            # Get records
            records = self.execute_query(query)
            return records
        else:
            print("This method is only for branch databases")
            return []
    
    def check_for_unsynced_sales(self):
        """
        Check if there are sales in the branch that might not be synced to head office
        """
        if self.db_type in ['branch1', 'branch2']:
            query = """
            SELECT COUNT(*) as count
            FROM product_sales
            """
            
            result = self.execute_query(query)
            return result[0]['count'] > 0 if result else False
        else:
            print("This method is only for branch databases")
            return False
            
    def add_sale_to_head_office(self, sale_data, source_branch):
        """Add a new sale record to the head office database."""
        if self.db_type == 'head_office':
            try:
                # First check if this sale_id from this branch already exists
                check_query = """
                SELECT COUNT(*) as count
                FROM product_sales
                WHERE original_sale_id = %s AND source_branch = %s
                """

                check_result = self.execute_query(check_query, (sale_data['sale_id'], source_branch))

                # If record already exists, skip insertion
                if check_result and check_result[0]['count'] > 0:
                    print(f"Sale {sale_data['sale_id']} from {source_branch} already exists in head office.")
                    return True

                # Debugging
                print(f"Inserting sale: {sale_data}")

                insert_query = """
                INSERT INTO product_sales 
                (original_sale_id, source_branch, date, region, product, qty, cost, amt, tax, total) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                params = (
                    sale_data['sale_id'],  # Store original sale_id
                    source_branch,
                    sale_data['date'],
                    sale_data['region'],
                    sale_data['product'],
                    sale_data['qty'],
                    sale_data['cost'],
                    sale_data['amt'],
                    sale_data['tax'],
                    sale_data['total']
                )

                result = self.execute_query(insert_query, params, commit=True)
                if result:
                    print(f"Successfully added sale {sale_data['sale_id']} from {source_branch} to head office")
                    return True
                else:
                    print(f"Failed to add sale {sale_data['sale_id']} from {source_branch} to head office")
                    return False

            except Exception as e:
                print(f"Error in add_sale_to_head_office: {e}")
                return False
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
            
            # Get the sale_id of the newly inserted record
            get_last_id_query = "SELECT LAST_INSERT_ID() as last_id"
            result = self.execute_query(get_last_id_query)
            
            if result and 'last_id' in result[0]:
                return result[0]['last_id']
            return None
        else:
            print("This method is only for branch databases")
            return None