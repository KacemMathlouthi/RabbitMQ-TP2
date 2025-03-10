import pika
import json
import time
from datetime import datetime, date
from db_connector import DatabaseConnector
from config import RABBITMQ_CONFIG

class SalesProducer:
    def __init__(self, branch_name):
        """
        Initialize producer for a branch
        :param branch_name: 'branch1' or 'branch2'
        """
        self.branch_name = branch_name
        self.db = DatabaseConnector(branch_name)
        self.connection = None
        self.channel = None
        
    def connect_to_rabbitmq(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(
                RABBITMQ_CONFIG['username'],
                RABBITMQ_CONFIG['password']
            )
            
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_CONFIG['host'],
                port=RABBITMQ_CONFIG['port'],
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange
            self.channel.exchange_declare(
                exchange=RABBITMQ_CONFIG['exchange'],
                exchange_type=RABBITMQ_CONFIG['exchange_type'],
                durable=True
            )
            
            # Declare queue
            queue_name = RABBITMQ_CONFIG['queues'][f'{self.branch_name}_queue']
            self.channel.queue_declare(
                queue=queue_name,
                durable=True
            )
            
            # Bind queue to exchange
            self.channel.queue_bind(
                exchange=RABBITMQ_CONFIG['exchange'],
                queue=queue_name,
                routing_key=self.branch_name
            )
            
            print(f"Connected to RabbitMQ and set up '{self.branch_name}' queue")
            return True
            
        except Exception as e:
            print(f"Error connecting to RabbitMQ: {e}")
            return False
    
    def close_connection(self):
        """Close RabbitMQ connection"""
        if self.connection and self.connection.is_open:
            self.connection.close()
            print("RabbitMQ connection closed")
    
    def send_sale_data(self, sale_data):
        """
        Send a single sale record to RabbitMQ
        :param sale_data: Dictionary containing sale record data
        """
        if not self.connection or not self.connection.is_open:
            if not self.connect_to_rabbitmq():
                print("Failed to connect to RabbitMQ")
                return False
        
        try:
            queue_name = RABBITMQ_CONFIG['queues'][f'{self.branch_name}_queue']
            message = {
                'sale_id': sale_data['sale_id'],
                'date': sale_data['date'].isoformat() if isinstance(sale_data['date'], (datetime, date)) else sale_data['date'],
                'region': sale_data['region'],
                'product': sale_data['product'],
                'qty': sale_data['qty'],
                'cost': float(sale_data['cost']),
                'amt': float(sale_data['amt']),
                'tax': float(sale_data['tax']),
                'total': float(sale_data['total']),
                'branch': self.branch_name,
                'timestamp': datetime.now().isoformat()
            }
            
            # Convert message to JSON
            message_body = json.dumps(message)
            
            # Publish message
            self.channel.basic_publish(
                exchange=RABBITMQ_CONFIG['exchange'],
                routing_key=self.branch_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            print(f"Sent sale_id {sale_data['sale_id']} to queue")
            return True
            
        except Exception as e:
            print(f"Error sending message to RabbitMQ: {e}")
            return False
    
    def sync_all_sales(self):
        """Send all sales to RabbitMQ (full sync)"""
        # Connect to database
        self.db.connect()
        
        # Get all sales
        all_sales = self.db.get_all_sales_for_sync()
        
        if not all_sales:
            print(f"No sales found in {self.branch_name}")
            return 0
        
        # Connect to RabbitMQ
        if not self.connect_to_rabbitmq():
            print("Failed to connect to RabbitMQ")
            return 0
        
        # Send each sale to RabbitMQ
        success_count = 0
        for sale in all_sales:
            if self.send_sale_data(sale):
                success_count += 1
                
                # Small delay to prevent overloading
                time.sleep(0.1)
        
        # Close connections
        self.close_connection()
        self.db.disconnect()
        
        print(f"Synchronized {success_count} sales from {self.branch_name}")
        return success_count

    def add_and_sync_new_sale(self, sale_data):
        """Add a new sale to the branch database and sync it immediately"""
        # Connect to database
        self.db.connect()
        
        # Add new sale and get its ID
        new_sale_id = self.db.add_new_sale(sale_data)
        
        if not new_sale_id:
            print("Failed to add new sale")
            self.db.disconnect()
            return False
        
        # Get the newly added sale
        query = "SELECT * FROM product_sales WHERE sale_id = %s"
        result = self.db.execute_query(query, (new_sale_id,))
        
        if not result:
            print("Failed to retrieve the new sale")
            self.db.disconnect()
            return False
        
        new_sale = result[0]
        
        # Sync the new sale
        if not self.connect_to_rabbitmq():
            print("Failed to connect to RabbitMQ")
            self.db.disconnect()
            return False
        
        # Send the new sale
        if self.send_sale_data(new_sale):
            # Close connections
            self.close_connection()
            self.db.disconnect()
            
            print(f"Added and synchronized new sale from {self.branch_name}")
            return True
        
        # Close connections
        self.close_connection()
        self.db.disconnect()
        
        return False
        
    def check_for_changes(self):
        """Check if there are sales that need to be synced"""
        self.db.connect()
        has_changes = self.db.check_for_unsynced_sales()
        self.db.disconnect()
        return has_changes