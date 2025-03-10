import pika
import json
import threading
from datetime import datetime
from db_connector import DatabaseConnector
from config import RABBITMQ_CONFIG

class SalesConsumer:
    def __init__(self):
        """Initialize consumer for the head office"""
        self.db = DatabaseConnector('head_office')
        self.connection = None
        self.channel = None
        self.threads = []
        self.is_consuming = False
        
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
            
            # Set up consumers for both branch queues
            for branch in ['branch1', 'branch2']:
                queue_name = RABBITMQ_CONFIG['queues'][f'{branch}_queue']
                
                # Declare queue
                self.channel.queue_declare(
                    queue=queue_name,
                    durable=True
                )
                
                # Bind queue to exchange
                self.channel.queue_bind(
                    exchange=RABBITMQ_CONFIG['exchange'],
                    queue=queue_name,
                    routing_key=branch
                )
                
                # Set QoS (quality of service)
                self.channel.basic_qos(prefetch_count=1)
                
                # Set up consumer with correct callback signature
                self.channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self.process_message,
                    auto_ack=False
                )
            
            print("Connected to RabbitMQ and ready to consume messages")
            return True
            
        except Exception as e:
            print(f"Error connecting to RabbitMQ: {e}")
            return False
    
    def close_connection(self):
        """Close RabbitMQ connection"""
        self.is_consuming = False
        if self.connection and self.connection.is_open:
            self.connection.close()
            print("RabbitMQ connection closed")
            
        # Wait for threads to complete
        for thread in self.threads:
            if thread.is_alive():
                thread.join()
    
    def process_message(self, ch, method, properties, body):
        """
        Process incoming messages from RabbitMQ
        :param ch: Channel
        :param method: Method
        :param properties: Properties
        :param body: Message body
        """
        try:
            # Parse message
            message = json.loads(body)
            print(f"Received message: {message}")
            
            # Connect to database
            self.db.connect()
            
            # Convert date string to date object if needed
            if isinstance(message['date'], str):
                try:
                    message['date'] = datetime.fromisoformat(message['date']).date()
                except ValueError:
                    # Handle ISO 8601 format with Z
                    if message['date'].endswith('Z'):
                        message['date'] = datetime.fromisoformat(message['date'][:-1]).date()
                    else:
                        # Try with different format
                        message['date'] = datetime.strptime(message['date'], "%Y-%m-%d").date()
            
            # Create sale data for head office
            sale_data = {
                'sale_id': message['sale_id'],  # Make sure to include sale_id
                'date': message['date'],
                'region': message['region'],
                'product': message['product'],
                'qty': message['qty'],
                'cost': message['cost'],
                'amt': message['amt'],
                'tax': message['tax'],
                'total': message['total']
            }
            
            # Add to head office database
            source_branch = message['branch']
            success = self.db.add_sale_to_head_office(sale_data, source_branch)
            
            if success:
                # Acknowledge message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f"Processed sale from {message['branch']}, Product: {message['product']}, Region: {message['region']}")
            else:
                # Reject message and requeue
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                print(f"Failed to process sale, requeueing: {message}")
            
            # Disconnect from database
            self.db.disconnect()
            
        except Exception as e:
            print(f"Error processing message: {e}")
            # Reject message but don't requeue if it's a parsing error
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def start_consuming(self):
        """Start consuming messages in a separate thread"""
        if not self.connect_to_rabbitmq():
            print("Failed to connect to RabbitMQ")
            return False
        
        def consume_thread():
            self.is_consuming = True
            try:
                print("Started consuming messages")
                self.channel.start_consuming()
            except Exception as e:
                print(f"Consuming stopped: {e}")
            finally:
                self.is_consuming = False
                print("Stopped consuming messages")
        
        # Start consuming in a separate thread
        thread = threading.Thread(target=consume_thread)
        thread.daemon = True
        thread.start()
        self.threads.append(thread)
        
        return True
    
    def stop_consuming(self):
        """Stop consuming messages"""
        if self.channel and self.is_consuming:
            self.channel.stop_consuming()
            self.is_consuming = False
            
        self.close_connection()
        print("Stopped consuming messages")