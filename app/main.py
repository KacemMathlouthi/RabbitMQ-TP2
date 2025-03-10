import gradio as gr
import pandas as pd
from datetime import datetime, date
import threading
import time
import schedule
from db_connector import DatabaseConnector
from producer import SalesProducer
from consumer import SalesConsumer
from config import SYNC_INTERVAL

class SalesSyncApp:
    def __init__(self):
        """Initialize the sales synchronization application"""
        self.head_office_db = DatabaseConnector('head_office')
        self.branch1_db = DatabaseConnector('branch1')
        self.branch2_db = DatabaseConnector('branch2')
        
        self.branch1_producer = SalesProducer('branch1')
        self.branch2_producer = SalesProducer('branch2')
        
        self.consumer = SalesConsumer()
        self.consumer_thread = None
        
        self.scheduler_thread = None
        self.scheduler_running = False
        
        # Flags to track pending changes
        self.branch1_has_changes = False
        self.branch2_has_changes = False
    
    def start_consumer(self):
        """Start the consumer thread to process messages"""
        if not self.consumer.is_consuming:
            success = self.consumer.start_consuming()
            return "Consumer started successfully" if success else "Failed to start consumer"
        return "Consumer is already running"
    
    def stop_consumer(self):
        """Stop the consumer thread"""
        if self.consumer.is_consuming:
            self.consumer.stop_consuming()
            return "Consumer stopped"
        return "Consumer is not running"
    
    def check_for_changes(self):
        """Check if there are changes to be synced in any branch"""
        self.branch1_has_changes = self.branch1_producer.check_for_changes()
        self.branch2_has_changes = self.branch2_producer.check_for_changes()
        
        message = []
        if self.branch1_has_changes:
            message.append("Branch 1 has sales that need to be synced")
        if self.branch2_has_changes:
            message.append("Branch 2 has sales that need to be synced")
            
        if not message:
            message.append("No changes detected in any branch")
            
        return "\n".join(message), self.branch1_has_changes, self.branch2_has_changes
    
    def sync_branch1(self):
        """Synchronize sales from Branch 1"""
        count = self.branch1_producer.sync_all_sales()
        self.branch1_has_changes = False
        return f"Synchronized {count} sales from Branch 1", False
    
    def sync_branch2(self):
        """Synchronize sales from Branch 2"""
        count = self.branch2_producer.sync_all_sales()
        self.branch2_has_changes = False
        return f"Synchronized {count} sales from Branch 2", False
    
    def sync_all_branches(self):
        """Synchronize sales from all branches"""
        count1 = self.branch1_producer.sync_all_sales()
        count2 = self.branch2_producer.sync_all_sales()
        self.branch1_has_changes = False
        self.branch2_has_changes = False
        return f"Synchronized {count1} sales from Branch 1 and {count2} sales from Branch 2"
    
    def start_auto_sync(self):
        """Start automatic synchronization at regular intervals"""
        if self.scheduler_running:
            return "Auto sync is already running"
        
        def run_schedule():
            self.scheduler_running = True
            
            # Define the sync job
            def sync_job():
                print(f"Running scheduled sync at {datetime.now()}")
                self.sync_all_branches()
            
            # Schedule the job
            schedule.every(SYNC_INTERVAL).seconds.do(sync_job)
            
            # Start the consumer if not running
            if not self.consumer.is_consuming:
                self.consumer.start_consuming()
            
            # Run the scheduler
            while self.scheduler_running:
                schedule.run_pending()
                time.sleep(1)
        
        # Start the scheduler in a separate thread
        self.scheduler_thread = threading.Thread(target=run_schedule)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        return f"Auto sync started. Will sync every {SYNC_INTERVAL} seconds"
    
    def stop_auto_sync(self):
        """Stop automatic synchronization"""
        if not self.scheduler_running:
            return "Auto sync is not running"
        
        # Stop the scheduler
        self.scheduler_running = False
        
        # Wait for the thread to complete
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=2)
        
        # Clear all scheduled jobs
        schedule.clear()
        
        return "Auto sync stopped"
    
    def get_branch1_sales(self):
        """Get sales data from Branch 1"""
        self.branch1_db.connect()
        sales = self.branch1_db.get_sales_summary()
        self.branch1_db.disconnect()
        
        if not sales:
            return pd.DataFrame()
        
        return pd.DataFrame(sales)
    
    def get_branch2_sales(self):
        """Get sales data from Branch 2"""
        self.branch2_db.connect()
        sales = self.branch2_db.get_sales_summary()
        self.branch2_db.disconnect()
        
        if not sales:
            return pd.DataFrame()
        
        return pd.DataFrame(sales)
    
    def get_head_office_sales(self):
        """Get sales data from Head Office"""
        self.head_office_db.connect()
        sales = self.head_office_db.get_sales_summary()
        self.head_office_db.disconnect()
        
        if not sales:
            return pd.DataFrame()
        
        return pd.DataFrame(sales)
    
    def add_new_sale_to_branch1(self, date_str, region, product, qty, cost, amt, tax, total):
        """Add a new sale to Branch 1"""
        try:
            # Parse inputs
            sale_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            qty = int(qty)
            cost = float(cost)
            amt = float(amt)
            tax = float(tax)
            total = float(total)
            
            # Create sale data
            sale_data = {
                'date': sale_date,
                'region': region,
                'product': product,
                'qty': qty,
                'cost': cost,
                'amt': amt,
                'tax': tax,
                'total': total
            }
            
            # Add and sync the sale
            success = self.branch1_producer.add_and_sync_new_sale(sale_data)
            
            if success:
                return "Sale added to Branch 1 and synchronized to Head Office"
            else:
                return "Failed to add sale"
                
        except Exception as e:
            return f"Error adding sale: {str(e)}"
    
    def add_new_sale_to_branch2(self, date_str, region, product, qty, cost, amt, tax, total):
        """Add a new sale to Branch 2"""
        try:
            # Parse inputs
            sale_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            qty = int(qty)
            cost = float(cost)
            amt = float(amt)
            tax = float(tax)
            total = float(total)
            
            # Create sale data
            sale_data = {
                'date': sale_date,
                'region': region,
                'product': product,
                'qty': qty,
                'cost': cost,
                'amt': amt,
                'tax': tax,
                'total': total
            }
            
            # Add and sync the sale
            success = self.branch2_producer.add_and_sync_new_sale(sale_data)
            
            if success:
                return "Sale added to Branch 2 and synchronized to Head Office"
            else:
                return "Failed to add sale"
                
        except Exception as e:
            return f"Error adding sale: {str(e)}"
    
    def launch_ui(self):
        """Launch the Gradio UI"""
        with gr.Blocks(title="Distributed Database Synchronization") as app:
            gr.Markdown("# Distributed Database Synchronization with RabbitMQ")
            
            with gr.Tabs():
                with gr.TabItem("Dashboard"):
                    with gr.Row():
                        with gr.Column():
                            gr.Markdown("### Synchronization Controls")
                            start_consumer_btn = gr.Button("Start Consumer")
                            stop_consumer_btn = gr.Button("Stop Consumer")
                            
                            check_changes_btn = gr.Button("Check for Changes")
                            changes_status = gr.Textbox(label="Changes Status", lines=2)
                            
                            with gr.Row():
                                # These buttons will be conditionally shown based on changes status
                                sync_branch1_btn = gr.Button("Sync Branch 1", visible=False)
                                sync_branch2_btn = gr.Button("Sync Branch 2", visible=False)
                            
                            sync_all_btn = gr.Button("Sync All Branches")
                            start_auto_btn = gr.Button("Start Auto Sync (60s)")
                            stop_auto_btn = gr.Button("Stop Auto Sync")
                            status_output = gr.Textbox(label="Status", lines=1)
                        
                    gr.Markdown("### Branch 1 Sales")
                    branch1_df = gr.DataFrame(self.get_branch1_sales())
                    refresh_branch1_btn = gr.Button("Refresh Branch 1 Data")
                    
                    gr.Markdown("### Branch 2 Sales")
                    branch2_df = gr.DataFrame(self.get_branch2_sales())
                    refresh_branch2_btn = gr.Button("Refresh Branch 2 Data")
                    
                    gr.Markdown("### Head Office Sales")
                    head_office_df = gr.DataFrame(self.get_head_office_sales())
                    refresh_ho_btn = gr.Button("Refresh Head Office Data")
                
                with gr.TabItem("Add New Sales"):
                    with gr.Row():
                        with gr.Column():
                            gr.Markdown("### Add Sale to Branch 1")
                            date_input1 = gr.Textbox(label="Date (YYYY-MM-DD)", value=date.today().isoformat())
                            region_input1 = gr.Dropdown(label="Region", choices=["East", "North-East"], value="East")
                            product_input1 = gr.Dropdown(
                                label="Product", 
                                choices=["Paper", "Pens", "Notebooks", "Desk Lamps", "Chairs"], 
                                value="Paper"
                            )
                            qty_input1 = gr.Number(label="Quantity", value=10)
                            cost_input1 = gr.Number(label="Cost", value=12.05)
                            amt_input1 = gr.Number(label="Amount", value=120.50)
                            tax_input1 = gr.Number(label="Tax", value=8.44)
                            total_input1 = gr.Number(label="Total", value=128.94)
                            add_branch1_btn = gr.Button("Add Sale to Branch 1")
                            status_branch1 = gr.Textbox(label="Status", lines=1)
                        
                        with gr.Column():
                            gr.Markdown("### Add Sale to Branch 2")
                            date_input2 = gr.Textbox(label="Date (YYYY-MM-DD)", value=date.today().isoformat())
                            region_input2 = gr.Dropdown(label="Region", choices=["West", "South-West"], value="West")
                            product_input2 = gr.Dropdown(
                                label="Product", 
                                choices=["Paper", "Pens", "Notebooks", "Desk Lamps", "Chairs"], 
                                value="Pens"
                            )
                            qty_input2 = gr.Number(label="Quantity", value=20)
                            cost_input2 = gr.Number(label="Cost", value=2.19)
                            amt_input2 = gr.Number(label="Amount", value=43.80)
                            tax_input2 = gr.Number(label="Tax", value=3.07)
                            total_input2 = gr.Number(label="Total", value=46.87)
                            add_branch2_btn = gr.Button("Add Sale to Branch 2")
                            status_branch2 = gr.Textbox(label="Status", lines=1)
            
            # Event handlers
            start_consumer_btn.click(self.start_consumer, inputs=[], outputs=[status_output])
            stop_consumer_btn.click(self.stop_consumer, inputs=[], outputs=[status_output])
            
            # Check for changes and show/hide sync buttons accordingly
            check_changes_btn.click(
                self.check_for_changes, 
                inputs=[], 
                outputs=[changes_status, sync_branch1_btn, sync_branch2_btn]
            )
            
            # Sync buttons with visibility updates
            sync_branch1_btn.click(
                self.sync_branch1,
                inputs=[],
                outputs=[status_output, sync_branch1_btn]
            )
            
            sync_branch2_btn.click(
                self.sync_branch2,
                inputs=[],
                outputs=[status_output, sync_branch2_btn]
            )
            
            sync_all_btn.click(self.sync_all_branches, inputs=[], outputs=[status_output])
            start_auto_btn.click(self.start_auto_sync, inputs=[], outputs=[status_output])
            stop_auto_btn.click(self.stop_auto_sync, inputs=[], outputs=[status_output])
            
            # Refresh data buttons
            refresh_branch1_btn.click(
                self.get_branch1_sales, 
                inputs=[], 
                outputs=[branch1_df]
            )
            
            refresh_branch2_btn.click(
                self.get_branch2_sales, 
                inputs=[], 
                outputs=[branch2_df]
            )
            
            refresh_ho_btn.click(
                self.get_head_office_sales, 
                inputs=[], 
                outputs=[head_office_df]
            )
            
            # Add sale buttons
            add_branch1_btn.click(
                self.add_new_sale_to_branch1, 
                inputs=[
                    date_input1, region_input1, product_input1, 
                    qty_input1, cost_input1, amt_input1, 
                    tax_input1, total_input1
                ], 
                outputs=[status_branch1]
            )
            
            add_branch2_btn.click(
                self.add_new_sale_to_branch2, 
                inputs=[
                    date_input2, region_input2, product_input2, 
                    qty_input2, cost_input2, amt_input2, 
                    tax_input2, total_input2
                ], 
                outputs=[status_branch2]
            )
            
        # Launch the app
        app.launch(server_name="0.0.0.0", server_port=7860, share=False)


if __name__ == "__main__":
    test = SalesSyncApp()
    test.launch_ui()