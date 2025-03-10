# Distributed Database Synchronization System

This project implements a **Distributed Sales Synchronization System** using **RabbitMQ and MySQL** to synchronize sales data from multiple branch databases to a central head office database.

## **Project Overview**

The system consists of:
- **Branch Databases (branch1, branch2)**: Each branch stores sales data independently.
- **Head Office Database**: Aggregates all sales from branches.
- **RabbitMQ Broker**: Facilitates message-based communication between branches and the head office.
- **Producers (Branch Sync)**: Reads sales data from branch databases and publishes them to RabbitMQ.
- **Consumer (Head Office Sync)**: Listens to RabbitMQ and stores sales data in the head office database.
- **Web UI (Gradio-based Dashboard)**: Provides real-time monitoring and control of synchronization.

## **Project Structure**

```bash
.
├── Dockerfile
├── docker-compose.yml
├── app
│   ├── main.py
│   ├── config.py
│   ├── consumer.py
│   ├── db_connector.py
│   ├── producer.py
│   ├── requirements.txt
├── db_init
│   ├── branch1.sql
│   ├── branch2.sql
│   ├── head_office.sql
└── README.md
```

## **Installation & Setup**

### **1. Prerequisites**
- **Docker & Docker Compose** installed.
- **RabbitMQ** should be running as a container.
- **MySQL** installed for local testing (or use a MySQL container).

### **2. Clone the Repository**
```bash
git clone https://github.com/KacemMathlouthi/RabbitMQ-TP2.git
cd RabbitMQ-TP2
```

### **3. Run the Application**
```bash
docker-compose up --build
```
This will start **RabbitMQ, MySQL, and the application containers**.

## **Configuration**

All configurations are stored in **`app/config.py`**:

## **How It Works**

### **1. Producers (Branch Sync)**
- Each branch has a **producer** that reads sales data and sends it to RabbitMQ.
- Runs **automatically or manually** via the Gradio UI.

### **2. Consumer (Head Office Sync)**
- Listens to RabbitMQ queues and inserts sales into the head office database.
- Prevents duplicate sales using **`original_sale_id` and `source_branch`**.

### **3. UI Dashboard**
- Provides options to **start/stop consumers, sync branches manually, and monitor sales**.
- Built with **Gradio** for a user-friendly interface.
- Access via **http://localhost:7860** (or the configured port).

## **Future Enhancements**
- ✅ Implement **batch processing** for better performance.
- ✅ Add **asynchronous RabbitMQ handling**.
- ✅ Implement **retry mechanisms for failed syncs**.

## **License**
This project is licensed under the **MIT License**.
