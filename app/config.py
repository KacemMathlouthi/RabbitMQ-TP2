# Database configuration
DB_CONFIG = {
    'head_office': {
        'host': 'HeadOfficeDB',
        'port': 3306,
        'user': 'user',
        'password': 'password',
        'database': 'head_office_db'
    },
    'branch1': {
        'host': 'BranchOneDB',
        'port': 3306,
        'user': 'user',
        'password': 'password',
        'database': 'branch1_db'
    },
    'branch2': {
        'host': 'BranchTwoDB',
        'port': 3306,
        'user': 'user',
        'password': 'password',
        'database': 'branch2_db'
    }
}

# RabbitMQ configuration
RABBITMQ_CONFIG = {
    'host': 'rabbitmq',
    'port': 5672,
    'username': 'guest',
    'password': 'guest',
    'exchange': 'sales_exchange',
    'exchange_type': 'direct',
    'queues': {
        'branch1_queue': 'branch1',
        'branch2_queue': 'branch2'
    }
}

# Sync interval in seconds
SYNC_INTERVAL = 60  # 1 minute