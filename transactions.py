from create_db_tables import IP_ADDRESSES, KEYSPACES
from schema import *

from cassandra import ConsistencyLevel
from cassandra.cqlengine import connection

def main():
    connection.setup(IP_ADDRESSES, KEYSPACES[0])
    # Test your functions using this script once it has been implemented
    top_balance_transaction()

READ_CONSISTENCY_LEVEL = ConsistencyLevel.ONE
WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.ALL

# Note: We will need to pass in parameters to most of these functions
# Write-heavy transactions
def new_order_transaction():
    pass

def payment_transaction():
    pass

def delivery_transaction():
    pass

def order_status_transaction():
    pass


# Read-heavy transactions
def stock_level_transaction():
    pass

def popular_item_transaction():
    pass

def top_balance_transaction():
    # Processing steps
    customers = Customer.all()
    sorted_customers = sorted(customers, key=lambda x: x.C_BALANCE, reverse=True)
    # Print output
    for customer in sorted_customers[:10]:
        print("Name of customer:", customer.C_FIRST, customer.C_MIDDLE, customer.C_LAST)
        print("Balance of customer's outstanding payment", customer.C_BALANCE)
        warehouse = Warehouse.filter(W_ID=customer.C_W_ID).consistency(READ_CONSISTENCY_LEVEL).get()
        district = District.filter(D_W_ID=customer.C_W_ID, D_ID=customer.C_D_ID).consistency(READ_CONSISTENCY_LEVEL).get()
        print('Warehouse name of customer', warehouse.W_NAME)
        print('District name of customer:', district.D_NAME)
    
def related_customer_transaction():
    pass


if __name__ == "__main__":
    main()
    