from create_db_tables import IP_ADDRESSES, KEYSPACES
from schema import *

import sys

from cassandra import ConsistencyLevel
from cassandra.cqlengine import connection

def main():
    connection.setup(IP_ADDRESSES, KEYSPACES[0])
    first_line = sys.stdin.readline().split(',')
    try:
        t_type = first_line[0].strip()
        if t_type == "N":
            type,c_id,w_id,d_id, M = first_line
            new_order_transaction(int(c_id), int(w_id), int(d_id), int(M))
        elif t_type == "P":
            type, c_w_id, c_d_id, c_id, payment = first_line
            payment_transaction(int(c_w_id), int(c_d_id), int(c_id), float(payment))
        elif t_type == "D":
            type, w_id, carrier_id = first_line
            delivery_transaction(int(w_id), int(carrier_id))
        elif t_type == "O":
            type, c_w_id, c_d_id, c_id = first_line
            order_status_transaction(int(c_w_id), int(c_d_id), int(c_id))
        elif t_type == "S":
            type, w_id, d_id, T, L = first_line
            stock_level_transaction(int(w_id), int(d_id), int(T), int(L))
        elif t_type == "I":
            type, w_id, d_id, L = first_line
            popular_item_transaction(int(w_id), int(d_id), int(L))
        elif t_type == "T":
            top_balance_transaction()
        elif t_type == "R":
            type, c_w_id, c_d_id, c_id = first_line
            related_customer_transaction(int(c_w_id), int(c_d_id), int(c_id))
        else:
            raise ValueError("Invalid transaction type specified.")
    except ValueError as e1:
        raise ValueError("Invalid transaction type specified.")
    except IndexError as e2:
        raise EOFError("No input detected.")


    # Test your functions using this script once it has been implemented
    # top_balance_transaction()

READ_CONSISTENCY_LEVEL = ConsistencyLevel.ONE
WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.ALL

# Note: We will need to pass in parameters to most of these functions
# Write-heavy transactions
def new_order_transaction(w_id, d_id, c_id, num_items):
    pass

def payment_transaction(c_w_id, c_d_id, c_id, payment):
    pass

def delivery_transaction(w_id, carrier_id):
    pass

def order_status_transaction(c_w_id, c_d_id, c_id):
    pass


# Read-heavy transactions
def stock_level_transaction(w_id, d_id, threshold, num_last_orders):
    pass

def popular_item_transaction(w_id, d_id, num_last_orders):
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

def related_customer_transaction(c_w_id, c_d_id, c_id):
    pass


if __name__ == "__main__":
    main()
