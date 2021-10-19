from create_db_tables import KEYSPACES, IP_ADDRESSES
from cassandra.cqlengine import connection
from transactions import *

import time

PATH_A = 'project_files_4/xact_files_A'
PATH_B = 'project_files_4/xact_files_B'

def main():
    connection.setup(IP_ADDRESSES, KEYSPACES[0])

    transaction_times = []
    start_time = time.time()
    # for i in range(40):
    for i in range(1):
        execute_transaction(PATH_A, i)
        transaction_time = time.time() - start_time
        transaction_times.append(transaction_time)

    
def execute_transaction(transaction_folder_path, file_number):
    file_path = f'{transaction_folder_path}/{file_number}.txt'
    with open(file_path) as file:
        while True:
            line = file.readline()
            if not line:
                break
            
            line_parts = line.split(',')
            transaction_type = line_parts[0].strip()
            
            if transaction_type == "N":
                c_id,w_id,d_id, M = line_parts[1:]
                item_number = []
                supplier_warehouse = []
                quantity = []
                
                for _ in range(int(M)):
                    line = file.readline()
                    line_parts = line.split(',')
                    params = [eval(param) for param in line_parts]
                    item_number.append(params[0])
                    supplier_warehouse.append(params[1])
                    quantity.append(params[2])
                    
                new_order_transaction(int(c_id), int(w_id), int(d_id), item_number, supplier_warehouse, quantity)
            elif transaction_type == "P":
                c_w_id, c_d_id, c_id, payment = line_parts[1:]
                payment_transaction(int(c_w_id), int(c_d_id), int(c_id), float(payment))
            elif transaction_type == "D":
                w_id, carrier_id = line_parts[1:]
                delivery_transaction(int(w_id), int(carrier_id))
            elif transaction_type == "O":
                c_w_id, c_d_id, c_id = line_parts[1:]
                order_status_transaction(int(c_w_id), int(c_d_id), int(c_id))
            elif transaction_type == "S":
                w_id, d_id, T, L = line_parts[1:]
                stock_level_transaction(int(w_id), int(d_id), int(T), int(L))
            elif transaction_type == "I":
                w_id, d_id, L = line_parts[1:]
                popular_item_transaction(int(w_id), int(d_id), int(L))
            elif transaction_type == "T":
                top_balance_transaction()
            elif transaction_type == "R":
                c_w_id, c_d_id, c_id = line_parts[1:]
                related_customer_transaction(int(c_w_id), int(c_d_id), int(c_id))
            else:
                raise ValueError(f"Invalid transaction type specified. Error occured when parsing the following line: {line_parts}")
                
        

if __name__ == "__main__":
    main()