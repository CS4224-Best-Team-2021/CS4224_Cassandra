from create_db_tables import KEYSPACES, IP_ADDRESSES
from cassandra.cqlengine import connection
from transactions import *

import csv
import time
import sys

transaction_times = []

def main():
    connection.setup(IP_ADDRESSES, KEYSPACES[0])
    experiment_number = sys.argv[1]
    client_number = sys.argv[2]
        
    # TODO: Handle client numbers and file path more generally
    print('executing transactions...')
    execute_transaction(f'project_files_4/xact_files_{experiment_number}/{client_number}.txt')
    
    # Compute performance statistics
    print('computing performance statistics...')
    number_of_transactions_executed = len(transaction_times)
    total_transaction_execution_time = sum(transaction_times)
    transaction_throughput = number_of_transactions_executed / total_transaction_execution_time
    average_transaction_latency = 1 / transaction_throughput
    sorted_transaction_times = sorted(transaction_times)
    median_transaction_latency = sorted_transaction_times[number_of_transactions_executed // 2]
    percentile_95_transaction_latency = sorted_transaction_times[int(number_of_transactions_executed * 0.95)]
    percentile_99_transaction_latency = sorted_transaction_times[int(number_of_transactions_executed * 0.99)]
    
    with open(f'results/{client_number}.csv', 'w') as f:
        file_writer = csv.writer(f)
        file_writer.writerow([client_number,
                              number_of_transactions_executed,
                              total_transaction_execution_time,
                              average_transaction_latency,
                              median_transaction_latency,
                              percentile_95_transaction_latency,
                              percentile_99_transaction_latency])
    print('performance statistics have been written to results/{client_number}.csv')
    
def execute_transaction(file_path):
    with open(file_path) as file:
        while True:
            line = file.readline()
            if not line:
                break
            
            line_parts = line.split(',')
            transaction_type = line_parts[0].strip()
            
            start_time = time.time()
            
            if transaction_type == "N":
                c_id,w_id,d_id, M = line_parts[1:]
                num_items = int(M)
                item_number = []
                supplier_warehouse = []
                quantity = []
                
                for _ in range(num_items):
                    line = file.readline()
                    line_parts = line.split(',')
                    params = [eval(param) for param in line_parts]
                    item_number.append(params[0])
                    supplier_warehouse.append(params[1])
                    quantity.append(params[2])
                    
                new_order_transaction(int(c_id), int(w_id), int(d_id), num_items, item_number, supplier_warehouse, quantity)
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
            
            transaction_time = time.time() - start_time
            transaction_times.append(transaction_time)
        

if __name__ == "__main__":
    main()