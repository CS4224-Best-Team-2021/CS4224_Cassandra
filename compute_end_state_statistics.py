from create_db_tables import KEYSPACES, IP_ADDRESSES
from schema import *
from cassandra.cqlengine import connection

import csv

def main():
    connection.setup(IP_ADDRESSES, KEYSPACES[0])
    print('computing end state statistics for db...')
    
    # Get all the records needed
    warehouses = Warehouse.all()
    districts = District.all()
    customers = Customer.all().limit(300000)
    customer_orders = CustomerOrder.all().limit(600000)
    order_lines = OrderLine.all().limit(8000000)
    stocks = Stock.all().limit(1000000)

    # Compute end state statistics
    end_state_statistics = [0] * 15
    for warehouse in warehouses:
        end_state_statistics[0] += warehouse.W_YTD
    for district in districts:
        end_state_statistics[1] += district.D_YTD
        end_state_statistics[2] += district.D_NEXT_O_ID
    for customer in customers:
        end_state_statistics[3] += customer.C_BALANCE
        end_state_statistics[4] += customer.C_YTD_PAYMENT   
        end_state_statistics[5] += customer.C_PAYMENT_CNT
        end_state_statistics[6] += customer.C_DELIVERY_CNT
    for customer_order in customer_orders:
        max_OID_so_far = end_state_statistics[7] 
        end_state_statistics[7] = max(customer_order.O_ID, max_OID_so_far)
        end_state_statistics[8] += customer_order.O_OL_CNT 
    for order_line in order_lines:
        end_state_statistics[9] += order_line.OL_AMOUNT 
        end_state_statistics[10] += order_line.OL_QUANTITY
    for stock in stocks:
        end_state_statistics[11] += stock.S_QUANTITY 
        end_state_statistics[12] += stock.S_YTD
        end_state_statistics[13] += stock.S_ORDER_CNT 
        end_state_statistics[14] += stock.S_REMOTE_CNT
    
    with open('results/dbstate.csv', 'a') as f:
        for end_state_statistic in end_state_statistics:   
            f.write(f'{end_state_statistic}\n')
        print('end state statistics have been written to results/dbstate.csv')
        
    
if __name__ == "__main__":
    main()