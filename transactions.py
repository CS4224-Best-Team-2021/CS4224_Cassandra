from create_db_tables import IP_ADDRESSES, KEYSPACES
from schema import *

import sys

from cassandra import ConsistencyLevel
from cassandra.cqlengine import connection

# Deprecated. Use driver instead.
"""
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
"""

READ_CONSISTENCY_LEVEL = ConsistencyLevel.ONE
WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.ALL

# Note: We will need to pass in parameters to most of these functions
# Write-heavy transactions
def new_order_transaction(w_id, d_id, c_id, num_items, item_number, supplier_warehouse, quantity):
    pass

def payment_transaction(c_w_id, c_d_id, c_id, payment):
    pass

def delivery_transaction(w_id, carrier_id):
    pass

def order_status_transaction(c_w_id, c_d_id, c_id):
    pass


# Read-heavy transactions
def stock_level_transaction(w_id, d_id, threshold, num_last_orders):
    num_below_threshold = 0
    N = District.filter(D_W_ID=w_id, D_ID=d_id).consistency(READ_CONSISTENCY_LEVEL).get().D_NEXT_O_ID
    order_lines = OrderLine.filter(OL_W_ID=w_id, OL_D_ID=d_id, OL_O_ID__gte=(N-num_last_orders)).consistency(READ_CONSISTENCY_LEVEL)
    for order_line in order_lines:
        quantity = Stock.filter(S_W_ID=w_id, S_I_ID=order_line.OL_I_ID).consistency(READ_CONSISTENCY_LEVEL).get().S_QUANTITY
        if quantity < threshold:
            num_below_threshold += 1
    print(f"Total number of items with stock quantity at W_ID {w_id} below the threshold: {num_below_threshold}")

def popular_item_transaction(w_id, d_id, num_last_orders):
    N = District.filter(D_W_ID=w_id, D_ID=d_id).consistency(READ_CONSISTENCY_LEVEL).get().D_NEXT_O_ID
    orders = CustomerOrder.filter(O_W_ID=w_id, O_D_ID=d_id, O_ID__gte(N-num_last_orders)).consistency(READ_CONSISTENCY_LEVEL)
    S = orders.values_list('O_ID', flat=True)
    items = {}
    popular_items = {}
    max_quantity = 0
    for x in S:
        popular_items[x] = []
        items[x] = OrderLine.filter(OL_W_ID=w_id, OL_D_ID=d_id, OL_O_ID=x)
        for i in items[x]:
            curr_quantity = i.OL_QUANTITY
            if curr_quantity < max_quantity:
                continue
            if curr_quantity > max_quantity:
                popular_items[x] = []
                max_quantity = curr_quantity
            popular_items[x].append(i)
    print(f"District identifier (W_ID, D_ID): {w_id}, {d_id}")
    print(f"Number of last orders to be examined L = {num_last_orders}")
    distinct_items = {}
    for x in S:
        order_x = CustomerOrder.filter(O_W_ID=w_id, O_D_ID=d_id, O_ID=x).get()
        print(f"Order number {x} at {order_x.O_ENTRY_D}")
        customer_x = Customer.filter(C_W_ID=w_id, C_D_ID=d_id, C_ID=order_x.O_C_ID).consistency(READ_CONSISTENCY_LEVEL).get()
        print(f"Customer name: {customer_x.C_FIRST}, {customer_x.C_MIDDLE}, {customer_x.C_LAST}")
        print("Popular items ordered <item name | quantity ordered>:")
        for p in popular_items[x]:
            item_id = p.OL_I_ID
            item = Item.filter(I_ID=item_id).get()
            item_name = item.I_NAME
            print(f"{item_name} | {p.OL_QUANTITY}")
            if item_id not in distinct_items:
                distinct_items[item_id] = {}
                distinct_items[item_id]['name'] = item_name
                distinct_items[item_id]['count'] = 0
            distinct_items[item_id]['count'] += 1
    print("Percentage of examined orders containing each popular item")
    print("<item name> | <% of orders in S containing the item>")
    total_num_orders = orders.count()
    for iid in distinct_items:
        print(f"{distinct_items[iid]['name']} | {distinct_items[iid]['count'] / total_num_orders * 100}")


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

# Attempt 1 - encountered issues with allow_filtering and IN filter
# maybe consider building a secondary index on OL_I_ID?
def related_customer_transaction(c_w_id, c_d_id, c_id):
    c_orders = CustomerOrder.allow_filtering().filter(O_W_ID=c_w_id, O_D_ID=c_d_id, O_C_ID=c_id).consistency(READ_CONSISTENCY_LEVEL)
    o_ids = list(c_orders.values_list('O_ID', flat=True))
    item_sets = {}
    for oid in o_ids:
        order_line = OrderLine.filter(OL_W_ID=c_w_id, OL_D_ID=c_d_id, OL_O_ID=oid).consistency(READ_CONSISTENCY_LEVEL)
        item_sets[oid] = set(order_line.values_list('OL_I_ID', flat=True))

    w_other = set(Warehouse.values_list('W_ID', flat=True))
    w_other.remove(c_w_id)
    c_other = list(Customer.filter(C_W_ID__in=list(w_other)).consistency(READ_CONSISTENCY_LEVEL).values_list(['C_W_ID', 'C_D_ID', 'C_ID']))
    neighbours = set()
    for wid_other, did_other, cid_other in c_other:
        o_other = CustomerOrder.allow_filtering().filter(O_W_ID=wid_other, O_D_ID=did_other, O_C_ID=cid_other).consistency(READ_CONSISTENCY_LEVEL)
        oid_other = list(o_other.values_list('O_ID', flat=True))
        for oid in oid_other:
            ol_other = OrderLine.filter(OL_W_ID=c_w_id, OL_D_ID=c_d_id, OL_O_ID=oid).consistency(READ_CONSISTENCY_LEVEL)
            items_other = set(ol_other.values_list('OL_I_ID', flat=True))
            for item_set in item_sets.values():
                if len(items_other.intersection(item_set)) >= 2:
                    neighbours.add((wid_other, did_other, cid_other))
                    break
            else:
                break

    print("Customer <C_W_ID>, <C_D_ID>, <C_ID>")
    print(f"{c_w_id}, {c_d_id}, {c_id}")
    print("Related customers <C_W_ID>, <C_D_ID>, <C_ID>")
    for wid, did, cid in neighbours:
        print(f"{wid}, {did}, {cid}")

# Attempt 2 - Runs successfully but takes too long (iterating through effectively 90% of the entries in Order-Line)
def related_customer_transaction(c_w_id, c_d_id, c_id):
    start_time = time.time()
    c_orders = CustomerOrder.objects().allow_filtering().filter(O_W_ID=c_w_id, O_D_ID=c_d_id, O_C_ID=c_id).consistency(READ_CONSISTENCY_LEVEL)
    o_ids = c_orders.limit()values_list('O_ID', flat=True)
    item_sets = {}
    for oid in o_ids:
        order_line = OrderLine.filter(OL_W_ID=c_w_id, OL_D_ID=c_d_id, OL_O_ID=oid).consistency(READ_CONSISTENCY_LEVEL)
        item_sets[oid] = set(order_line.values_list('OL_I_ID', flat=True))
    # print(f"Obtained item-sets. Time elapsed: {time.time()-start_time} seconds")
    c_other = list(filter(lambda x: x[0] != c_w_id, list(Customer.all().limit(300000).values_list('C_W_ID', 'C_D_ID', 'C_ID'))))
    # print(f"Obtained list of customers (size {len(c_other)}). Time elapsed: {time.time()-start_time} seconds")
    neighbours = set()
    # count = 0
    for wid_other, did_other, cid_other in c_other:
        # if count % 100 == 99:
        #     print(f"{count} iterations completed. Time elapsed: {time.time()-start_time} seconds")
        o_other = CustomerOrder.objects().allow_filtering().filter(O_W_ID=wid_other, O_D_ID=did_other, O_C_ID=cid_other).consistency(READ_CONSISTENCY_LEVEL)
        oid_other = list(o_other.values_list('O_ID', flat=True))
        for oid in oid_other:
            ol_other = OrderLine.filter(OL_W_ID=c_w_id, OL_D_ID=c_d_id, OL_O_ID=oid).consistency(READ_CONSISTENCY_LEVEL)
            items_other = set(ol_other.values_list('OL_I_ID', flat=True))
            for item_set in item_sets.values():
                if len(items_other.intersection(item_set)) >= 2:
                    neighbours.add((wid_other, did_other, cid_other))
                    break
            else:
                break
        # count += 1

    print("Customer <C_W_ID>, <C_D_ID>, <C_ID>")
    print(f"{c_w_id}, {c_d_id}, {c_id}")
    print("Related customers <C_W_ID>, <C_D_ID>, <C_ID>")
    for wid, did, cid in neighbours:
        print(f"{wid}, {did}, {cid}")

"""
if __name__ == "__main__":
    main()
"""
