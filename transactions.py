from create_db_tables import IP_ADDRESSES, KEYSPACES, CONSISTENCY_LEVEL
from schema import *

import sys
import time

from cassandra.cqlengine import connection

from datetime import datetime
import decimal

# Deprecated. Use driver instead.
def main():
    connection.setup(IP_ADDRESSES, KEYSPACES[0])
    # Test your functions using this script once it has been implemented
    top_balance_transaction()
    related_customer_transaction(1, 1, 1)

# Note: We will need to pass in parameters to most of these functions
# Write-heavy transactions
def new_order_transaction(c_id, w_id, d_id, num_items, item_number, supplier_warehouse, quantity):
    # print("New order transaction")
    D = District.filter(D_W_ID=w_id, D_ID=d_id).consistency(CONSISTENCY_LEVEL).get()
    W = Warehouse.filter(W_ID=w_id).consistency(CONSISTENCY_LEVEL).get()
    C = Customer.filter(C_W_ID=w_id, C_D_ID=d_id, C_ID=c_id).consistency(CONSISTENCY_LEVEL).get()
    print(f"Customer({C.C_W_ID}, {C.C_D_ID}, {C.C_ID}): {C.C_LAST}, {C.C_CREDIT}, {C.C_DISCOUNT}")
    print(f"Warehouse tax rate: {W.W_TAX}. Distract tax rate: {D.D_TAX}")
    N = D.D_NEXT_O_ID
    D.update(D_NEXT_O_ID=N+1).consistency(CONSISTENCY_LEVEL)
    local = 1
    TOTAL_AMOUNT = 0
    item_output_strings = []
    for i in range(num_items):
        if supplier_warehouse[i] != w_id:
            local = 0
        S = Stock.filter(S_W_ID=supplier_warehouse[i], S_I_ID=item_number[i]).consistency(CONSISTENCY_LEVEL).get()
        I = Item.filter(I_ID=item_number[i]).consistency(CONSISTENCY_LEVEL).get()
        S.QUANTITY = S.S_QUANTITY
        ADJUSTED_QTY = S.QUANTITY - quantity[i]
        if ADJUSTED_QTY < 10:
            ADJUSTED_QTY = ADJUSTED_QTY + 100
        s_remote_cnt = (S.S_REMOTE_CNT + 1) if supplier_warehouse[i] != w_id else (S.S_REMOTE_CNT)
        S.update(S_QUANTITY=ADJUSTED_QTY, S_YTD=S.S_YTD+quantity[i], S_ORDER_CNT=S.S_ORDER_CNT+1, S_REMOTE_CNT=s_remote_cnt).consistency(CONSISTENCY_LEVEL)
        ITEM_AMOUNT = quantity[i] * I.I_PRICE
        TOTAL_AMOUNT += ITEM_AMOUNT
        s_dis = "S_DIST_10" if d_id == 10 else "S_DIST_0" + str(d_id)
        OrderLine.create(OL_O_ID=N, OL_D_ID=d_id, OL_W_ID=w_id, OL_NUMBER=i+1, OL_I_ID=item_number[i], OL_SUPPLY_W_ID=supplier_warehouse[i], OL_QUANTITY=quantity[i], OL_AMOUNT=ITEM_AMOUNT,  OL_DIST_INFO=S[s_dis]).consistency(CONSISTENCY_LEVEL)
        item_output_strings.append(f"Item number: {item_number[i]}")
        item_output_strings.append(f"Item name: {I.I_NAME}")
        item_output_strings.append(f"Supplier warehouse: {supplier_warehouse[i]}")
        item_output_strings.append(f"Quantity: {quantity[i]}")
        item_output_strings.append(f"Order-line amount: {ITEM_AMOUNT}")
        item_output_strings.append(f"Stock quantity: {ADJUSTED_QTY}")
    order = CustomerOrder.create(O_ID=N, O_D_ID=d_id, O_W_ID=w_id, O_C_ID=c_id, O_ENTRY_D=datetime.now(), O_OL_CNT=num_items, O_ALL_LOCAL=local).consistency(CONSISTENCY_LEVEL)
    CustomerOrderByCID.create(O_ID=N, O_D_ID=d_id, O_W_ID=w_id, O_C_ID=c_id, O_ENTRY_D=order.O_ENTRY_D, O_OL_CNT=num_items, O_ALL_LOCAL=local).consistency(CONSISTENCY_LEVEL)
    TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + W.W_TAX + D.D_TAX) * (1 - C.C_DISCOUNT)
    print(f"Order number: {N}. Entry date: {order.O_ENTRY_D}")
    print(f"Number of Items: {num_items}. Total amount: {TOTAL_AMOUNT}")
    print("\n".join(item_output_strings))

def payment_transaction(c_w_id, c_d_id, c_id, payment):
    # print("Payment transaction")
    C = Customer.filter(C_W_ID=c_w_id, C_D_ID=c_d_id, C_ID=c_id).consistency(CONSISTENCY_LEVEL).get()
    W = Warehouse.filter(W_ID=c_w_id).consistency(CONSISTENCY_LEVEL).get()
    D = District.filter(D_W_ID=c_w_id, D_ID=c_d_id).consistency(CONSISTENCY_LEVEL).get()
    payment_d = decimal.Decimal(float(payment))
    W.update(W_YTD=W.W_YTD+payment_d).consistency(CONSISTENCY_LEVEL)
    D.update(D_YTD=D.D_YTD+payment_d).consistency(CONSISTENCY_LEVEL)
    C.update(C_BALANCE=C.C_BALANCE-payment_d, C_YTD_PAYMENT=C.C_YTD_PAYMENT+payment, C_PAYMENT_CNT=C.C_PAYMENT_CNT+1).consistency(CONSISTENCY_LEVEL)
    print(f"Customer({c_w_id}, {c_d_id}, {c_id}): name({C.C_FIRST} {C.C_MIDDLE} {C.C_LAST}), address ({C.C_STREET_1}, {C.C_STREET_2}, {C.C_CITY}, {C.C_STATE}, {C.C_ZIP}), {C.C_PHONE}, {C.C_SINCE}, {C.C_CREDIT}, {C.C_CREDIT_LIM}, {C.C_DISCOUNT}, {C.C_BALANCE}")
    print(f"Warehouse: {W.W_STREET_1}, {W.W_STREET_2}, {W.W_CITY}, {W.W_STATE}, {W.W_ZIP}")
    print(f"District: {D.D_STREET_1}, {D.D_STREET_2}, {D.D_CITY}, {D.D_STATE}, {D.D_ZIP}")
    print(f"Payment: {payment}")

def delivery_transaction(w_id, carrier_id):
    # print("Delivery transaction")
    for i in range(1, 11):
        X = CustomerOrder.filter(O_W_ID=w_id, O_D_ID=i, O_CARRIER_ID=-1).consistency(CONSISTENCY_LEVEL).allow_filtering().order_by('O_ID').first()
        if not X:
            continue
        Y = CustomerOrderByCID.filter(O_W_ID=w_id, O_D_ID=i, O_ID=X.O_ID).consistency(CONSISTENCY_LEVEL).get()
        C = Customer.filter(C_W_ID=X.O_W_ID, C_D_ID=X.O_D_ID, C_ID=X.O_C_ID).get()
        X.update(O_CARRIER_ID=carrier_id).consistency(CONSISTENCY_LEVEL)
        Y.update(O_CARRIER_ID=carrier_id).consistency(CONSISTENCY_LEVEL)
        ols = OrderLine.filter(OL_W_ID=X.O_W_ID, OL_D_ID=X.O_D_ID, OL_O_ID=X.O_ID).consistency(CONSISTENCY_LEVEL).all()
        date = datetime.now()
        B = 0
        for ol in ols:
            ol.update(OL_DELIVERY_D=date).consistency(CONSISTENCY_LEVEL)
            B += ol.OL_AMOUNT
        C.update(C_BALANCE=C.C_BALANCE+B, C_DELIVERY_CNT=C.C_DELIVERY_CNT+1).consistency(CONSISTENCY_LEVEL)

# Read-heavy transactions
def order_status_transaction(c_w_id, c_d_id, c_id):
    # print("Order status transaction")
    C = Customer.filter(C_W_ID=c_w_id, C_D_ID=c_d_id, C_ID=c_id).consistency(CONSISTENCY_LEVEL).get()
    print(f"Customer: {C.C_FIRST}, {C.C_MIDDLE}, {C.C_LAST}, {C.C_BALANCE}")
    O = CustomerOrder.filter(O_W_ID=c_w_id, O_D_ID=c_d_id, O_C_ID=c_id).consistency(CONSISTENCY_LEVEL).allow_filtering().order_by('O_ID')[-1]
    print(f"Order: {O.O_ID}, {O.O_ENTRY_D}, {O.O_CARRIER_ID}")
    ols = OrderLine.filter(OL_W_ID=c_w_id, OL_D_ID=c_d_id, OL_O_ID=O.O_ID).consistency(CONSISTENCY_LEVEL).all()
    for ol in ols:
        print(f"OrderLine: {ol.OL_I_ID}, {ol.OL_SUPPLY_W_ID}, {ol.OL_QUANTITY}, {ol.OL_AMOUNT}, {ol.OL_DELIVERY_D}")

def stock_level_transaction(w_id, d_id, threshold, num_last_orders):
    # Processing steps
    num_below_threshold = 0
    N = District.filter(D_W_ID=w_id, D_ID=d_id).consistency(CONSISTENCY_LEVEL).get().D_NEXT_O_ID
    order_lines = OrderLine.filter(OL_W_ID=w_id, OL_D_ID=d_id, OL_O_ID__gte=(N-num_last_orders)).consistency(CONSISTENCY_LEVEL)
    item_ids = set(order_lines.values_list('OL_I_ID', flat=True))
    for iid in item_ids:
        quantity = Stock.filter(S_W_ID=w_id, S_I_ID=iid).consistency(CONSISTENCY_LEVEL).get().S_QUANTITY
        if quantity < threshold:
            num_below_threshold += 1
    # Print output
    print(f"Total number of items with stock quantity at W_ID {w_id} below the threshold: {num_below_threshold}")

def popular_item_transaction(w_id, d_id, num_last_orders):
    # Processing steps
    N = District.filter(D_W_ID=w_id, D_ID=d_id).consistency(CONSISTENCY_LEVEL).get().D_NEXT_O_ID
    orders = CustomerOrder.filter(O_W_ID=w_id, O_D_ID=d_id, O_ID__gte=(N-num_last_orders)).consistency(CONSISTENCY_LEVEL)
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
    # Print output
    print(f"District identifier (W_ID, D_ID): {w_id}, {d_id}")
    print(f"Number of last orders to be examined L = {num_last_orders}")
    distinct_items = {}
    for x in S:
        order_x = CustomerOrder.filter(O_W_ID=w_id, O_D_ID=d_id, O_ID=x).get()
        print(f"Order number {x} at {order_x.O_ENTRY_D}")
        customer_x = Customer.filter(C_W_ID=w_id, C_D_ID=d_id, C_ID=order_x.O_C_ID).consistency(CONSISTENCY_LEVEL).get()
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
    customers = Customer.all().limit(300000)
    sorted_customers = sorted(customers, key=lambda x: x.C_BALANCE, reverse=True)
    # Print output
    for customer in sorted_customers[:10]:
        print("Name of customer:", customer.C_FIRST, customer.C_MIDDLE, customer.C_LAST)
        print("Balance of customer's outstanding payment", customer.C_BALANCE)
        warehouse = Warehouse.filter(W_ID=customer.C_W_ID).consistency(CONSISTENCY_LEVEL).get()
        district = District.filter(D_W_ID=customer.C_W_ID, D_ID=customer.C_D_ID).consistency(CONSISTENCY_LEVEL).get()
        print('Warehouse name of customer', warehouse.W_NAME)
        print('District name of customer:', district.D_NAME)

# NOTE: Run usr/bin/cqlsh --file create_index.cql first, or else this transaction will fail and require ALLOW FILTERING
def related_customer_transaction(c_w_id, c_d_id, c_id):
    # Processing steps
    c_orders = CustomerOrderByCID.filter(O_W_ID=c_w_id, O_D_ID=c_d_id, O_C_ID=c_id).consistency(CONSISTENCY_LEVEL)
    c_item_set_list = []
    for c_order in c_orders:
        c_item_set = set()
        c_order_lines = OrderLine.filter(OL_W_ID=c_w_id, OL_D_ID=c_d_id, OL_O_ID=c_order.O_ID).consistency(CONSISTENCY_LEVEL)
        for c_order_line in c_order_lines:
            c_item_set.add(c_order_line.OL_I_ID)
        c_item_set_list.append(c_item_set)

    neighbours = set()
    for item_set in c_item_set_list:
        scores = {}
        for item in item_set:
            related_orders = list(OrderLine.filter(OL_I_ID=item).consistency(CONSISTENCY_LEVEL).values_list('OL_W_ID', 'OL_D_ID', 'OL_O_ID').allow_filtering())
            for wid, did, oid in related_orders:
                if wid == c_w_id:
                    continue
                if (wid, did, oid) not in scores:
                    scores[(wid, did, oid)] = 0
                scores[(wid, did, oid)] += 1
        for identifier, score in scores.items():
            if score >= 2:
                wid, did, oid = identifier
                customer = CustomerOrderByCID.filter(O_W_ID=wid, O_D_ID=did, O_ID=oid).consistency(CONSISTENCY_LEVEL).get()
                neighbours.add((customer.O_W_ID, customer.O_D_ID, customer.O_C_ID))

    # Print output
    print("Customer <W_ID>, <D_ID>, <C_ID>")
    print(f"{c_w_id}, {c_d_id}, {c_id}")
    print("Related customers <W_ID>, <D_ID>, <C_ID>")
    for wid, did, oid in neighbours:
        print(f"{wid}, {did}, {oid}")

if __name__ == "__main__":
    main()
