from cassandra.cqlengine.management import create_keyspace_simple, sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

def main():
    KEYSPACES = ['cs4224_keyspace']
    IP_ADDRESSES = ['127.0.0.1']
    
    # create cluster
    connection.register_connection('default', IP_ADDRESSES)
    connections=['default']
    
    # create keyspace
    create_keyspace_simple(KEYSPACES[0], replication_factor=3, connections=connections)
    print(f'keyspace {KEYSPACES[0]} has been created')
    
    # create tables
    for table in [Warehouse, District, Customer, Order, Item, OrderLine, Stock]:
        sync_table(table, KEYSPACES, connections)
        print(f'Table {table} has been created')

class Warehouse(Model):
    W_ID = columns.Integer(primary_key=True)
    W_NAME = columns.Text(max_length=10)
    W_STREET_1 = columns.Text(max_length=20)
    W_STREET_2 = columns.Text(max_length=20)
    W_CITY = columns.Text(max_length=20)
    W_STATE = columns.Text(max_length=2)
    W_ZIP = columns.Text(max_length=9)
    W_TAX = columns.Decimal()
    W_YTD = columns.Decimal()

class District(Model):
    D_W_ID = columns.Integer(partition_key=True)
    D_ID = columns.Integer(partition_key=True)
    D_NAME = columns.Text(max_length=10)
    D_STREET_1 = columns.Text(max_length=20)
    D_STREET_2 = columns.Text(max_length=20)
    D_CITY = columns.Text(max_length=20)
    D_STATE = columns.Text(max_length=2)
    D_ZIP = columns.Text(max_length=9)
    D_TAX = columns.Decimal()
    D_YTD = columns.Decimal()
    
class Customer(Model):
    C_W_ID = columns.Integer(partition_key=True)
    C_D_ID = columns.Integer(partition_key=True)
    C_ID = columns.Integer(partition_key=True)
    

class Order(Model):
    O_W_ID = columns.Integer(partition_key=True)
    O_D_ID = columns.Integer(partition_key=True)
    O_ID = columns.Integer(partition_key=True)


class Item(Model):
    I_ID = columns.Integer(partition_key=True)

class OrderLine(Model):
    OL_W_ID = columns.Integer(partition_key=True)
    OL_D_DID = columns.Integer(partition_key=True)
    OLD_O_ID = columns.Integer(partition_key=True)
    OL_NUMBER = columns.Integer(partition_key=True)

class Stock(Model):
    S_W_ID = columns.Integer(partition_key=True)
    S_I_ID =  columns.Integer(partition_key=True)


if __name__ == "__main__":
    main()