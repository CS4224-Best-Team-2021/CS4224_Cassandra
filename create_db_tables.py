from schema import TABLES
from cassandra.cqlengine.management import create_keyspace_simple, sync_table
from cassandra.cqlengine import connection

KEYSPACES = ['cs4224_keyspace']
IP_ADDRESSES = ['127.0.0.1']

def main():
    # create cluster
    connection.register_connection('default', IP_ADDRESSES)
    connections=['default']
    
    # create keyspace
    create_keyspace_simple(KEYSPACES[0], replication_factor=3, connections=connections)
    print(f'keyspace {KEYSPACES[0]} has been created')
    
    # create tables
    for table in TABLES:
        sync_table(table, KEYSPACES, connections)
        print(f'Table {table} has been created')

if __name__ == "__main__":
    main()