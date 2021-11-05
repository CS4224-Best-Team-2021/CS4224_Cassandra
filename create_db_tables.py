from schema import TABLES
from cassandra import ConsistencyLevel
from cassandra.cqlengine.management import create_keyspace_simple, sync_table
from cassandra.cqlengine import connection

KEYSPACES = ['cs4224_keyspace']

# Specify the IP address (and, optionally, port number) of each host to be used in the connection.
# If only one node is used, set CONSISTENCY_LEVEL to ONE. Otherwise, set CONSISTENCY_LEVEL to LOCAL_QUORUM.

# Connect to localhost
IP_ADDRESSES = ['127.0.0.1']
CONSISTENCY_LEVEL = ConsistencyLevel.ONE

# Connect to seed nodes xcnc20 and xcnc21
# IP_ADDRESSES = [('192.168.48.169', 6042), ('192.168.48.170', 6042)]
# CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM

def main():
    # create cluster
    connection.register_connection('default', IP_ADDRESSES, cluster_options={'control_connection_timeout': 120})
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
