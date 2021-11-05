from schema import TABLES
from cassandra import ConsistencyLevel
from cassandra.cqlengine.management import create_keyspace_simple, sync_table
from cassandra.cqlengine import connection

KEYSPACES = ['cs4224_keyspace']
# Connect to localhost
IP_ADDRESSES = ['127.0.0.1']
# Connect to seed nodes xcnc20 and xcnc21
# IP_ADDRESSES = [('192.168.48.169', 6042), ('192.168.48.170', 6042)]
MESSAGE_REENTER_IP_ADDRESSES = "Please try again or enter 'exit' to exit."

def main():
    # create cluster
    global IP_ADDRESSES, CONSISTENCY_LEVEL
    while True:
        usr_in = input("Enter IP address(es) and port number(s) [IP_ADDRESS_1]:[port_1], [IP_ADDRESS_2]:[port_2], ...:\n")
        if len(usr_in) == 0:
            print(f"No input detected. {MESSAGE_REENTER_IP_ADDRESSES}")
            continue
        if usr_in.lower() == "exit":
            return
        args = list(map(lambda x: x.strip(), usr_in.split(",")))
        IP_ADDRESSES = []
        try:
            for arg in args:
                ip_address, port_str = arg.split(":")
                port = int(port_str)
                IP_ADDRESSES.append((ip_address, port))
        except ValueError as e:
            print(f"Invalid input: {usr_in}\n{MESSAGE_REENTER_IP_ADDRESSES}")
            continue
        if len(IP_ADDRESSES) == 1:
            CONSISTENCY_LEVEL = ConsistencyLevel.ONE
            break
        CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM
        break

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
