import csv
from datetime import datetime

from schema import TABLES
from create_db_tables import KEYSPACES, IP_ADDRESSES

from cassandra.cqlengine import connection
from cassandra.cqlengine import columns


PATH_TO_DATA_FOLDER = 'project_files_4/data_files/'
DATA_FILES =  ['warehouse.csv', 'district.csv', 'customer.csv', 'order.csv', 'item.csv', 'order-line.csv', 'stock.csv']

def main():
    connection.setup(IP_ADDRESSES, KEYSPACES[0])
    
    for i in range(len(DATA_FILES)):
        print(f'Loading data from {DATA_FILES[i]}...')
        load_data_to_table(i)
    
    print('Completed data loading!')
        
        
def parse_date_time(time_string):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f')
                
# count is used for mock data, uncomment the lines if you only need mock data
def load_data_to_table(index):
    with open(PATH_TO_DATA_FOLDER + DATA_FILES[index]) as data_file:
        file_reader = csv.reader(data_file, delimiter=',')
        current_table = TABLES[index]
        keys = current_table().keys()
        count = 0
        
        for row in file_reader:
            if count >= 15:
                break
            count += 1
            records_dict = {}
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    key = keys[i]
                    if isinstance(current_table._columns[keys[i]], columns.DateTime):
                        value = parse_date_time(row[i])
                    else:
                        value = row[i]
                    records_dict[key] = value 
            current_table.create(**records_dict)
        
if __name__ == "__main__":
    main()