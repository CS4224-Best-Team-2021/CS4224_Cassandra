# This file is for cleaning data prior to using load_db_data.cql

# replace all occurrences of null with -1 in order.csv
with open('project_files_4/data_files/order.csv') as f:
    newText=f.read().replace('null', '-1')
with open('project_files_4/data_files/modified-order.csv', "w") as f:
    f.write(newText)
    
# replace all occurrences of null with empty value in order-line.csv 
with open('project_files_4/data_files/order-line.csv') as f:
    newText=f.read().replace(',null,', ',,')
with open('project_files_4/data_files/modified-order-line.csv', "w") as f:
    f.write(newText)