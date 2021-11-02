# replace all occurrences of null with -1 in order.csv
with open('order.csv') as f:
    newText=f.read().replace('null', '-1')
with open('modified-order.csv', "w") as f:
    f.write(newText)
    
# replace all occurrences of null with empty value in order-line.csv 
with open('order-line.csv') as f:
    newText=f.read().replace('null', '')
with open('modified-order-line.csv', "w") as f:
    f.write(newText)