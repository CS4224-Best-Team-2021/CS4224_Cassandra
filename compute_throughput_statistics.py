import csv

def main():
    print('computing throughput statistics...')
    with open('results/clients.csv') as f:
        file_reader = csv.reader(f, delimiter=',')
        throughputs = []
        for row in file_reader:
            transaction_throughput = float(row[2])
            throughputs.append(transaction_throughput)
        min_throughput = min(throughputs)
        max_throughput = max(throughputs)
        avg_throughput = sum(throughputs) / len(throughputs)

    with open('results/throughput.csv', 'w') as throughput_file:
        file_writer = csv.writer(throughput_file)
        file_writer.writerow([min_throughput, max_throughput, avg_throughput])
        print('throughput statistics have been written to results/throughput.csv')
    
if __name__ == "__main__":
    main()