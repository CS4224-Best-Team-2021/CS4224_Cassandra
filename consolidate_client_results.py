def main():
    print('consolidating client results...')
    with open('results/clients.csv', 'w') as outfile:
        for i in range(0, 40):
            with open(f'results/{i}.csv') as infile:
                outfile.write(infile.read())
    print('client results have been consolidated into result/clients.csv')
        
if __name__ == "__main__":
    main()