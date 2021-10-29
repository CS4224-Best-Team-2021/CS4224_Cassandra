import pandas as pd

def main():
    orders = pd.read_csv('project_files_4/data_files/order.csv', header=None,
                          names=['O_W_ID', 'O_D_ID', 'O_ID', 'O_C_ID', 'O_CARRIER_ID', 'O_OL_CNT', 'O_ALL_LOCAL', 'O_ENTRY_D'])
    orders = orders.filter(items=['O_W_ID', 'O_D_ID', 'O_ID', 'O_C_ID'])

    order_lines = pd.read_csv('project_files_4/data_files/order-line.csv', header=None,
                              names=['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_DELIVER_D',
                               'OL_AMOUNT', 'OL_SUPPLY_W_ID', 'OL_QUANTITY', 'OL_DIST_INFO'])
    order_lines = order_lines.filter(items=['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_I_ID'])

    combined = order_lines.merge(orders, how='left', left_on=['OL_W_ID', 'OL_D_ID', 'OL_O_ID'], right_on=['O_W_ID', 'O_D_ID', 'O_ID'])
    combined = combined.filter(items=['O_W_ID', 'O_D_ID', 'O_C_ID', 'O_ID', 'OL_I_ID'])
    combined.rename(columns={'O_W_ID':'W_ID', 'O_D_ID':'D_ID', 'O_C_ID':'C_ID', 'OL_I_ID':'I_ID'}, inplace=True)

    fragments = {}
    for i in range(10):
        fragments[i] = combined[combined['W_ID']==i+1]

    dataframes = []
    for i in range(9):
        for j in range(i+1, 10):
            left = fragments[i]  # warehouse i
            right = fragments[j] # warehouse j
            pair1 = left.merge(right, on='I_ID') # orders with one item in common
            # Note: C_ID_x and C_ID_y are not required for the join but we include them in order to avoid duplicating columns
            pair2 = pair1.merge(pair1, on=['W_ID_x', 'D_ID_x', 'C_ID_x', 'O_ID_x', 'W_ID_y', 'D_ID_y', 'C_ID_y', 'O_ID_y']) # orders with two items in common
            distinct = pair2[pair2['I_ID_x']!=pair2['I_ID_y']].filter(items=['W_ID_x', 'D_ID_x', 'C_ID_x', 'W_ID_y', 'D_ID_y', 'C_ID_y']).drop_duplicates()
            dataframes.append(distinct)

    all_pairs = pd.concat(dataframes)
    double_pairs = all_pairs.append(all_pairs.rename(columns={'W_ID_x':'W_ID_y', 'D_ID_x':'D_ID_y', 'C_ID_x':'C_ID_y', 'W_ID_y':'W_ID_x', 'D_ID_y':'D_ID_x', 'C_ID_y':'C_ID_x'}))

    double_pairs.to_csv('project_files_4/data_files/related_pairs.csv', header=False, index=False)

if __name__ == "__main__":
    main()
