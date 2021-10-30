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
    grouped = combined.groupby(['W_ID', 'D_ID', 'C_ID', 'O_ID'], as_index=False).agg({'I_ID':[min, max, set]})
    grouped.columns = ['W_ID', 'D_ID', 'C_ID', 'O_ID', 'I_ID_MIN', 'I_ID_MAX', 'ITEM_SET']

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
            distinct = pair2[pair2['I_ID_x']!=pair2['I_ID_y']].filter(items=['W_ID_x', 'D_ID_x', 'C_ID_x', 'O_ID_x', 'W_ID_y', 'D_ID_y', 'C_ID_y', 'O_ID_y']).drop_duplicates()
            dataframes.append(distinct)

    all_pairs = pd.concat(dataframes)
    double_pairs = all_pairs.append(all_pairs.rename(columns={'W_ID_x':'W_ID_y', 'D_ID_x':'D_ID_y', 'C_ID_x':'C_ID_y', 'O_ID_x':'O_ID_y', 'W_ID_y':'W_ID_x', 'D_ID_y':'D_ID_x', 'C_ID_y':'C_ID_x', 'O_ID_y':'O_ID_x'}), ignore_index=True)
    related_dataset = double_pairs.merge(grouped, left_on=['W_ID_x', 'D_ID_x', 'C_ID_x', 'O_ID_x'], right_on=['W_ID', 'D_ID', 'C_ID', 'O_ID']).filter(items=['W_ID_x', 'D_ID_x', 'C_ID_x', 'O_ID_x', 'I_ID_MIN', 'I_ID_MAX', 'ITEM_SET', 'W_ID_y', 'D_ID_y', 'C_ID_y', 'O_ID_y'])
    related_dataset = related_dataset.merge(grouped, left_on=['W_ID_y', 'D_ID_y', 'C_ID_y', 'O_ID_y'], right_on=['W_ID', 'D_ID', 'C_ID', 'O_ID']).filter(items=['W_ID_x', 'D_ID_x', 'C_ID_x', 'O_ID_x', 'I_ID_MIN_x', 'I_ID_MAX_x', 'ITEM_SET_x', 'W_ID_y', 'D_ID_y', 'C_ID_y', 'O_ID_y', 'I_ID_MIN_y', 'I_ID_MAX_y', 'ITEM_SET_y'])
    related_dataset.to_csv('project_files_4/data_files/related_pairs.csv', header=False, index=False

if __name__ == "__main__":
    main()
