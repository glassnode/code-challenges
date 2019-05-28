from utils import QueryManager
from plots import pie, graph_compound
from functools import reduce
import matplotlib.pyplot as plt
import numpy as np

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

if __name__ == '__main__':
    data_limit = 700000
    start_point = 7790000
    manager = QueryManager()
    df_top = manager.do_query('top-erc20', 5, start_block=start_point)

    print(df_top)

    # Pie chart for 1) most active addresses
    #labels = [a for a in df_top['address']]
    labels = ['More Gold Coin (MGC)', 'FairDollars (FDS)', 'Internet Cashowbiz (ICBB)', 'Baer Chain (BRC)', 'CpcToken (CPCT)']
    tx_counts = [c for c in df_top['tx_count']]

    pie(tx_counts, labels, 'Onchain')

    df_top_transfers = manager.do_query('top-erc20-transfers', 5, start_block=start_point)
    print(df_top_transfers)

    labels = ['More Gold Coin (MGC)', 'FairDollars (FDS)', 'Internet Cashowbiz (ICBB)', 'Baer Chain (BRC)', 'YottaLing (YTL)']
    tx_counts = [c for c in df_top_transfers['tx_count']]
    pie(tx_counts, labels, 'TokenTransfer')

    #from Etherscan
    decimals = [18, 18, 18, 8, 18]
    #top_txns = {}
    #for address in df_top['address']:
    #    top_txns[address] = manager.do_query('transactions-to', data_limit, address=address, start_block=start_point)
    #    print(top_txns[address])
    #    print(top_txns[address].dtypes)

    tokensum = lambda x, y: int(x) + int(y)

    #studing only token transfer data
    top_transfer_txns = {}
    month_volumes = {}
    volumes = []

    for address in df_top['address']:
        top_transfer_txns[address] = manager.do_query('transactions-transfer', data_limit, address=address, start_block=start_point)
        print(top_transfer_txns[address])
        #print(top_transfer_txns[address].dtypes)
        #print(top_transfer_txns[address].apply(np.sum, axis = 0))
        values = top_transfer_txns[address]['value'].tolist()
        volumes.append(reduce(tokensum, values))
        month_volumes[address] = manager.do_query('daily-erc20-transfers', data_limit, address=address, start_block=7600000)
        print(month_volumes[address])

    weights = [ v / np.power(10, d) for v,d in zip(volumes, decimals)]

    print("Weights: {}".format(weights))

    graph_compound(month_volumes, 'montly-count')

    all_txns_dump = manager.do_query('transactions', 10000000, start_block=start_point)
    print(all_txns_dump)

    import sys
    sys.exit()

    print("Found txn volumes {}".format(volumes))

    for d, v in zip(decimals, volumes):
        v = v/np.power(10, d)

