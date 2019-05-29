#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
import os
import pandas
import hashlib

from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ilya/Documents/test-4759c933da2c.json"

def get_template(query_type: str, limit: int, *args, **kwargs)-> str:
    address = '0x0'
    if 'address' in kwargs:
        try:
            address = str(kwargs['address'])
        except:
            pass
    start_block = 0
    if 'start_block' in kwargs:
        try:
            start_block = int(kwargs['start_block'])
        except:
            pass
    if query_type == 'transactions':
        transactions = """
            SELECT * 
            FROM `bigquery-public-data.ethereum_blockchain.transactions` AS transactions
            WHERE transactions.block_number >= {0:.0f}
            LIMIT {1:.0f}
        """.format(start_block, limit)
        return transactions
    if query_type == 'transactions-to':
        transactions = """
            SELECT *
            FROM `bigquery-public-data.ethereum_blockchain.transactions` AS transactions
            WHERE TRUE
                AND transactions.to_address = "{0:}"
                AND transactions.block_number >= {1:.0f}
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return transactions
    if query_type == 'transactions-from':
        transactions = """
            SELECT *
            FROM `bigquery-public-data.ethereum_blockchain.transactions` AS transactions
            WHERE TRUE
                AND transactions.from_address = "{0:}"
                AND transactions.block_number >= {1:.0f}
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return transactions
    if query_type == 'token-transfers':
        transfers = """
            SELECT 
                SUM(CAST(value AS NUMERIC)/POWER(10,18)) AS daily_weight,
                DATE(timestamp) AS tx_date
            FROM 
                `bigquery-public-data.ethereum_blockchain.token_transfers` AS token_transfers, 
                `bigquery-public-data.ethereum_blockchain.blocks` AS blocks
            WHERE token_transfers.block_number >= {0:.0f}
            AND token_transfers.block_number = blocks.number
            AND token_transfers.token_address = "{1:}"
            GROUP BY tx_date
            ORDER BY tx_date
        """.format(start_block, address)
        return transfers
    if query_type == 'top-erc20':
        top = """
            SELECT contracts.address, COUNT(1) AS tx_count
            FROM `bigquery-public-data.ethereum_blockchain.contracts` AS contracts
            JOIN `bigquery-public-data.ethereum_blockchain.transactions` AS transactions ON ((transactions.block_number >= {0:.0f}) AND (transactions.to_address = contracts.address))
            WHERE contracts.is_erc20 = TRUE
            GROUP BY contracts.address
            ORDER BY tx_count DESC
            LIMIT {1:.0f}
        """.format(start_block, limit)
        return top
    if query_type == 'top-erc20-transfers':
        top = """
            SELECT contracts.address, COUNT(1) AS tx_count
            FROM `bigquery-public-data.ethereum_blockchain.contracts` AS contracts
            JOIN `bigquery-public-data.ethereum_blockchain.token_transfers` AS transactions ON ((transactions.block_number >= {0:.0f}) AND (transactions.token_address = contracts.address))
            WHERE contracts.is_erc20 = TRUE
            GROUP BY contracts.address
            ORDER BY tx_count DESC
            LIMIT {1:.0f}
        """.format(start_block, limit)
        return top
    if query_type == 'daily-erc20-transfers':
        top = """
            SELECT 
            COUNT(1) AS tx_count,
            DATE(block_timestamp) AS tx_date
            FROM 
            `bigquery-public-data.ethereum_blockchain.token_transfers` AS token_transfers
            WHERE TRUE
                AND token_transfers.token_address = "{0:}"
                AND token_transfers.block_number >= {1:.0f}
            GROUP BY tx_date
            ORDER BY tx_date
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return top
    if query_type == 'daily-erc20-transfers-volume':
        top = """
            SELECT 
            SUM(CAST(value AS NUMERIC)) AS volume,
            DATE(block_timestamp) AS tx_date
            FROM 
            `bigquery-public-data.ethereum_blockchain.token_transfers` AS token_transfers
            WHERE TRUE
                AND token_transfers.token_address = "{0:}"
                AND token_transfers.block_number >= {1:.0f}
            GROUP BY tx_date
            ORDER BY tx_date
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return top
    if query_type == 'transactions-transfer':
        transactions = """
            SELECT *
            FROM `bigquery-public-data.ethereum_blockchain.token_transfers` AS transactions
            WHERE TRUE
                AND transactions.token_address = "{0:}"
                AND transactions.block_number >= {1:.0f}
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return transactions
    if query_type == 'gas-cost':
        gas_cost = """
            SELECT 
              SUM(value/POWER(10,18)) AS sum_tx_ether,
              AVG(gas_price*(receipt_gas_used/POWER(10,18))) AS avg_tx_gas_cost,
              DATE(timestamp) AS tx_date
            FROM
              `bigquery-public-data.ethereum_blockchain.transactions` AS transactions,
              `bigquery-public-data.ethereum_blockchain.blocks` AS blocks
            WHERE TRUE
              AND transactions.block_number = blocks.number
              AND receipt_status = 1
              AND value > 0
            GROUP BY tx_date
            HAVING tx_date >= '2018-01-01' AND tx_date <= '2018-12-31'
            ORDER BY tx_date
        """
        return gas_cost

class QueryManager():
    def __init__(self):
        self.client = bigquery.Client()

    def do_query(self, query_type: str, limit: int, *args, **kwargs) -> pandas.DataFrame:
        sql = get_template(query_type, limit, *args, **kwargs)
        if sql == 'error':
            return pandas.DataFrame()
        query_id = hashlib.md5(sql.encode())
        if not os.path.exists("./cache"):
            os.makedirs("./cache")
        df_dump = "./cache/{}".format(query_id.hexdigest())
        if os.path.isfile(df_dump):
            df = pandas.read_pickle(df_dump)
            return df
        else:
            df = self.client.query(sql).to_dataframe()
            df.to_pickle(df_dump)
            return df


if __name__ == '__main__':
    manager = QueryManager()
    df = manager.do_query('top-erc20', 10, start_block=7790000)
    print(df.head())
    print(df.dtypes)
    df = manager.do_query('top-erc20-transfers', 10, start_block=7790000)
    print(df.head())
    print(df.dtypes)
    df = manager.do_query('transactions-to', 10, start_block=7230000, address = '0x174bfa6600bf90c885c7c01c7031389ed1461ab9')
    print(df.head())
    print(df.dtypes)