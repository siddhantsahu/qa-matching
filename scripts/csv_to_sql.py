"""Module to write parse multi-line csv files in pandas and write to a sqlite database.
Only pass the csv files of stacksample dataset.
"""
import argparse
import sqlite3

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description='CSV to SQLite')
    parser.add_argument('sqlite_db', help='path to sqlite3 database')
    parser.add_argument('csv_file', help='csv file path')
    parser.add_argument('table_name', help='table name to store it, replaces data if table exists')
    parser.add_argument('--chunksize', help='chunksize that gets written to sql, default 50000',
                        default=50000)
    args = parser.parse_args()

    conn = sqlite3.connect(args.sqlite_db)
    df = pd.read_csv(args.csv_file, encoding='latin1')
    df.to_sql(args.table_name, conn, if_exists='replace', chunksize=args.chunksize)


if __name__ == '__main__':
    main()