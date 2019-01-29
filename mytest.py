from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
import time
from datetime import datetime
from datetime import timedelta
# from numpy import numpy

def download_all_histroy():
    engine = create_engine('sqlite:///chinese.db')
    stock_basics = ts.get_stock_basics()
    total = len(stock_basics)
    current = 0
    for code, row in stock_basics.iterrows():
        if row['timeToMarket'] != 0 and not engine.dialect.has_table(engine, 'history_data_' + code):
            timeToMarket = str(row['timeToMarket'])
            timeToMarket = "{}-{}-{}".format(timeToMarket[0:4], timeToMarket[4:6], timeToMarket[6:8])
            print("\n{}({}/{})".format(code, current, total))

            def download_history(code):
                for _ in range(3):
                    try:
                        history = ts.get_h_data(code, start = timeToMarket, pause=2)
                        history.to_sql('history_data_' + code, engine, if_exists='append')
                        return
                    except Exception as e:
                        time.sleep(300)
                        print(e)
                print("Failed to download code {}".format(code))

            download_history(code)

        current += 1

import sqlite3

def merge_history_data():
    conn = sqlite3.connect('chinese.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS history_data(
        code TEXT,
        date DATETIME,
        open FLOAT,
        high FLOAT,
        close FLOAT,
        low FLOAT,
        volume FLOAT,
        amount FLOAT,
        PRIMARY KEY (code,date) 
        )''')
    c.execute('CREATE INDEX IF NOT EXISTS index_code ON history_data(code)')
    c.execute('CREATE INDEX IF NOT EXISTS index_date ON history_data(date)')
    counter = 0
    rows = c.execute("SELECT name from sqlite_master where type = 'table' AND name != 'history_data'").fetchall() 
    total_code = len(rows)
    for row in rows:
        table_name = row[0]
        code = str(table_name[-6:])
        c.execute("INSERT INTO history_data SELECT '{}' AS code, * FROM {}".format(code,table_name))
        counter += 1
        print("{},{},{}".format(code,counter,total_code))

    conn.commit()
    conn.close()
    print('ok.')

def update_new_histroy():
    conn = sqlite3.connect('chinese.db')
    c = conn.cursor()
    update_stock_basic = ts.get_stock_basics()
    total = len(update_stock_basic)
    current = 0
    for code,row in update_stock_basic.iterrows():
        def download_none_history(timeToMarket):
            for _ in range(3):
                try:
                    none_history = ts.get_h_data(code,start = timeToMarket, pause = 4 )
                    print(none_history)
                    for date_none,row_none in none_history.iterrows():
                        # print(f"INSERT INTO history_data (code,date,open,high,close,low,volume,amount) VALUES ('{code}','{date_none}',{row_none.open},{row_none.high},{row_none.close},{row_none.low},{row_none.volume},{row_none.amount})")
                        print("Start Inserting Data To Sqlite...")
                        print("INSERT INTO history_data (code,date,open,high,close,low,volume,amount) VALUES (?,?,?,?,?,?,?,?)",(code,str(date_none),row_none.open,row_none.high,row_none.close,row_none.low,row_none.volume,row_none.amount))
                        # c.execute(f"INSERT INTO history_data (code,date,open,high,close,low,volume,amount) VALUES ('{code}','{date_none}',{row_none.open},{row_none.high},{row_none.close},{row_none.low},{row_none.volume},{row_none.amount})")
                        c.execute("INSERT INTO history_data (code,date,open,high,close,low,volume,amount) VALUES (?,?,?,?,?,?,?,?)",(code,str(date_none),row_none.open,row_none.high,row_none.close,row_none.low,row_none.volume,row_none.amount))
                    return
                except Exception as e:
                    time.sleep(60)
                    print(e)
                    print("Failed to download Data,Code: {}".format(code))       
        last_date = c.execute("SELECT MAX(date) from history_data where code = '{}'".format(code)).fetchall()
        print("Get Old History Timepoint..." + str(last_date))
        if last_date[0][0] is None:
            timeToMarket = str(row['timeToMarket'])
            timeToMarket = "{}-{}-{}".format(timeToMarket[0:4], timeToMarket[4:6], timeToMarket[6:8])
            current += 1
            # print("\n{}({}/{})".format("Code:"+code,"Current:"+current,"Total Number:"+total))
            download_none_history(timeToMarket)
        else:
            update_date = datetime.strptime(last_date[0][0][:10],'%Y-%m-%d')+timedelta(days=1)
            update_date_str = str(update_date)
            print("Changing TimePoint To update..."+str(update_date_str))
            download_none_history(update_date_str)
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    # merge_history_data()
    update_new_histroy()
