import sqlite3
def change_data_fromat:
    conn = sqlite3.connect('chinese.db')
    cur = conn.cursor()
    for row in rows:
        cur.execute("select * from history_data")
        print(row)