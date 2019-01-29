import sqlite3
# 用于合并总表后，清除临时表，注意：需手工更名，然后再更名回来：
# sqlite 命令：alter table history_data rename to stock_data;
# 最后：alter table stock_data rename to history_data;
def delete_sqlite_tables():
    conn = sqlite3.connect('chinese.db')
    cur = conn.cursor()
    cur.execute("select name from sqlite_master where type = 'table';")
    rows = cur.fetchall()
    counter = 0
    for row in rows:
        print(row)
        # print(type(row))
        table_name = row[0]
        if not row != ('stock_data',):
            print('保留，不删除！')
        else:
            cur.execute("drop table {}".format(table_name))
            counter += 1
        print(counter) 
        print('删除此表！')
        
    conn.commit()
    conn.close()


def rename_vacuum():
    conn = sqlite3.connect('chinese.db')
    cur = conn.cursor()
    cur.execute("alter table stock_data rename to history_data;")
    conn.commit()
    conn.close()

def sqlite_vacuum():
    conn = sqlite3.connect('chinese.db')
    cur = conn.cursor()
    cur.execute("vacuum;")
    conn.commit()
    conn.close()

if __name__ == '__main__':
    delete_sqlite_tables()
    rename_vacuum()
    # sqlite_vacuum()