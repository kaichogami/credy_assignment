from bs4 import BeautifulSoup
import requests
import pandas as pd
from os import listdir
from os.path import isfile, join
import sqlite3
from pandas.util.testing import assert_frame_equal

URL = "https://www.rbi.org.in/scripts/bs_viewcontent.aspx?Id=2009"

def get_links():
    r = requests.get(URL)
    soup = BeautifulSoup(r.text)
    td_list = soup.findAll('td')
    #first td has every link we want

    a_tag = td_list[0].findAll("a")
    links = []
    for tag in a_tag:
        links.append(tag.get('href'))
    return links

def get_excels(links):
    """
    df_list = []
    for l in links:
        df = pd.read_excel(l).parse(0)
        df_list.append(df)
    return df_list
    """
    df_list = []
    files = [f for f in listdir('corpus') if isfile(join('corpus', f))]
    for f in files:
        df_list.append(pd.read_excel("corpus/" + f))
    return df_list


def check_changes(old_df, new_df):
    import numpy as np
    if len(old_df) != len(new_df):
        return ('length', new_df)

    # check if their data has changed in any way
    # if it has, return the new_df

    old_df = old_df.sort_values('IFSC')    
    new_df = new_df.sort_values('IFSC')
    old_df.index = new_df.index
    new_df.iloc[:, 5] = new_df.iloc[:, 5].astype(float)

    old_df =old_df.applymap(unicode)
    new_df = new_df.applymap(unicode)
    if check_df(old_df, new_df):
        return ('no_change', old_df)
    else:
        return ('value_change', new_df)

def check_df(old_df, new_df):
    for col in old_df.columns:
        if list(old_df[col] == new_df[col]).count(False) >0:
           return False

    if old_df['BANK'].iloc[0] == "ANDHRA PRAGATHI GRAMEENA BANK":
        pass

    return True
            
def save_database(df, table_name, db):
    df.to_sql(table_name, db, if_exists="replace")

def combine(df_list):
    return pd.concat(df_list)

def get_df_from_db(db, table_name, bank_name):
    df = pd.read_sql_query("SELECT * FROM {0} WHERE BANK = '{1}';".format(table_name,
                                                                       bank_name),
                           db, coerce_float=False)
    return df

def get_branch(db, table_name, ifsc):
    cur = conn.cursor()
    cur.execute("SELECT * FROM {0} WHERE IFSC = '{1}';".format(table_name,
                                                           ifsc))
    result = cur.fetchall()
    return result


if __name__ == '__main__':
    DO = raw_input('What needs to be done?\n1)create\n2)update\n3)query\n')
    db_name = 'datas.db'
    table_name = 'banks'
    if DO == 'create':
        all_df = get_excels([])
        one_df = combine(all_df)
        one_df = one_df.applymap(unicode)

        # create sqlite3 connection and save data
        conn = sqlite3.connect(db_name)
        save_database(one_df, table_name, conn)
        conn.close()

    elif DO == 'update':
        conn = sqlite3.connect(db_name)
        all_df = get_excels([])
        change_list = []
        for index, new_df in enumerate(all_df):
            old_df = get_df_from_db(conn, table_name, new_df.iloc[0]['BANK'])
            old_df.drop('index', axis=1, inplace=True)
            return_values = check_changes(old_df, new_df)
            all_df[index] = return_values[1]
            change_list.append(return_values[0])

        #check if change_list has all no_change value
        print(change_list)
        if change_list.count('no_change') == len(change_list):
            print('no change, nothing to update')
        else:
            print("saw changes")
            save_database(combine(all_df), table_name, conn)
        conn.close()

    elif DO =='query':
        conn = sqlite3.connect(db_name)
        ifsc = raw_input('enter ifsc')
        print(get_branch(conn, table_name, ifsc))
        conn.close()
