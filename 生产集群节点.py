import pandas as pd
import base64
import json
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


prodb = {
  'database': 'prod',
  'username': 'root',
  'password': 'prod',
  'host': '1.1.1.1',
  'port': '3306',
  'drivername': 'mysql+pymysql',
  'query': {'charset': 'utf8'}
}

engine_prod = create_engine(URL(**prodb))

engine_local = create_engine('mysql+pymysql://user:pass@localhost:3306/test?charset=utf8')



def local():
    excel = pd.ExcelFile("生产集群节点.xlsx")
    sheet_names = excel.sheet_names

    for sheet_name in sheet_names:

        if 'df' not in locals():
            df = pd.read_excel("生产集群节点.xlsx", sheet_name)
            df['vc_cluster'] = sheet_name
        else:
            df_ = pd.read_excel("生产集群节点.xlsx", sheet_name)
            df_['vc_cluster'] = sheet_name
            df = pd.concat([df, df_], join='inner')

    df.rename(columns={"主机名": "hostname"}, inplace=True)

    df.to_sql('cluster_info', engine, if_exists='replace')


def read_local():
    local = pd.read_sql_query('SELECT * FROM cluster_info', engine_local)
    return local


def read_prod():
    prod = pd.read_sql_query('SELECT * FROM cluster_info', engine_prod)
    return prod


def gen_concated():
    prod = read_prod()
    local = read_local()
    prod_only = prod[-prod['IP'].isin(local)]

    df = pd.concat([prod_only, local], join='outer')

    # gen auto_increasement primary key `id`
    df.reset_index(drop=True, inplace=True)
    df['id'] = result.index + 1

    return df


df = gen_concated()

# remove the default index label field
df.to_sql('cluster_info_update', engine_prod, if_exists='append', index=False)
# <<<END