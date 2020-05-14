from sqlalchemy import create_engine
import pandas as pd
import os


filedir = 'RVTOOL'
sheets = [ 'vInfo', 'vHost', 'vDatastore', 'dvPort', 'dvSwitch', 'vCluster' ]
engine = create_engine('mysql+pymysql://root:@localhost:3306/test')


for sheet_name in sheets:

    for root, dirs, files in os.walk(filedir):
        if root == filedir:
            for file in files:
                file = os.path.join(root, file)
                if not 'df' in locals():
                    df = pd.read_excel(file, sheet_name)
                else:
                    df = pd.concat([df, pd.read_excel(file, sheet_name)], join='inner')

    print(sheet_name)
    df.to_sql(sheet_name, engine, if_exists='replace', index=True, index_label='id')

    del locals()['df']

