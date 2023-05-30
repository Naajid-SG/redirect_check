import sys
import pandas as pd, numpy as np
import subprocess
from sqlalchemy import create_engine



# print("Success Connection")

df1 = pd.read_csv("meta_all_domains_distinct.csv")
df2 = pd.read_csv("redirect_domains.csv")

df3 = df1[~df1['Domain'].isin(df2['domain'])]

domain_list = df3['Domain'].tolist()
del df1, df2, df3

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
c=0
for domain in domain_list:
    try:
        result = subprocess.check_output([
            'curl',
            '-Ls',
            '-w',
            '%{url_effective}',
            '-o',
            '/dev/null',
            domain
        ])
    except BaseException as E:
        result = E
    engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
                        .format(user="admin",pw="d5Sj5U7lZqwNYsqRjhJI", db="datacollection"))
    conn = engine.connect()
    conn.execute(
                'insert ignore into redirect_url (Domain, redirected_to)  values(%s,%s)', (domain,result))
    
    conn.close()
    c+=1
    print(result)
    print(c, end = "")
    sys.stdout.flush()

