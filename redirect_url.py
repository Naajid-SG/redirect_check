import sys
import pandas as pd, numpy as np
import subprocess
from sqlalchemy import create_engine


engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
                        .format(user="admin",pw="d5Sj5U7lZqwNYsqRjhJI", db="datacollection"))
conn = engine.connect()
print("Success Connection")

df = pd.read_excel("DomainsToScrape.xlsx")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
c=0
for indx,row in df[:10].iterrows():
    try:
        result = subprocess.check_output([
            'curl',
            '-Ls',
            '-w',
            '%{url_effective}',
            '-o',
            '/dev/null',
            row.Domain
        ])
    except BaseException as E:
        result = E

 
    conn.execute(
                'insert ignore into redirect_url (Domain, redirected_to)  values(%s,%s)', (row.Domain,result.decode('utf8')))

    c+=1
    print(result)
    print(c, end = "")
    sys.stdout.flush()
