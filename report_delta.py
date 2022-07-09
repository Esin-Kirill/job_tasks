import pandas as pd
import psycopg2 
import os
from datetime import date, timedelta
import urllib3
import owncloud
import time
from configs import *

# CHANGE DIR
os.chdir(r'<replaced>')

# OwnCloud
REMOTE_DIR = '<replaced>'

sql3 = """
        drop table if exists current_tmp;
        create temp table current_tmp as 
        select 
            fphd."Rep_Date"
            ,fphd."Region_ID"
            ,count(distinct "<replaced>") as "<replaced>"
            ,sum("<replaced>") as "<replaced>"
            ,sum("<replaced>") as "<replaced>"
        from "<replaced>" fphd 
        where "Rep_Date" between '{date_from}' and '{date_to}'
        group by 1, 2;

        drop table if exists last_tmp;
        create temp table last_tmp as 
        select 
            fphd."Rep_Date"
            ,fphd."Region_ID"
            ,count(distinct "<replaced>") as "<replaced>"
            ,sum("<replaced>") as "<replaced>"
            ,sum("<replaced>") as "<replaced>"
        from "<replaced>" fphd 
        where "Rep_Date" between '{date_from2}' and '{date_to}'
        group by 1, 2;

        drop table if exists emp_tmp;
        create temp table emp_tmp as
        select 
            fpen."Region_ID"
            ,sum(fpen."<replaced>") as "<replaced>"
        from "<replaced>" fpen 
        where "Rep_Date" = (select max("Rep_Date") from "<replaced>")
        group by 1;


        --FINAL SELECT
        select 
            ctp."Rep_Date"
            ,dr."Region" 
            ,ctp."<replaced>"
            ,ctp."<replaced>" - ltp."<replaced>"
            ,ctp."<replaced>" - ltp."<replaced>"
            ,etp."<replaced>"
        from current_tmp ctp
        inner join "D_Region" dr on dr."Region_ID" = ctp."Region_ID"
        left join last_tmp ltp on ltp."Region_ID" = ctp."Region_ID" and (ltp."Rep_Date"+1)::date = ctp."Rep_Date"
        left join emp_tmp etp on etp."Region_ID" = ctp."Region_ID";
    
"""

def log_time():
    return time.strftime('%H:%M |')


def format_sql(sql):
    date_today = date.today() # Запускается во вторник

    date_to = date_today.strftime('%Y-%m-%d')
    date_from = (date_today - timedelta(days=8)).strftime('%Y-%m-%d')
    date_from2 = (date_today - timedelta(days=9)).strftime('%Y-%m-%d')
    
    sql = sql.format(date_to=date_to, date_from=date_from, date_from2=date_from2)
    print(log_time(), f'Date_to: {date_to}, Date_from: {date_from}, Date_from2: {date_from2}.')
    return sql


def connect_and_execute(sql, cols=None):
    # Connect
    conn = psycopg2.connect(dbname='<replaced>', user='<replaced>',
                            password='<replaced>', host='<replaced>', port='<replaced>')
    cursor = conn.cursor()
    
    # Execute
    cursor.execute(sql)
    
    # Collect
    df = pd.DataFrame(cursor.fetchall(), columns=cols)
    return df


def send_report(remote_dir, file, user, password):
    # Auth OwnCloud
    urllib3.disable_warnings()
    oc = owncloud.Client('<replaced>', verify_certs=False)
    oc.login(user, password)
    
    # Send report
    path = remote_dir + "/" +  file
    oc.put_file(path, file)


def main(sql):
    # Start
    print(log_time(), 'Start.')
    
    # Preprocess
    sql = format_sql(sql)
    
    # Execute
    cols = ["<replaced>"]
    df = connect_and_execute(sql, cols=cols)
    print(log_time(), 'Executed.')
    
    # Process
    df[cols[2:]] = df[cols[2:]].astype('int64')
    
    # Sort
    df = df.sort_values(by=["<replaced>"])
    print(log_time(), 'Processed.')
    
    # Save
    date_report = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    file = f'<replaced> ({date_report}).xlsx'
    df.to_excel(file, index=False)
    print(log_time(), 'Saved.')
    
    # Send
    send_report(REMOTE_DIR, file, USER_OC, PASS_OC)
    print(log_time(), 'Send.')
    print(log_time(), 'End.')

if __name__ == '__main__':
    main(sql3)