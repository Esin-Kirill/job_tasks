from configs import *
import os
import pandas as pd
import requests
from datetime import date, timedelta
import requests
import time
import urllib3
import owncloud

# CHANGE DIR
path = r'<replaced>'
os.chdir(path)

# Словарь методов
DICT_METHODS = {
    '<replaced>':'<replaced>',
    '<replaced>':'<replaced>',
    '<replaced>':'<replaced>',
    '<replaced>':'<replaced>'
}
   
# GLOBALS
METHODS_LABOUR_DYNAMICS = [m1, m2, m3, m4, m5, m6, m7, m8, m9]
METHODS_PIVOT_RF = [m10, m11, m12, m13, m14, m15]
DATE_YESTERDAY = date.today() - timedelta(days=1)
DATE_BEFORE_YESTERDAY = date.today() - timedelta(days=2)
DATE_REPORT = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
DATE_COMPARE = (date.today() - timedelta(days=8)).strftime('%Y-%m-%d')
REMOTE_DIR = '<replaced>'
HEADERS = {'User-Agent':'<replaced>'}

# FUNCTIONS
def log_time():
    return time.strftime('%a, %d %b %Y %H:%M |')

# Отправка в OwnCloud
def send_report_owncloud(file):
    # Auth OwnCloud
    urllib3.disable_warnings()
    oc = owncloud.Client(PAGE, verify_certs=False)
    oc.login(USER, PASS)
    
    # Send report
    path = REMOTE_DIR + "/" +  file
    oc.put_file(path, file)
    print(log_time(), 'Отчет по автотесту загружен в OwnClud.')

# Отправка сообщения ботом
def send_telegram(text):
    token = "<replaced>"
    url = "https://api.telegram.org/bot"
    channel_id = 0
    url += token
    method = url + "/sendMessage"

    r = requests.post(method, data={"chat_id": channel_id, "text": text})
    if r.status_code != 200:
        print(log_time(), r.status_code, r.text)

# Виджеты отображают данные
def check_methods(tip, methods):
    check = {}
    for method in methods:
        code = requests.get(method, headers=HEADERS).status_code
        if code != 200:
            name = method.split('/')[7].split('?')[0]
            name = tip + DICT_METHODS[name]
            check[name] = False
        else:
            name = method.split('/')[7].split('?')[0]
            name = tip + DICT_METHODS[name]
            check[name] = True
    
    return check

# В графе "replaced" вчерашняя дата
def check_api_date(api_path):
    result = requests.get(api_path, headers=HEADERS).json()
    date_api = result[-1]['<replaced>']
    if date_api == DATE_REPORT:
        return True
        
    return False

# В "replaced" указана разница
def check_api_delta(api_path):
    result = requests.get(api_path, headers=HEADERS).json()
    delta = result.get('<replaced>'), result.get('<replaced>')
    if all(delta):
        return True
    
    return False

# В "replaced" указана разница в серых скобках
def check_api_percent(api_path):
    result = requests.get(api_path, headers=HEADERS)
    result = result.json()['kpi']
    params = [kpi.get('<replaced>') for kpi in result]
    percent = [kpi.get('<replaced>') for kpi in result]
    if all(params) and all(percent) and len(params) == len(percent):
        return True
    
    return False

# Собираем статистику в файл
def collect_stats(methods_check):
    # Collect
    method = m11.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE)
    stats = requests.get(method, headers=HEADERS)

    method = m14.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE)
    prin_uvol = requests.get(method, headers=HEADERS)

    # Collect statuses of methods
    df_methods = {key:[methods_check.get(key)] for key in methods_check}
    df_methods = pd.DataFrame(df_methods)

    if stats.status_code==200 and prin_uvol.status_code==200:
        # New stats
        stats = stats.json()
        prin_uvol = prin_uvol.json()
        df = {
            '<replaced>': [DATE_YESTERDAY],
            '<replaced>':[stats[0].get('value')],
            '<replaced>':[stats[1].get('value')], 
            '<replaced>':[stats[2].get('value')], 
            '<replaced>':[stats[3].get('value')], 
            '<replaced>':[stats[4].get('value')], 
            '<replaced>':[prin_uvol.get('value')], 
            '<replaced>':[prin_uvol.get('value')]
            }
        df = pd.DataFrame(df)
        cols = df.columns.to_list()[1:]
        df[cols] = df[cols].astype('int64')

        # Concat 
        file = f"Отчет автотеста за {date.today().strftime('%Y-%m-%d')}.xlsx"
        df_new = pd.concat([df, df_methods], axis=1)
        df_new.to_excel(file, index=False)

        # Send
        send_report_owncloud(file)

        # Save to old file
        file = 'Statistics.xlsx'
        df_old = pd.read_excel(file)
        df_old = pd.concat([df_old, df], axis=0, ignore_index=True)
        df_old['Дата отчета'] = df_old['Дата отчета'].astype('datetime64').dt.date
        df_old = df_old.drop_duplicates()
        df_old.to_excel(file, index=False)
        return True
    else:
        return False

# Проверка, что данные за день изменились
def check_stats_change():
    # Check statistics change
    cols = ['<replaced>', '<replaced>', '<replaced>']
    df = pd.read_excel('Statistics.xlsx')
    df['Дата отчета'] = df['Дата отчета'].astype('datetime64').dt.date

    # Check
    check = []
    check_text = ''
    row_new = df.loc[df['Дата отчета']==DATE_YESTERDAY, cols].values[0]
    row_old = df.loc[df['Дата отчета']==DATE_BEFORE_YESTERDAY, cols].values[0]
    for i, col in enumerate(cols):
        if row_new[i] == row_old[i]:
            check += [False]
            check_text += f"- {col}: {row_new[i]} ({DATE_YESTERDAY}) == {row_old[i]} ({DATE_BEFORE_YESTERDAY})\n"
        else:
            check += [True]

    return all(check), check_text


def report_error(tip, methods, date_api, prin_uvol, percent_api):
    if all(methods.values()) and all([date_api, prin_uvol, percent_api]):
        return True, ''
    else:
        text = "Кажется, есть некоторые ошибки в " + tip

        if False in methods.values():
            text += "Не отвечают методы:\n"
            for key, val in methods.items():
                if val == False:
                    text += "- " + key + "\n"
        
        if not all([date_api, prin_uvol, percent_api]):
            text += "Ошибки:\n"
            if not date_api:
                text += "- " + "В чекбоксе '<replaced>' НЕ вчерашняя дата.\n"

            if not prin_uvol:
                text += "- " + "В '<replaced>' НЕ указана разница.\n"

            if not percent_api:
                text += "- " + "В '<replaced>' НЕ указана разница в серых скобках.\n"

        return False, text

def main():
    # Start
    print(log_time(), 'start')

    # <replaced>
    tip = '<replaced>'
    methods = [method.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE) for method in METHODS_LABOUR_DYNAMICS]
    methods1 = check_methods(tip, methods)
    date_api = check_api_date(m1.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE))
    prin_uvol = check_api_delta(m4.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE))
    percent_api = check_api_percent(m6.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE))
    status1, status1_text = report_error(tip, methods1, date_api, prin_uvol, percent_api)    

    # <replaced>
    tip = '<replaced>'
    methods = [method.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE) for method in METHODS_PIVOT_RF]
    methods2 = check_methods(tip, methods)
    date_api = check_api_date(m10.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE))
    prin_uvol = check_api_delta(m14.format(date_report=DATE_REPORT, date_compare=DATE_COMPARE))
    percent_api = True
    status2, status2_text = report_error(tip, methods2, date_api, prin_uvol, percent_api)

    # Собираем статистику в файл
    methods_check = {key:methods1.get(key, False)+methods2.get(key, False) for key in list(methods1.keys()) + list(methods2.keys())}
    collect_stats(methods_check)

    # Проверка, что данные обновились
    status3, status3_text = check_stats_change()
    status3_text = '' if status3_text == '' else '<replaced>' + status3_text

    # Send message
    if all([status1, status2, status3]):
        text = "Всем привет! Всё ок."
        print(log_time(), text)
        send_telegram(text)
    else:
        text = 'Всем привет!\n' + status1_text + status2_text + status3_text
        text += '<replaced>'
        print(log_time(), text)
        send_telegram(text)

    # End
    print(log_time(), 'end')
    
if __name__ == "__main__":
    main()
