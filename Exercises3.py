import requests
import json
from fake_useragent import UserAgent
from apscheduler.schedulers.blocking import BlockingScheduler
import pymssql
import datetime
from concurrent.futures import ThreadPoolExecutor

# 根據自己的Database來填入資訊
db_settings = {
    "host": "127.0.0.1",
    "user": "BlackWater",
    "password": "blender628",
    "database": "NeverCareU",
    "charset": "utf8"
}

# 兩家公司的即時股價網站 台積電、南亞
all_url = ['https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_2330.tw&json=1&delay=0', 'https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_1303.tw&json=1&delay=0']

def set_schedule():
    conn = pymssql.connect(**db_settings)   # 連接MSSQL，並使用前面寫好的設定
    with conn.cursor() as cursor:
        today = datetime.date.today().strftime('%Y-%m-%d')
        print(today)
        command = "Select * from [dbo].[calendar] where date  = '" + today + "'"
        cursor.execute(command)
        result = cursor.fetchall()[0]
        # print(result[2])
        # set result[1] != -1 when in practice
        if result[1] != 0:
            scheduler = BlockingScheduler()

            # use in practice
            # start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # end = datetime.date.today().strftime('%Y-%m-%d') + " 14:00:00"

            # testing use
            start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            time_del = datetime.timedelta(seconds=10)
            end = datetime.datetime.now() + time_del
            end = end.strftime('%Y-%m-%d %H:%M:%S')

            scheduler.add_job(daily_search, 'interval', seconds=5, start_date=start, end_date=end, next_run_time=start)
 
            scheduler.start()
        else:
            print("no work!")
    conn.close()

def daily_search():
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(find, all_url)

# load JSON & load db 
def find(url):
    ua = UserAgent()
    response = requests.get(url, headers = {'User-Agent': ua.random})  # response.text為json格式
    dict = json.loads(response.text)    # 把json格式轉換為Python的dictionary
    info = dict['msgArray'][0]
    stock = []
    stock.append(int(info['c']))    # 股票代號
    stock.append(info['d'][:4] + '-' + info['d'][4:6] + '-' + info['d'][6:8])    # 日期
    stock.append(info['t'])    # 時間
    stock.append(int(info['v']) * 1000)  # 成交股數 (上市)
    stock.append(0) #成交金額 
    stock.append(int(float(info['o'])))    # 開盤價
    stock.append(int(float(info['h'])))    # 最高價
    stock.append(int(float(info['l'])))    # 最低價
    stock.append(0 if info['z'] == '-' else int(float(info['z'])))   # 收盤價
    stock.append(0 if info['z'] == '-' else int(float(info['z'])) - int(float(info['y'])))    # 漲跌價差
    stock.append(0) # 成交筆數
    print(stock)    # 請自己注意存入的類型和格式
    # 20230306 print出來的結果
    # ['2330', '20230306', '13:30:00', 21846000.0, 0, '520.0000', '524.0000', '517.0000', 521.0, 5.0, 0]
    try:
        conn = pymssql.connect(**db_settings)   # 連接MSSQL，並使用前面寫好的設定
        # 要執行的命令 (注意型態)
        query = "INSERT INTO [dbo].[realtime_data](stock_code, date, time, tv, t, o, h, l, c, d, v) VALUES (%d, %s, %s, %d, %d, %d, %d, %d, %d, %d, %d)"    
        with conn.cursor() as cursor:
            cursor.execute(query, (stock[0], stock[1], stock[2], stock[3], stock[4], stock[5], stock[6], stock[7], stock[8], stock[9], stock[10]))   # 執行命令
        conn.commit()   # 記得要commit，才會將資訊儲存到資料庫，不然只會暫存到記憶體
        conn.close()
    except Exception as e:
        print(e)

set_schedule()
