import os
from datetime import datetime, timedelta
import pandas as pd
import tushare as ts
class TushareTool:
    token = 'c3f87035d1bdaf8d946f8eca7e0de867e9ef30a40dc226973f44762f'
    ts.set_token(token)
    pro = ts.pro_api()

    def __init__(self):
        pass

    def get_all_stocks(self):
        return TushareTool.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    def get_trade_cal(self,start_date,end_date):
        result = TushareTool.pro.trade_cal(exchange='SSE',start_date=start_date,end_date=end_date);
        return result

    def get_index_daily(self,ts_code):
        result = TushareTool.pro.index_daily(ts_code)
        return result

    def get_bak_basic(self):
        basic_file= 'data/Basic.csv'
        result = pd.DataFrame()
        if os.path.exists(basic_file):
            result = pd.read_csv(basic_file)

        today = datetime.today()
        trade_date = today - timedelta(30);
        if len(result) > 0:
            trade_date =datetime.strptime(str(result['trade_date'].max()),'%Y%m%d')

        # keep 15 days
        if trade_date + timedelta(15) < today:
            return result

        trade_date = today
        tushare_result = TushareTool.pro.bak_basic(trade_date=trade_date.strftime('%Y%m%d'))
        if(len(tushare_result) > 0):
            result = tushare_result
            result.to_csv(basic_file,  index=False,float_format='%.2f')
        return result

    def download_k_line(self,stock, start_date, end_date):
        result = ts.pro_bar(ts_code=stock, start_date=start_date, end_date=end_date, adj='qfq', ma=[5, 10, 20, 30,60],
                            factors=['tor', 'vr'])
        return result

    def download_moneyflow(self):
        result = pd.DataFrame()
        moneyflow_file = "data/moneyflow.csv"
        if os.path.exists(moneyflow_file):
            result = pd.read_csv(moneyflow_file)

        today = datetime.today()
        end_date = today
        start_date = (today - timedelta(365 * 3))
        trade_cal = self.get_trade_cal(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"));
        end_date = datetime.strptime(str(trade_cal[trade_cal['is_open'] == 1]['cal_date'].max()),"%Y%m%d")

        if len(result) > 0 :
            start_date = datetime.strptime(str(result['trade_date'].max()),"%Y%m%d") + timedelta(1)

        while start_date <= end_date:
            try:
                df = TushareTool.pro.moneyflow(trade_date=start_date.strftime("%Y%m%d"))
            except:
                print("download money error date="+ start_date.strftime("%Y%m%d"))
                continue
            print("download moneyflow date=" + start_date.strftime("%Y%m%d"))
            start_date = start_date + timedelta(1)
            if os.path.exists(moneyflow_file):
                df.to_csv(moneyflow_file, mode='a', header=None, index=False, float_format='%.2f')
            else:
                df.to_csv(moneyflow_file, index=False, float_format='%.2f')

    def load_moneyflow(self):
        result=pd.DataFrame()
        moneyflow_file = "data/moneyflow.csv"
        if os.path.exists(moneyflow_file):
            result = pd.read_csv(moneyflow_file)
        return result

    def download_daily_line(self,stocks):
        k_line_file = "data/k_line.csv"
        result = pd.DataFrame()
        if os.path.exists(k_line_file):
            result = pd.read_csv(k_line_file)

        if not result.empty:
            result.sort_values(by=['trade_date'], axis=0, ascending=True,inplace=True)
            result.drop_duplicates(['ts_code'],inplace=True,keep='last')


        today = datetime.today()
        end_date = today.strftime('%Y%m%d')
        start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
        trade_cal = self.get_trade_cal(start_date,end_date);
        end_date = str(trade_cal[trade_cal['is_open'] == 1]['cal_date'].max())
        count = 0
        result_none = len(result) == 0

        for index, row in stocks.iterrows():
            stock = row ['ts_code']
            start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
            if result_none:
                count = count + 1
                df = self.download_k_line(stock, start_date, end_date)
                if not os.path.exists(k_line_file):
                    df.to_csv(k_line_file, index=False, float_format='%.2f')
                else:
                    df.to_csv(k_line_file, mode='a', header=None, index=False, float_format='%.2f')
            else:
                stock_k_line = result[result['ts_code'] == stock]
                start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
                # last_end_date = start_date
                if (len(stock_k_line) > 0):
                    last_end_date = (datetime.strptime(str(stock_k_line.iloc[-1]['trade_date']),'%Y%m%d') + timedelta(1)).strftime('%Y%m%d')
                    start_date = str(last_end_date )
                if start_date < end_date:
                    count = count + 1
                    df = self.download_k_line(stock,start_date, end_date)
                    if df is None :
                        continue

                    # df = pd.concat([stock_k_line,df],axis=0)
                    # df = df[df['trade_date'].astype(int) > int(last_end_date)]
                    df.to_csv(k_line_file, mode='a', header=None, index=False, float_format='%.2f')

            print('download k line stock=' + stock + " " + start_date + " " + end_date + " " + str(count))

            # if count % 20 == 0:
            #     time.sleep(1)
