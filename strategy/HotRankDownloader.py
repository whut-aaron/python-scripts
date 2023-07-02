from aktool import  aktool
from datetime import datetime,timedelta
import os

rank_file= 'data/hot_rank.csv'
if __name__ == '__main__':
    aktool = aktool()
    today = datetime.today()
    trade_date = today.strftime('%Y%m%d')
    rank = aktool.download_hot_rank()
    rank['trade_date'] = trade_date
    rank['code'] = rank['code'].str.slice(2,8) + '.' + rank['code'].str.slice(0,2)

    if os.path.exists(rank_file):
        rank.to_csv(rank_file, mode='a', header=None, index=False, float_format='%.2f')
    else:
        rank.to_csv(rank_file, index=False, float_format='%.2f')

    print(rank.head(20))
