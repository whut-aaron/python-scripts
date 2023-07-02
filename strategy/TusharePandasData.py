import backtrader as bt
class TusharePandasData(bt.feeds.PandasData):
    lines=('pe','pb','pct_chg')
    params=(
        ('pe',-1),
        ('pb', -1),
        ('pct_chg', -1),
    )