import pandas as pd
class Context:
    def __init__(self):
        self.balance = 1000000
        self.orders = {}
        self.positions = pd.DataFrame()
