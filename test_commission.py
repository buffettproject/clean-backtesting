import pandas as pd
from backtesting import Backtest
from backtesting.lib import Strategy

df = pd.DataFrame(
    {
        "Open": [100] * 20,
        "High": [100] * 20,
        "Low": [100] * 20,
        "Close": [100] * 20,
        "Volume": [1] * 20,
    }
)


class Basisguard(Strategy):
    def init(self):
        super().init()
        self.funding_rate = None
        self.um = None
        self.spot = None
        self.um_1m = None

    def next(self):
        super().next()
        # print(self.data)
        print(self.position.size)
        if self.position.size == 0:
            print("buy")
            self.sell()
        elif self.position.size < 0:
            print("sell")
            self.position.close()
        else:
            raise ValueError("position size is not 0 or negative")


bt = Backtest(
    df,
    Basisguard,
    cash=100_000_000,
    commission=0.0001,
    margin=1,
    trade_on_close=False,
)

stats = bt.run()
equity_curve = stats["_equity_curve"]

print(stats)
bt.plot()
