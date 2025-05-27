import numpy as np
from backtesting import Backtest
from backtesting.lib import Strategy
from preprocessor import Dataset

funding_rate = Dataset.load("binance.fundingrate.um.btcusdt", update=False)
um = Dataset.load("binance.klines.um.btcusdt.8h", update=False)
spot = Dataset.load("binance.klines.spot.btcusdt.8h", update=False)
um_1m = Dataset.load("binance.klines.um.btcusdt.1m", update=False)

um_1m_pandas = Dataset.load("binance.klines.um.btcusdt.1m", update=False, pandas=True)

print(funding_rate)
print(um)
print(spot)
print(um_1m)
print(um_1m_pandas)


class Basisguard(Strategy):
    def init(self):
        super().init()
        self.funding_rate = None
        self.um = None
        self.spot = None
        self.um_1m = None

    def trailing_pct(self, pct: float = 1):
        index = len(self.data) - 1
        for trade in self.trades:
            if trade.is_long:
                trade.sl = max(trade.sl or -np.inf, self.data.Close[index] * 0.99)
            else:
                trade.sl = min(
                    trade.sl or np.inf,
                    self.data.Close[index] * 1.01,
                )

    def cancel_all_orders(self):
        for o in self.orders:
            if not o.is_contingent:
                o.cancel()

    def next(self):
        super().next()
        self.trailing_pct()

        datetime = self.data.datetime[-1]

        if datetime.hour % 8 == 0 and datetime.minute == 0:
            print(datetime)
            funding_rate_idx = funding_rate["datetime"].search_sorted(
                datetime, side="right"
            )
            um_idx = um["datetime"].search_sorted(datetime, side="right")
            spot_idx = spot["datetime"].search_sorted(datetime, side="right")
            um_1m_idx = um_1m["datetime"].search_sorted(datetime, side="right")

            self.funding_rate = funding_rate.slice(0, funding_rate_idx)
            self.um = um.slice(0, um_idx)
            self.spot = spot.slice(0, spot_idx)
            self.um_1m = um_1m.slice(0, um_1m_idx)
            self.run_every_8h()

    def run_every_8h(self):
        if len(self.um) < 42:
            return

        self.cancel_all_orders()
        self.position.close()

        close_8h_std = self.um_1m["close"].rolling_std(window_size=480)
        avg_std = close_8h_std[-42:].mean()

        spreads = self.um["close"][-42:] - self.spot["close"][-42:]
        spread = spreads.last()
        spread_mean = spreads.mean()

        avg_funding_rate = self.funding_rate["funding_rate"].tail(30).mean()
        funding_rate = self.funding_rate["funding_rate"].last()

        price_change_8h = self.um["close"].last() - self.um["close"][-2]

        if spread < spread_mean and (
            funding_rate < 0 or funding_rate < avg_funding_rate
        ):
            self.buy()

        elif (
            spread > spread_mean and price_change_8h < 0 and close_8h_std[-1] > avg_std
        ):
            self.sell()


bt = Backtest(
    um_1m_pandas,
    Basisguard,
    cash=100_000_000,
    commission=0,  # 0.0002  # Gateio taker fee: 0.01%
    margin=1,
    trade_on_close=False,
)

stats = bt.run()
equity_curve = stats["_equity_curve"]

print(stats)
bt.plot()
