from backtesting import Backtest
from backtesting.lib import Strategy
from preprocessor import Dataset

funding_rate = Dataset.load("binance.fundingrate.um.btcusdt", update=False)
um = Dataset.load("binance.klines.um.btcusdt.1d", update=False)
spot = Dataset.load("binance.klines.spot.btcusdt.1d", update=False)

um_1d_pandas = Dataset.load("binance.klines.um.btcusdt.1d", update=False, pandas=True)

# 2025-06-20까지 데이터 자르기
um_1d_pandas = um_1d_pandas.loc[:"2025-06-20"]

print(type(um_1d_pandas.index))


class Spread2(Strategy):
    def init(self):
        super().init()
        self.funding_rate = None
        self.um = um
        self.spot = spot

    def cancel_all_orders(self):
        for o in self.orders:
            if not o.is_contingent:
                o.cancel()

    def next(self):
        super().next()
        datetime = self.data.datetime[-1]
        um_idx = um["datetime"].search_sorted(datetime, side="right")
        spot_idx = spot["datetime"].search_sorted(datetime, side="right")
        funding_rate_idx = funding_rate["datetime"].search_sorted(
            datetime, side="right"
        )

        self.um = um.slice(0, um_idx)
        self.spot = spot.slice(0, spot_idx)
        self.funding_rate = funding_rate.slice(0, funding_rate_idx)

        self.run_every_1d()

    def run_every_1d(self):
        if len(self.um) < 14:
            return
        self.cancel_all_orders()
        self.position.close()

        spread = self.um["close"][-1] / self.spot["close"][-1] - 1
        spread_mean = (self.um["close"][-14:] / self.spot["close"][-14:] - 1).mean()
        spread_std = (self.um["close"][-14:] / self.spot["close"][-14:] - 1).std()
        spread_z = (spread - spread_mean) / spread_std
        spread_3d_before = self.um["close"][-3] / self.spot["close"][-3] - 1

        last_funding_rate = self.funding_rate["funding_rate"].last()
        avg_funding_rate = self.funding_rate["funding_rate"].tail(30).mean()

        if (
            (spread_z < 0)
            & (spread_3d_before > spread)
            & (last_funding_rate < avg_funding_rate)
        ):
            self.buy()


bt = Backtest(
    um_1d_pandas,
    Spread2,
    cash=100_000_000,
    commission=0.0002,  # 0.0002  # Gateio taker fee: 0.01%
    margin=1,
    trade_on_close=False,
)

stats = bt.run()
equity_curve = stats["_equity_curve"]

print(stats)
bt.plot()

# Start                     2020-01-01 00:00:00
# End                       2025-06-20 00:00:00
# Duration                   1997 days 00:00:00
# Exposure Time [%]                    40.04004
# Equity Final [$]             1188859234.07105
# Equity Peak [$]              1202371040.25682
# Commissions [$]               100024554.61895
# Return [%]                         1088.85923
# Buy & Hold Return [%]              1334.45774
# Return (Ann.) [%]                    57.18377
# Volatility (Ann.) [%]                53.76491
# CAGR [%]                             57.21937
# Sharpe Ratio                          1.06359
# Sortino Ratio                         2.97277
# Calmar Ratio                          2.93345
# Alpha [%]                           747.96155
# Beta                                  0.25546
# Max. Drawdown [%]                    -19.4937
# Avg. Drawdown [%]                    -3.24004
# Max. Drawdown Duration      148 days 00:00:00
# Avg. Drawdown Duration       23 days 00:00:00
# # Trades                                  492
# Win Rate [%]                         55.89431
# Best Trade [%]                       16.95742
# Worst Trade [%]                     -14.18428
# Avg. Trade [%]                        0.54467
# Max. Trade Duration           1 days 00:00:00
# Avg. Trade Duration           1 days 00:00:00
# Profit Factor                         1.68542
# Expectancy [%]                        0.60435
# SQN                                   3.12616
# Kelly Criterion                       0.20614
# _strategy                     BasicBasisGuard
# _equity_curve                             ...
# _trades                         Size  Entr...
# dtype: object
