import numpy as np
import polars as pl
from backtesting import Backtest
from backtesting.lib import Strategy
from preprocessor import Dataset

funding_rate = Dataset.load("binance.fundingrate.um.btcusdt", update=False)
um = Dataset.load("binance.klines.um.btcusdt.1d", update=False)
spot = Dataset.load("binance.klines.spot.btcusdt.1d", update=False)
# um_1m = Dataset.load("binance.klines.um.btcusdt.1m", update=False)

um_1d_pandas = Dataset.load("binance.klines.um.btcusdt.1d", update=False, pandas=True)


print(funding_rate)
# print(um)
# print(spot)
# print(um_1m)
# print(um_1m_pandas)


class BasicBasisGuard(Strategy):
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
            # print(self.data.datetime[-1], spread, spread_mean, spread_3d_before)
            self.buy()
        # elif spread_z > 0.5:
        #     self.sell()

    # def run_every_8h(self):
    #     if len(self.um) < 42:
    #         return

    #     self.cancel_all_orders()
    #     self.position.close()

    #     # close_8h_std = self.um_1m["close"].rolling_std(window_size=480)
    #     # avg_std = close_8h_std[-42:].mean()
    #     # polars
    #     # 8시간 bin으로 표준편차 계산
    #     close_8h_std = (
    #         self.um_1m.group_by_dynamic(
    #             index_column="datetime", every="8h", closed="right"
    #         ).agg([pl.col("close").std().alias("close_std")])
    #     )["close_std"]

    #     # 마지막 42개 값 평균
    #     avg_std = close_8h_std[-42:].mean()

    #     spreads = self.um["close"][-42:] / self.spot["close"][-42:] - 1
    #     spread = spreads.last()
    #     spread_mean = spreads.mean()

    #     avg_funding_rate = self.funding_rate["funding_rate"].tail(30).mean()
    #     last_funding_rate = self.funding_rate["funding_rate"].last()

    #     price_change_8h = self.um["close"].last() - self.um["close"][-2]

    #     if spread < spread_mean:
    #         #     and (
    #         #     (last_funding_rate < 0) or (last_funding_rate < avg_funding_rate)
    #         # ):
    #         print(spread, spread_mean)
    #         print(spreads)
    #         self.buy()

    #     # elif (
    #     #     (spread > spread_mean) and (price_change_8h < 0) and (close_8h_std[-1] > avg_std)
    #     # ):
    #     #     self.sell()


bt = Backtest(
    um_1d_pandas,
    BasicBasisGuard,
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
# Equity Final [$]             1173157842.89701
# Equity Peak [$]              1202371040.25682
# Commissions [$]               100024554.61895
# Return [%]                         1073.15784
# Buy & Hold Return [%]              1334.45774
# Return (Ann.) [%]                    56.80247
# Volatility (Ann.) [%]                53.64496
# CAGR [%]                              56.8378
# Sharpe Ratio                          1.05886
# Sortino Ratio                         2.95164
# Calmar Ratio                          2.91389
# Alpha [%]                           732.14995
# Beta                                  0.25554
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


# 2025-06-21
