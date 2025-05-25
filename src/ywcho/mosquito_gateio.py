from zoneinfo import ZoneInfo

from backtesting import Backtest
from backtesting.lib import Strategy
from preprocessor import Dataset

um = Dataset.load("binance.klines.um.btcusdt.5m", update=False, pandas=True)
spot = Dataset.load("binance.klines.spot.btcusdt.5m", update=False, pandas=True)
gateio_um = Dataset.load("gateio.klines.um.btc_usdt.5m", update=False, pandas=True)

gateio_um["Open"] = gateio_um["Open"].astype("float64")
gateio_um["High"] = gateio_um["High"].astype("float64")
gateio_um["Low"] = gateio_um["Low"].astype("float64")
gateio_um["Close"] = gateio_um["Close"].astype("float64")
gateio_um["Volume"] = gateio_um["Volume"].astype("float64")

spot_renamed = spot.add_prefix("spot_")
um_renamed = um.add_prefix("um_")

merged = gateio_um.join(spot_renamed, how="left").join(um_renamed, how="left")

merged = merged[merged.index >= "2023-06-28"]


print(merged)


print(merged.columns)
print(merged.dtypes)


class MosquitoGateio(Strategy):
    def init(self):
        super().init()

    def is_est_4pm(self) -> bool:
        est_dt = self.data.datetime[-1].astimezone(ZoneInfo("America/New_York"))
        return est_dt.hour == 16 and est_dt.minute == 0

    def is_est_4pm_20m(self) -> bool:
        est_dt = self.data.datetime[-1].astimezone(ZoneInfo("America/New_York"))
        return est_dt.hour == 16 and est_dt.minute == 20

    def next(self):
        super().next()

        if self.is_est_4pm():
            if len(self.data.spot_Close) > 288:
                today_price = self.data.spot_Close[
                    -1 - 6
                ]  # 30분전 데이터 (15시 30분 EST)
                yesterday_price = self.data.spot_Close[
                    -1 - 288
                ]  # 24시간전 데이터 (16시 EST)
                price_change = (today_price - yesterday_price) / yesterday_price

                if price_change > 0.005:
                    self.sell()
                elif price_change < -0.005:
                    self.buy()

        elif self.is_est_4pm_20m():
            self.position.close()


bt = Backtest(
    merged,
    MosquitoGateio,
    cash=100_000_000,
    commission=0.0002,  # Gateio taker fee: 0.01%
    margin=0.2,
    trade_on_close=False,
)

stats = bt.run()
equity_curve = stats["_equity_curve"]

print(stats)
bt.plot()


# Start                     2023-06-28 00:00:00
# End                       2025-05-19 08:05:00
# Duration                    691 days 08:05:00
# Exposure Time [%]                      1.3209
# Equity Final [$]              237036814.19964
# Equity Peak [$]               246535438.89686
# Commissions [$]               153520801.40036
# Return [%]                          137.03681
# Buy & Hold Return [%]               235.85895
# Return (Ann.) [%]                    57.65186
# Volatility (Ann.) [%]                47.23274
# CAGR [%]                             57.72072
# Sharpe Ratio                          1.22059
# Sortino Ratio                         3.23031
# Calmar Ratio                          1.96058
# Alpha [%]                           134.36357
# Beta                                  0.01133
# Max. Drawdown [%]                   -29.40544
# Avg. Drawdown [%]                    -2.44166
# Max. Drawdown Duration      155 days 01:00:00
# Avg. Drawdown Duration        7 days 22:10:00
# # Trades                                  526
# Win Rate [%]                         60.45627
# Best Trade [%]                        2.76509
# Worst Trade [%]                      -1.34043
# Avg. Trade [%]                        0.07529
# Max. Trade Duration           0 days 00:20:00
# Avg. Trade Duration           0 days 00:20:00
# Profit Factor                         1.91331
# Expectancy [%]                        0.07591
# SQN                                   4.77373
# Kelly Criterion                       0.28548
# _strategy                      TrendFollowing
# _equity_curve                             ...
# _trades                         Size  Entr...
