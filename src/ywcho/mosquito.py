from zoneinfo import ZoneInfo

from backtesting import Backtest
from backtesting.lib import Strategy
from preprocessor import Dataset

um = Dataset.load("binance.klines.um.btcusdt.5m", update=False, pandas=True)
spot = Dataset.load("binance.klines.spot.btcusdt.5m", update=False, pandas=True)

um["spot_close"] = spot["Close"]

um = um[um.index >= "2023-06-28"]
spot = spot[spot.index >= "2023-06-28"]


print(um)
print(spot)


class Mosquito(Strategy):
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
            if len(self.data.spot_close) > 288:
                today_price = self.data.spot_close[
                    -1 - 6
                ]  # 30분전 데이터 (15시 30분 EST)
                yesterday_price = self.data.spot_close[
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
    um,
    Mosquito,
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
# End                       2025-05-18 01:20:00
# Duration                    690 days 01:20:00
# Exposure Time [%]                     1.32336
# Equity Final [$]              226397539.92786
# Equity Peak [$]               235507194.75568
# Commissions [$]               150836283.97214
# Return [%]                          126.39754
# Buy & Hold Return [%]               236.78444
# Return (Ann.) [%]                    53.97504
# Volatility (Ann.) [%]                 46.2522
# CAGR [%]                             54.06602
# Sharpe Ratio                          1.16697
# Sortino Ratio                         3.00179
# Calmar Ratio                          1.84239
# Alpha [%]                           123.65439
# Beta                                  0.01158
# Max. Drawdown [%]                   -29.29623
# Avg. Drawdown [%]                     -2.5745
# Max. Drawdown Duration      155 days 00:55:00
# Avg. Drawdown Duration        8 days 09:24:00
# # Trades                                  526
# Win Rate [%]                          59.5057
# Best Trade [%]                        2.74062
# Worst Trade [%]                      -1.33192
# Avg. Trade [%]                        0.07356
# Max. Trade Duration           0 days 00:20:00
# Avg. Trade Duration           0 days 00:20:00
# Profit Factor                         1.88448
# Expectancy [%]                        0.07417
# SQN                                   4.63498
# Kelly Criterion                        0.2734
# _strategy                      TrendFollowing
# _equity_curve                             ...
# _trades                         Size  Entr...
