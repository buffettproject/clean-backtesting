# Volatility Breakout strategy
# Filter entry conditions using spot-futures price gap

# 2025/01/16: First version
import pandas as pd
from backtesting import Backtest
from backtesting.lib import Strategy
from preprocessor import Dataset

BTCUSDT_UM_1MIN = Dataset.load(
    "binance.klines.um.btcusdt.1m", update=False, pandas=True
)
BTCUSDT_UM = Dataset.load("binance.klines.um.btcusdt.1d", update=False, pandas=True)
BTCUSDT_SPOT = Dataset.load("binance.klines.spot.btcusdt.1d", update=False, pandas=True)

BTCUSDT_UM["long_range"] = BTCUSDT_UM["High"] - BTCUSDT_UM["Open"]
BTCUSDT_UM["short_range"] = BTCUSDT_UM["Open"] - BTCUSDT_UM["Low"]
BTCUSDT_UM["spread"] = BTCUSDT_UM["Close"] - BTCUSDT_SPOT["Close"]
BTCUSDT_UM["spread_14_avg"] = BTCUSDT_UM["spread"].rolling(14).mean()

BTCUSDT_UM_1MIN.index = BTCUSDT_UM_1MIN.index + pd.Timedelta(minutes=1)
BTCUSDT_UM_1MIN.rename_axis("close_time", inplace=True)

BTCUSDT_UM.index = BTCUSDT_UM.index + pd.Timedelta(days=1)
BTCUSDT_UM.rename_axis("close_time", inplace=True)

BTCUSDT_UM_1MIN["high_1d"] = BTCUSDT_UM["High"]
BTCUSDT_UM_1MIN["low_1d"] = BTCUSDT_UM["Low"]
BTCUSDT_UM_1MIN["open_1d"] = BTCUSDT_UM["Open"]
BTCUSDT_UM_1MIN["close_1d"] = BTCUSDT_UM["Close"]
BTCUSDT_UM_1MIN["volume_1d"] = BTCUSDT_UM["Volume"]
BTCUSDT_UM_1MIN["spread_1d"] = BTCUSDT_UM["spread"]
BTCUSDT_UM_1MIN["spread_14_avg_1d"] = BTCUSDT_UM["spread_14_avg"]
BTCUSDT_UM_1MIN["long_range_1d"] = BTCUSDT_UM["long_range"]
BTCUSDT_UM_1MIN["short_range_1d"] = BTCUSDT_UM["short_range"]
BTCUSDT_UM_1MIN["new_day"] = False
BTCUSDT_UM_1MIN.loc[BTCUSDT_UM_1MIN.index.isin(BTCUSDT_UM.index), "new_day"] = True


class TrendFollowing(Strategy):
    def init(self):
        super().init()

    def cancel_all_orders(self):
        for o in self.orders:
            if not o.is_contingent:
                o.cancel()

    def next(self):
        super().next()

        new_day = self.data.new_day[-1]

        if new_day:
            self.cancel_all_orders()
            for trade in self.trades:
                trade.close()

            close = self.data.Close[-1]
            long_range = self.data.long_range_1d[-1]
            spread = self.data.spread_1d[-1]
            spread_14_avg = self.data.spread_14_avg_1d[-1]

            if spread < spread_14_avg:
                self.buy(stop=close + long_range)


bt = Backtest(
    BTCUSDT_UM_1MIN,
    TrendFollowing,
    cash=100_000_000,
    commission=0.0001 + 0.0001,  # Taker Trading Fee: 0.01%, Estimated Slippage: 0.01%
    margin=1,
    trade_on_close=False,
)

stats = bt.run()
equity_curve = stats["_equity_curve"]

print(stats)
bt.plot()

# Start                     2020-01-01 00:01:00
# End                       2025-06-01 08:57:00
# Duration                   1978 days 08:56:00
# Exposure Time [%]                    17.94765
# Equity Final [$]             2608319877.08467
# Equity Peak [$]              2777540911.20696
# Commissions [$]               239569555.95532
# Return [%]                         2508.31988
# Buy & Hold Return [%]              1351.29232
# Return (Ann.) [%]                    82.48567
# Volatility (Ann.) [%]                55.47461
# CAGR [%]                              82.5205
# Sharpe Ratio                          1.48691
# Sortino Ratio                         5.94558
# Calmar Ratio                          3.09945
# Alpha [%]                          2203.33779
# Beta                                   0.2257
# Max. Drawdown [%]                   -26.61303
# Avg. Drawdown [%]                    -0.89804
# Max. Drawdown Duration      191 days 21:14:00
# Avg. Drawdown Duration        1 days 14:31:00
# # Trades                                  513
# Win Rate [%]                         57.50487
# Best Trade [%]                       16.15303
# Worst Trade [%]                       -9.7856
# Avg. Trade [%]                        0.67804
# Max. Trade Duration           1 days 00:00:00
# Avg. Trade Duration           0 days 16:36:00
# Profit Factor                         2.07822
# Expectancy [%]                         0.7217
# SQN                                   3.58927
# Kelly Criterion                       0.23628
# _strategy                      TrendFollowing
# _equity_curve                             ...
# _trades                         Size  Entr...
# dtype: object
