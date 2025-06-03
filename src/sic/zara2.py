from datetime import UTC, datetime

from backtesting import Backtest
from backtesting.lib import TrailingStrategy
from preprocessor import Dataset

ETHUSDT = Dataset.load("binance.klines.um.ethusdt.15m", update=False, pandas=True)


class Zara2(TrailingStrategy):
    time = 5 * 60
    breakthrough_threshold = 0
    large_price_gap = 0.08

    small_price_gap = 0.05
    sl_threshold = 4

    def init(self):
        super().init()
        self.set_atr_periods(14)
        self.set_trailing_sl(self.sl_threshold)

    def cancel_all_orders(self):
        for o in self.orders:
            if not o.is_contingent:
                o.cancel()

    def next(self):
        super().next()

        timestamp = self.data.close_time[-1] + 1
        utc_time = datetime.fromtimestamp(timestamp / 1000, UTC)

        self.cancel_all_orders()
        high = max(self.data.High[-self.time :])
        low = min(self.data.Low[-self.time :])
        current_price_gap = (high - low) / low

        if utc_time.hour in [
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
        ]:
            high_leverage = True
        else:
            high_leverage = False

        if not self.position:
            if high_leverage:
                if current_price_gap <= self.small_price_gap:
                    self.buy(stop=high * (1 + self.breakthrough_threshold))
                    self.sell(stop=low * (1 - self.breakthrough_threshold))
                elif current_price_gap <= self.large_price_gap:
                    self.buy(size=0.5, stop=high * (1 + self.breakthrough_threshold))
                    self.sell(size=0.5, stop=low * (1 - self.breakthrough_threshold))
            else:
                if current_price_gap <= self.small_price_gap:
                    self.buy(size=0.5, stop=high * (1 + self.breakthrough_threshold))
                    self.sell(size=0.5, stop=low * (1 - self.breakthrough_threshold))
                elif current_price_gap <= self.large_price_gap:
                    self.buy(size=0.25, stop=high * (1 + self.breakthrough_threshold))
                    self.sell(size=0.25, stop=low * (1 - self.breakthrough_threshold))


bt = Backtest(
    ETHUSDT,
    Zara2,
    cash=100_000_000,
    commission=0.0002,  # Taker Trading Fee: 0.01%, Estimated Slippage: 0.01%
    margin=0.25,
    trade_on_close=False,
)

stats = bt.run()
equity_curve = stats["_equity_curve"]

print(stats)
bt.plot()

# Start                     2020-01-01 00:00:00
# End                       2025-06-03 06:15:00
# Duration                   1980 days 06:15:00
# Exposure Time [%]                    14.80016
# Equity Final [$]            29944145723.37813
# Equity Peak [$]              32964742990.6157
# Commissions [$]               2481184017.4235
# Return [%]                        29844.14572
# Buy & Hold Return [%]              1921.55661
# Return (Ann.) [%]                   188.03959
# Volatility (Ann.) [%]               171.99297
# CAGR [%]                            186.04332
# Sharpe Ratio                           1.0933
# Sortino Ratio                         10.4097
# Calmar Ratio                          6.39073
# Alpha [%]                         29875.47351
# Beta                                  -0.0163
# Max. Drawdown [%]                   -29.42382
# Avg. Drawdown [%]                    -3.13865
# Max. Drawdown Duration      150 days 15:00:00
# Avg. Drawdown Duration        5 days 09:20:00
# # Trades                                  496
# Win Rate [%]                         43.34677
# Best Trade [%]                       24.55027
# Worst Trade [%]                      -3.68881
# Avg. Trade [%]                        0.64257
# Max. Trade Duration           2 days 17:00:00
# Avg. Trade Duration           0 days 13:56:00
# Profit Factor                         2.02196
# Expectancy [%]                        0.69069
# SQN                                    2.4473
# Kelly Criterion                       0.18251
# _strategy                               Zara2
# _equity_curve                             ...
# _trades                            Size  E...
# dtype: object
