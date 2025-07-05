# =======================
# Strategy Parameters
# =======================

# - target_vol: 전략이 목표로 하는 연간 변동성 (0.075 = 7.5%)
# - max_leverage: 최대 허용 레버리지 비율 (1.0 = 100% 자본만 사용)
# - lookback_spread: 스프레드 평균/표준편차 계산에 사용하는 기간 (14일)
# - lookback_vol: 실현 변동성 계산에 사용하는 수익률 기간 (14일)
# - ann_factor: 실현 변동성 연율화 계수 (np.sqrt(365 * 0.4)) → 약 146일 연율화
# - funding_rate_lookback: 펀딩레이트 평균 계산 기간 (30일)
# - margin: 백테스트 시 사용되는 마진 비율 (0.2 = 5배 레버리지 허용)

# =======================

import numpy as np
import polars as pl
from backtesting import Backtest
from backtesting.lib import Strategy
from preprocessor import Dataset

funding_rate = Dataset.load("binance.fundingrate.um.btcusdt", update=False)
um = Dataset.load("binance.klines.um.btcusdt.1d", update=False)
spot = Dataset.load("binance.klines.spot.btcusdt.1d", update=False)
um_1d_pandas = Dataset.load("binance.klines.um.btcusdt.1d", update=False, pandas=True)


class BasicBasisGuard(Strategy):
    target_vol = 0.075
    max_leverage = 1.0

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
        if len(self.um) < 20:
            return
        self.cancel_all_orders()
        self.position.close()

        # 스프레드 기반 신호
        spread = self.um["close"][-1] / self.spot["close"][-1] - 1
        spread_mean = (self.um["close"][-14:] / self.spot["close"][-14:] - 1).mean()
        spread_std = (self.um["close"][-14:] / self.spot["close"][-14:] - 1).std()
        spread_z = (spread - spread_mean) / spread_std
        spread_3d_before = self.um["close"][-3] / self.spot["close"][-3] - 1

        last_funding_rate = self.funding_rate["funding_rate"].last()
        avg_funding_rate = self.funding_rate["funding_rate"].tail(30).mean()

        # 변동성 계산 시 .to_numpy()로 변환
        returns_np = self.um["close"].pct_change().tail(14).to_numpy()
        realized_vol = np.std(returns_np) * np.sqrt(365 * 0.4)
        leverage_ratio = min(self.target_vol / realized_vol, self.max_leverage)

        if (
            (spread_z < 0)
            & (spread_3d_before > spread)
            & (last_funding_rate < avg_funding_rate)
        ):
            self.buy(size=leverage_ratio)


bt = Backtest(
    um_1d_pandas,
    BasicBasisGuard,
    cash=100_000_000,
    commission=0.0002,  # 0.0002  # Gateio taker fee: 0.01%
    margin=0.2,
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
