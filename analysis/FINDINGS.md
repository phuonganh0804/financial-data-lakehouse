# Findings: big tech, crypto and the rate cycle (2021–2026)

Five questions answered with SQL over the gold mart (`mart_returns_vs_macro`),
covering **2021-01-01 to 2026-09-30**: about 5¾ years spanning zero rates, the
2022–23 hiking cycle, a plateau, the 2024–25 cuts and a hold in 2026.

**Universe:** 8 US tech megacaps (AAPL, MSFT, AMZN, NVDA, TSLA, GOOGL, META,
ADBE), the Nasdaq-100 ETF **QQQ** as a benchmark, and **BTC/ETH**. Macro data
comes from FRED. Each finding links to its query in [`sql/`](sql/).

New to the finance terms? See the [glossary](#glossary) at the end. Every
metric used here is defined there in plain language.

## Summary

1. **The index beat most of its stars on risk-adjusted terms.** Only NVDA and
   GOOGL had a better Sharpe ratio than simply holding QQQ.
2. **The "average tech stock" is a story about NVDA.** The 8 stocks averaged
   +305%, but the typical (median) one returned +141%, about the same as QQQ's +136%.
3. **The 2022 crash was synchronised.** 8 of 11 assets hit their worst drawdown
   between June and December 2022, at the height of the rate hikes.
4. **Crypto is only loosely tied to tech.** Weekly BTC–QQQ correlation ranged from
   0.14 to 0.51 and showed no steady upward trend.
5. **In 2021–22, tech lost about a quarter to a third of its real value.** CPI rose
   13.8%, so a 15% nominal loss on QQQ was a 25% loss in purchasing power.

---

## 1. Who paid best for the risk taken?

[`01_risk_adjusted_performance.sql`](sql/01_risk_adjusted_performance.sql). The
Sharpe ratio is excess return over the Fed funds rate, divided by volatility.

| Asset | CAGR | Volatility | Sharpe | Max drawdown | Trough |
|---|---|---|---|---|---|
| NVDA | 64.9% | 50.6% | **1.22** | −66.4% | 2022-10-14 |
| GOOGL | 27.0% | 31.3% | 0.76 | −44.3% | 2022-11-03 |
| **QQQ (benchmark)** | 16.2% | **22.4%** | **0.58** | **−35.6%** | 2022-12-28 |
| AAPL | 17.4% | 27.7% | 0.51 | −33.4% | 2025-04-08 |
| MSFT | 15.7% | 27.3% | 0.46 | −37.6% | 2022-11-03 |
| META | 18.6% | 43.5% | 0.35 | −76.7% | 2022-11-03 |
| BTC | 20.3% | 57.1% | 0.30 | −76.6% | 2022-11-21 |
| ETH | 25.2% | 77.2% | 0.28 | −79.3% | 2022-06-18 |
| AMZN | 7.7% | 35.1% | 0.13 | −56.1% | 2022-12-28 |
| TSLA | 7.4% | 59.0% | 0.07 | −73.6% | 2023-01-03 |
| ADBE | −12.0% | 36.5% | −0.42 | −71.9% | 2026-06-25 |

**So what:** QQQ had the lowest volatility of any asset here, the second-shallowest
drawdown (after AAPL), and a better Sharpe ratio than 6 of the 8 stocks.
Picking winners paid off spectacularly in one case (NVDA) and was worse than the
index in most others. Crypto delivered tech-like returns with 2–3× the
volatility, which is a poor trade on a risk-adjusted basis.

## 2. Did the stocks beat the index?

[`02_stocks_vs_benchmark.sql`](sql/02_stocks_vs_benchmark.sql). Beta is how much
a stock moves for each 1% move in QQQ.

| Stock | Beta | Correlation | CAGR | vs. QQQ (16.2%) |
|---|---|---|---|---|
| NVDA | 1.76 | 0.78 | 64.9% | **+48.7 pp** |
| GOOGL | 0.98 | 0.70 | 27.0% | +10.8 pp |
| META | 1.26 | 0.65 | 18.6% | +2.4 pp |
| AAPL | 0.88 | 0.71 | 17.4% | +1.3 pp |
| MSFT | 0.89 | 0.73 | 15.7% | −0.5 pp |
| AMZN | 1.15 | 0.73 | 7.7% | −8.5 pp |
| TSLA | 1.68 | 0.64 | 7.4% | −8.7 pp |
| ADBE | 0.93 | 0.57 | −12.0% | −28.2 pp |

**So what:** it's an even split. 4 stocks beat the index, 1 matched it and 3
lagged. **High beta didn't mean high return:** NVDA and TSLA both amplify the
index by about 1.7×, but one returned 65% a year and the other 7%. Beta measures
how much a stock moves with the market, not whether it rises.

> **Average vs. median.** The equal-weighted average of the 8 stocks' total
> returns is +305%, but NVDA alone (+1,649%) pulls it up. The median stock
> returned **+141%**, roughly the same as QQQ's **+136%**. Reporting only the
> average would badly overstate the typical outcome.

## 3. How did rate regimes change the picture?

[`03_rate_regimes.sql`](sql/03_rate_regimes.sql). Regimes are taken from the Fed
funds path in the data. Returns are **monthly**, and groups are equal-weighted
portfolios rebalanced each month.

| Regime | Months | 8 tech stocks | QQQ | Crypto | QQQ volatility |
|---|---|---|---|---|---|
| Zero rates (2021-01 → 2022-02) | 14 | +23.2% | +10.5% | **+164.2%** | 16.4% |
| Hiking to 5.33% (2022-03 → 2023-07) | 17 | +26.0% | +10.6% | **−32.8%** | **27.7%** |
| Plateau at 5.33% (2023-08 → 2024-08) | 13 | +35.2% | +24.1% | +67.0% | 16.5% |
| Cutting to 3.64% (2024-09 → 2025-12) | 16 | +37.2% | +29.0% | +38.8% | **14.1%** |
| Hold at ~3.6% (2026-01 → 2026-09) | 9 | +4.9% | **+20.4%** | −6.7% | 24.9% |

**So what:**
- **Crypto was the most rate-sensitive asset.** It returned +164% with free money
  and −33% while rates rose, a swing far larger than tech's.
- **Hiking raised risk more than it hurt returns.** QQQ still gained about 11%
  over the 17-month hiking cycle (2022's losses were recovered in early 2023),
  but its volatility rose by about 70%, from 16% to 28%.
- **Rate cuts were the calmest period.** QQQ had its lowest volatility (14%) and
  69% of months were positive.
- **In 2026 the index has pulled ahead of its megacaps** (+20% vs. +5%), so the
  rest of the Nasdaq-100 is now driving returns.

## 4. Has crypto started moving like a tech stock?

[`04_crypto_vs_tech_correlation.sql`](sql/04_crypto_vs_tech_correlation.sql).
Correlation of weekly returns, because crypto trades 7 days a week.

| Year | BTC vs. QQQ | ETH vs. QQQ | BTC vs. ETH |
|---|---|---|---|
| 2021 | 0.31 | 0.31 | 0.73 |
| 2022 | **0.42** | **0.50** | 0.91 |
| 2023 | 0.20 | 0.21 | 0.88 |
| 2024 | **0.14** | 0.30 | 0.74 |
| 2025 | **0.51** | 0.44 | 0.75 |
| 2026 (to Sep) | 0.29 | 0.20 | 0.95 |

**So what:** no. The link is moderate and unstable rather than rising. It
tightened in the stress years (2022, 2025), when everything sold off together,
and loosened in calm years. BTC and ETH move closely with each other (0.73–0.95)
far more than with tech. For diversification, that's a mixed message: crypto
diversifies least exactly when you need it most.

## 5. What did investors earn after inflation?

[`05_inflation_real_returns.sql`](sql/05_inflation_real_returns.sql). Real return
is the nominal return deflated by the change in CPI.

| Period | Group | Nominal | CPI inflation | **Real** |
|---|---|---|---|---|
| 2021–22 inflation surge | 8 tech stocks | −20.8% | 13.8% | **−30.4%** |
| | QQQ | −15.1% | 13.8% | **−25.4%** |
| | Crypto | +9.8% | 13.8% | **−3.5%** |
| Full window (2021-01 → 2026-09) | 8 tech stocks | +305.1% | 27.2% | +218.5% |
| | QQQ | +135.8% | 27.2% | **+85.4%** |
| | Crypto | +226.9% | 27.2% | +157.0% |

**So what:** in 2021–22, inflation turned a 15% loss on the index into a 25%
loss of purchasing power. Over the full window CPI rose 27%, which removed about
a third of QQQ's nominal gain (+136% → +85%). Crypto's positive nominal return in
2021–22 depends heavily on the start date: it came from the 2021 boom, and BTC
alone lost 43% over the same period.

---

## Caveats

- **Survivorship and selection bias.** The 8 stocks are today's household names,
  chosen with hindsight. A real investor in 2021 wouldn't have known to pick them,
  so their performance overstates what "investing in tech" delivered. QQQ is the
  fairer yardstick.
- **One rate cycle.** Each regime happened once, so the comparisons describe what
  happened, not a repeatable rule. Other things changed at the same time,
  including the AI boom (2023–24) and tariff shocks (April 2025).
- **Price return only.** Prices are split-adjusted but exclude dividends. That
  matters little for these low-yield stocks, but it slightly understates AAPL,
  MSFT and QQQ.
- **Equal weights.** Group figures weight each stock equally, unlike QQQ, which
  weights by market value.
- **Macro granularity.** CPI is monthly and GDP quarterly, so macro comparisons
  use monthly returns. Daily rows would repeat the same CPI value about 21 times
  and overstate the evidence.

## Data issue found during this analysis

The mart's `real_daily_return` deflates each **row** by 1/365 of the yearly
inflation rate. Crypto has a row every calendar day, so that's correct. Equities
only have about 252 trading-day rows a year, so about 30% of inflation is never
removed:

| 2021–22 | Nominal | Mart's real, before fix | Correct real (CPI ratio) |
|---|---|---|---|
| QQQ | −15.1% | −22.0% | **−25.4%** |
| BTC | −42.8% | −49.4% | −49.7% ✅ |

**Fixed:** the mart now deflates by the calendar days each return covers
(`power(1 + cpi_yoy, days_covered / 365.0)`, so 3 days over a weekend). QQQ's
compounded real return for 2021–22 is now **−24.9%**, within 0.5 pp of the exact
CPI-ratio figure. The remaining gap is because the mart approximates with a
daily year-over-year rate. This analysis still uses the exact CPI ratio.

## Next questions

- **Is the rate story really an AI story?** Split 2023–24 into NVDA versus the
  other 7 stocks, to separate the AI boom from the rate plateau.
- **Rolling beta.** Did NVDA's beta to the index rise as it became a larger part
  of it?
- **Real yield vs. tech valuations.** Use `DGS10 − T10YIE` (the real 10-year
  yield) on a monthly basis against QQQ returns. This is the usual explanation for
  why rising rates hurt long-duration growth stocks.

## Glossary

The finance concepts used above, in plain language, with where each appears.

| Term | Plain meaning | Example from this analysis | Used in |
|---|---|---|---|
| **Return** | The % change in price over a period. | QQQ returned +135.8% from 2021 to Sep 2026. | All |
| **Log return** | A form of return that can be **added up** across days instead of multiplied. It's a calculation convenience, and converts back with `exp()`. | `exp(sum(log_return)) - 1` gives the total return. | All queries |
| **CAGR** (compound annual growth rate) | The steady yearly growth rate that would produce the same end result. | NVDA's +1,649% over 5¾ years is 64.9% a year. | Q1, Q2 |
| **Volatility** | How much returns swing around, measured as the standard deviation and scaled to a year. Higher means a bumpier ride. | QQQ 22%, BTC 57%. | Q1, Q3 |
| **Risk-free rate** | What you could earn with no risk. Here, the Fed funds rate. | It averaged about 3% over the window. | Q1 |
| **Sharpe ratio** | Return above the risk-free rate, divided by volatility: "how much was I paid per unit of risk?" Above 1 is excellent; below 0 means you'd have done better in cash. | NVDA 1.22, QQQ 0.58, ADBE −0.42. | Q1 |
| **Maximum drawdown** | The worst fall from a previous peak before recovering: "how bad did it get?" | ETH −79%: $100 at the peak became $21. | Q1 |
| **Benchmark** | A standard to compare against. Here QQQ (the Nasdaq-100 index fund) answers "would I have done better just buying the index?" | 4 of 8 stocks beat QQQ. | Q2 |
| **Beta** | How much an asset moves when the benchmark moves 1%. 1 means in step, above 1 amplifies, below 1 dampens. | NVDA 1.76: about 1.8% for every 1% move in QQQ. | Q2 |
| **Correlation** | How closely two series move together, from −1 (opposite) through 0 (unrelated) to +1 (in lockstep). | BTC vs. QQQ: 0.14–0.51 depending on the year. | Q2, Q4 |
| **Nominal vs. real return** | Nominal is the number you see. Real is after inflation: what the money actually buys. | QQQ 2021–22: −15% nominal, −25% real. | Q5 |
| **CPI** (Consumer Price Index) | The US price level for a basket of everyday goods. Its yearly change is the inflation rate. | It rose 13.8% over 2021–22. | Q5 |
| **Rate regime** | A period defined by what the central bank is doing with interest rates: holding near zero, hiking, holding high or cutting. | Hiking: Mar 2022 → Jul 2023, 0.08% → 5.33%. | Q3 |
| **Equal-weighted** | Each asset counts the same in a group average, regardless of size. Indexes like QQQ are instead weighted by company size. | The "8 tech stocks" group. | Q3, Q5 |
| **Survivorship bias** | Studying only the winners that are still around, which makes the past look better than it was for someone choosing at the time. | Today's megacaps were picked with hindsight. | Caveats |

**Why interest rates matter to asset prices:** when safe savings pay 5%,
risky assets have to offer more to be worth holding, so their prices tend to
come under pressure when rates rise. Growth companies, whose profits are
expected far in the future, and speculative assets like crypto are usually the
most sensitive. Q3 tests how well that story held up in 2021–2026.
