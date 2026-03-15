# Participation-Weighted Fair Value in Limit Order Books

The standard microprice weights bid and ask sides by order **quantity**. This project shows that weighting by order **count** (the number of independent orders) produces a 30 to 38% more accurate fair value estimator, validated across 185 instruments with p < 0.0001.

The intuition: 10,000 shares on the bid from 200 independent orders represents stronger consensus about fair value than 10,000 shares from 2 large orders. Count captures participation diversity. Quantity does not.

This is consistent with Rosu (2009, *Review of Financial Studies*), who proved that in equilibrium, bid and ask prices depend only on the **numbers** of buy and sell orders, not their sizes.

## Key Results

**Fair value comparison** across 183 NSE equities and 2 index futures (NIFTY, BANKNIFTY), 5-level LOB depth, 5 trading days, strictly causal evaluation with non-overlapping samples:

| Estimator | IC at H=10 | IC at H=20 | IC at H=50 | IC at H=100 |
|-----------|-----------|-----------|-----------|------------|
| Quantity L1 (standard microprice) | +0.112 | +0.077 | +0.047 | +0.042 |
| **Count L1** | **+0.129** | **+0.096** | **+0.062** | +0.045 |
| Quantity L5 (multi-level) | +0.144 | +0.112 | +0.072 | +0.056 |
| **Count L5 (multi-level)** | **+0.189** | **+0.149** | **+0.099** | **+0.074** |

IC = Spearman rank correlation between FV displacement and forward return. Count-based wins at every horizon.

**Statistical significance:** Paired t-test across 185 instruments at L5: t = 18.8, p < 0.0001 at H=10. Count wins on 94% of instruments. Wilcoxon signed-rank test agrees at every horizon.

**Additional findings:**

- Signed order count flow predicts returns correctly (IC = +0.032). Signed volume flow predicts the wrong direction (IC = -0.021). Volume mean-reverts. Count carries momentum.
- The advantage is strongest for illiquid stocks (+0.043 IC for wide-spread stocks vs +0.022 for tight-spread).
- Survives a fragmentation control: works even when average order size is large (IC = +0.097), ruling out pure order splitting as the explanation.
- Extends to NIFTY/BANKNIFTY futures at deeper book levels (L5), but not at L1 where HFT order cycling dominates.
- Consistent on each test day independently (count wins on 63 to 75% of instruments per day).
- GRU with order flow features (including order count imbalance) achieves IC = +0.087 for return prediction, +36% over Ridge baseline on standard LOB features.
- Cross-sectional long/short ranked by order count imbalance: +1.48 bps at 30s with 86.6% hit rate.

## Repository Structure

```
lob-microstructure/
    README.md
    LICENSE
    capture/                    # Production tick capture system
        auth.py                 # Daily Kite Connect authentication
        config.py               # Central configuration
        constituents.py         # NIFTY 50 constituent fetcher
        holidays.py             # NSE holiday calendar
        instruments.py          # Instrument resolution and subscription
        main.py                 # Main orchestrator
        monitor.py              # Real-time health monitoring
        postmarket.py           # Post-market consolidation and compression
        query.py                # Data inspection utilities
        storage.py              # Double-buffered Parquet tick writer
        ticker.py               # WebSocket connection manager
        requirements.txt
    research/                   # Analysis notebooks
        research_notebook.ipynb         # All statistical tests and robustness checks
        research_figures_gru.ipynb      # Publication figures and GRU training
    proposal/
        proposal.pdf            # Research proposal
        proposal.tex            # LaTeX source
```

## Data

Anonymised 5-level LOB data for 183 equities and 2 index futures. 5 trading days (March 2026). Approximately 11.5 million ticks total.

**[Download from Google Drive](https://drive.google.com/file/d/1YtHvKLWu4VzLXHv9PodVGbzLPci6LOde/view?usp=sharing)**

The Drive folder contains:
- `eq_001.parquet` through `eq_183.parquet` (anonymised equities)
- `fut_001.parquet`, `fut_002.parquet` (anonymised NIFTY and BANKNIFTY futures)
- `README.json` (field descriptions and anonymisation details)

Anonymisation: stock identities removed, timestamps replaced with seconds from market open, prices normalised by Day 1 opening mid-price. All order book structure (5-level depth, quantities, order counts) is preserved. Relative price relationships (spreads, returns, imbalances) are unchanged.

### Data schema

Each parquet file contains:

| Field | Description |
|-------|-------------|
| `day` | Integer day index (1 to 5) |
| `seconds_from_open` | Float, seconds since 9:15 IST |
| `tick_index` | Sequential integer within file |
| `bid_price_1..5` | Normalised bid prices at levels 1 to 5 |
| `ask_price_1..5` | Normalised ask prices at levels 1 to 5 |
| `bid_qty_1..5` | Raw bid quantities |
| `ask_qty_1..5` | Raw ask quantities |
| `bid_orders_1..5` | Raw bid order counts |
| `ask_orders_1..5` | Raw ask order counts |
| `last_price` | Normalised last trade price |
| `volume` | Raw cumulative volume |

## Reproducing Results

1. Download the anonymised data from the Drive link above
2. Open `research/research_notebook.ipynb` in Google Colab
3. Update `DATA_DIR` to point to your data folder
4. Run all cells

The notebook reproduces: the fair value shootout (Table 1), statistical significance tests, per-day robustness, equities vs futures breakdown, spread conditioning, fragmentation control, queue dynamics, and signed flow analysis.

For figures and the GRU, run `research/research_figures_gru.ipynb` (requires GPU runtime).

## References

- Stoikov (2018), "The micro-price: a high-frequency estimator of future prices," *Quantitative Finance* 18(12)
- Rosu (2009), "A dynamic model of the limit order book," *Review of Financial Studies* 22(11)
- Cont, Kukanov & Stoikov (2014), "The price impact of order book events," *Journal of Financial Econometrics* 12(1)
- Kolm, Turiel & Westray (2023), "Deep order flow imbalance: Extracting alpha at multiple horizons from the limit order book," *Mathematical Finance* 33(4)

## License

Code: MIT. Data: research use only, derived from NSE India market data.
