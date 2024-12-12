<p align="center">
  <img src="images/logo.png" alt="BackBoard Logo">
</p>

## BackBoard

BackBoard is a tool for testing stock trading strategies because we’re doubtful any of them work. It’s like a blackboard for sketchy backtests not yet worthy of ink, and like a basketball backboard off which strategies will either bounce or sink, adding to the ever-growing tally of doubt. 

We’re starting with William O’Neil’s CANSLIM strategy.  He seemed ripe for confirming our doubts.  We’ll move on to others soon enough. This space is a log of progress for now. Proper documentation may happen eventually, though I wouldn’t hold my breath.

## Project Progress Highlights

- **2024-12-12**: Made a [fun interative visualization of the S&P 500 historical constituents](https://backboard.uk/html/sp500.html).
- **2024-12-11**: Each of CANSLIM validated. [Reports](https://backboard.uk/html/backtest_report.html) now more descriptive. Metrics use money market proxy.
- **2024-12-10**: Now have unique reports for each strategy. The L and A in CANSLIM need work.
- **2024-12-09**: First full pipeline execution with backtesting and reporting; NaNs and flat lines in outputs, will debug further.
- **2024-12-07**: Project reorganized and renamed to [BackBoard](https://backboard.uk/).
- **2024-12-06**: Splits, dividends, and name changes now accounted for in data processing.
- **2024-12-03**: Pulled historic financials and completed basic calculations for all of CANSLIM.
- **2024-11-28**: Successfully pulled and processed all day aggregates from 2003 to present.
- **2024-11-28**: Initial S3 client setup for fetching flat files.
- **2024-11-28**: Initial commit and project setup.

## License

This project is licensed under the GPL-3.0 License. See the `LICENSE` file for details.
