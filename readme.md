You are an expert algorithmic trading system architect and elite Python developer specializing in the Kite Connect (Zerodha) API and quantitative trading strategies. 

Generate a complete, production-ready, and highly robust Python script for an intraday option buying strategy on NIFTY 50. The code must be clean, modular, well-commented, and include comprehensive error handling, logging, and performance tracking.

### 1. Strategy Core Logic
* **Instrument:** NIFTY 50 Index Options.
* **Selection Time (9:20 AM):** At exactly 9:20 AM, look at the NIFTY 50 spot price. Identify the nearest ATM Call (CE) and Put (PE) contracts. From those surrounding strikes, select the specific CE and PE contracts whose trading premium is closest to ₹100.
* **Opening Range (9:15 AM - 9:25 AM):** Track the 10-minute intraday opening range (High and Low) for both the selected CE and PE contracts.
* **Trade Trigger (Breakout):** * Monitor live LTP or 1-minute candle closes after 9:25 AM.
    * Initiate a BUY trade on a single lot if either the CE breaks above its 10-min high OR the PE breaks above its 10-min high.
    * Execution must only happen after a clear breakout confirmation (e.g., LTP crosses high or 1-min candle closes above the high).
* **Trade Management:**
    * **Target:** No fixed target (let profits run).
    * **Stop Loss (SL):** Initial SL set at the 10-minute range low or a fixed point structure (make this a configurable variable).
    * **Profit Locking:** As soon as the trade moves +10 points in profit, instantly lock +10 points by moving the Stop Loss to (Entry Price + 10 points).
    * **Trailing SL:** Implement a continuous trailing mechanism (e.g., trail SL by X points for every Y points move in favor).

### 2. Risk Management & Capital Controls
* **Total Capital:** ₹10,000 (Algorithm must dynamically calculate lot sizing based on this capital for a single lot of Nifty).
* **Max Trades Per Day:** Strictly maximum 4 trade executions per day.
* **Max Losses Per Day:** If the strategy hits 2 losses in a single day, immediately halt all trading for the day, cancel open orders, and square off any open positions.

### 3. API & Third-Party Integrations
* **Zerodha Kite Connect:** * Implement clean session handling, handling access tokens, public tokens, and automatic re-login redirection if the token expires.
    * Use websocket `KiteTicker` for real-time data streaming to capture precise breakout levels.
* **Telegram Integration:** * Send real-time instant alerts for: Login successful, Range Marked (with High/Low prices), Trade Triggered (Strike, Price, Time), SL Hit/Profit Locked, Daily Goal/Stop Hit, and EOD Summary.

### 4. Advanced Performance Analytics & Pattern Recognition
* **Resonance Learning & Pattern Tracing Structure:** * Implement a local JSON or SQLite-based data logging mechanism that acts as a feedback loop.
    * For every trade, log features such as: Date, Time of Breakout, Strike Type (CE/PE), Opening Range Width (High minus Low), Market Volatility (VIX if available, or point size of range), and Trade Outcome (Profit/Loss points).
    * The script should include an analytics module that processes this historical data to print which specific patterns (e.g., "CE breakouts between 9:30-10:00 AM with range width < 15 points") are highly profitable versus those that are consistently failing.
* **Performance Summaries:**
    * Generate and send via Telegram a **Day-wise**, **Week-wise**, and **Month-wise** PnL and Win-Rate summary using the saved database.

### 5. Technical Requirements & Architecture
* Use standard Python libraries like `pandas`, `requests`, `sqlite3` (or `json`), and official `kiteconnect`.
* Wrap the execution in a robust `try-except` loop with detailed logging to a `trading_bot.log` file.
* Handle market edge cases: order slippages, partial fills, network disconnections (auto-reconnect websocket), and market closure handling at 3:15 PM.
* Keep all credentials (API Key, API Secret, Access Token, Telegram Bot Token, Chat ID) in a separate configuration block or `.env` load structure.

Provide the complete code in a single executable file with placeholder variables for the configuration keys. Do not use pseudo-code or leave functions incomplete.
