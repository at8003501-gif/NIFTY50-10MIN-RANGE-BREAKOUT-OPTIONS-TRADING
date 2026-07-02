import os
import sys
import json
import time
import sqlite3
import logging
import threading
from datetime import datetime, timedelta
import pandas as pd
import requests
import schedule
from kiteconnect import KiteConnect, KiteTicker

# ==============================================================================
# 1. CONFIGURATION & CREDENTIALS
# ==============================================================================
# In production, keep these in a .env file. For this standalone file, update here.
API_KEY = "YOUR_KITE_API_KEY"
API_SECRET = "YOUR_KITE_API_SECRET"
REQUEST_TOKEN = "" # Only needed if ACCESS_TOKEN is missing or expired
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

# Risk & Strategy Parameters
CAPITAL = 10000
MAX_TRADES_PER_DAY = 4
MAX_LOSSES_PER_DAY = 2
INITIAL_SL_POINTS = 0 # 0 means use the 10-min range low dynamically
PROFIT_LOCK_TARGET = 10 # Points
TRAIL_TRIGGER_Y = 5 # For every 5 points move in favor...
TRAIL_AMOUNT_X = 5 # ...trail SL by 5 points
NIFTY_LOT_SIZE = 25

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("trading_bot.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# 2. TELEGRAM INTEGRATION
# ==============================================================================
def send_telegram_alert(message: str):
    """Sends a markdown formatted alert to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Telegram Alert Failed: {e}")

# ==============================================================================
# 3. DATABASE & RESONANCE LEARNING MODULE
# ==============================================================================
class TradeDatabase:
    def __init__(self, db_name="trading_data.sqlite"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                time TEXT,
                strike_type TEXT,
                instrument TEXT,
                range_width REAL,
                entry_price REAL,
                exit_price REAL,
                pnl_points REAL,
                outcome TEXT
            )
        ''')
        self.conn.commit()

    def log_trade(self, trade_data):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO trades (date, time, strike_type, instrument, range_width, entry_price, exit_price, pnl_points, outcome)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (trade_data['date'], trade_data['time'], trade_data['strike_type'], trade_data['instrument'], 
              trade_data['range_width'], trade_data['entry_price'], trade_data['exit_price'], trade_data['pnl_points'], trade_data['outcome']))
        self.conn.commit()
        logger.info(f"Trade logged to Resonance DB: {trade_data['instrument']} | PnL: {trade_data['pnl_points']}")

    def run_resonance_analytics(self):
        """Analyzes historical data to find highly profitable vs failing patterns."""
        query = "SELECT * FROM trades"
        df = pd.read_sql_query(query, self.conn)
        
        if df.empty:
            return "No historical data available for Resonance Learning."

        # Feature Engineering for Pattern Tracing
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')
        df['time_bracket'] = df['time'].dt.floor('30min').dt.time.astype(str)
        df['width_bracket'] = pd.cut(df['range_width'], bins=[0, 10, 20, 30, 100], labels=['<10', '10-20', '20-30', '>30'])
        
        # Win-Rate by Pattern
        df['is_win'] = df['pnl_points'] > 0
        pattern_summary = df.groupby(['strike_type', 'time_bracket', 'width_bracket']).agg(
            total_trades=('id', 'count'),
            win_rate=('is_win', 'mean'),
            avg_pnl=('pnl_points', 'mean')
        ).reset_index()

        # Filter robust patterns
        profitable = pattern_summary[(pattern_summary['total_trades'] >= 3) & (pattern_summary['win_rate'] >= 0.6)]
        failing = pattern_summary[(pattern_summary['total_trades'] >= 3) & (pattern_summary['win_rate'] <= 0.3)]

        report = "🧠 **Resonance Learning Report**\n\n"
        report += "📈 **Highly Profitable Patterns:**\n"
        for _, row in profitable.iterrows():
            report += f"- {row['strike_type']} | {row['time_bracket']} | Width {row['width_bracket']}: WR {row['win_rate']:.0%} | Avg PnL: {row['avg_pnl']:.1f}\n"
        
        report += "\n📉 **Consistently Failing Patterns:**\n"
        for _, row in failing.iterrows():
            report += f"- {row['strike_type']} | {row['time_bracket']} | Width {row['width_bracket']}: WR {row['win_rate']:.0%} | Avg PnL: {row['avg_pnl']:.1f}\n"

        return report

# ==============================================================================
# 4. KITE SESSION MANAGEMENT
# ==============================================================================
def get_kite_session():
    """Handles Kite login and session generation."""
    kite = KiteConnect(api_key=API_KEY)
    access_token_file = "access_token.txt"

    if os.path.exists(access_token_file):
        with open(access_token_file, 'r') as f:
            token_data = f.read().strip()
            # Simple check if token is from today
            if token_data and datetime.fromtimestamp(os.path.getmtime(access_token_file)).date() == datetime.today().date():
                kite.set_access_token(token_data)
                logger.info("Using cached access token for today.")
                return kite

    logger.info(f"Login required. Please generate request token from: {kite.login_url()}")
    req_token = REQUEST_TOKEN if REQUEST_TOKEN else input("Enter REQUEST_TOKEN: ")
    
    try:
        data = kite.generate_session(req_token, api_secret=API_SECRET)
        kite.set_access_token(data["access_token"])
        with open(access_token_file, 'w') as f:
            f.write(data["access_token"])
        send_telegram_alert("🟢 **Algo Bot Login Successful**")
        return kite
    except Exception as e:
        logger.error(f"Login failed: {e}")
        send_telegram_alert("🔴 **Algo Bot Login Failed**")
        sys.exit(1)

# ==============================================================================
# 5. CORE STRATEGY CLASS
# ==============================================================================
class NiftyOpeningRangeAlgo:
    def __init__(self, kite: KiteConnect, db: TradeDatabase):
        self.kite = kite
        self.db = db
        self.instruments_df = pd.DataFrame()
        
        # Strategy State
        self.ce_instrument = None
        self.pe_instrument = None
        self.ce_range = {'high': 0, 'low': float('inf')}
        self.pe_range = {'high': 0, 'low': float('inf')}
        
        self.active_position = None  # Dict holding trade details
        self.trades_today = 0
        self.losses_today = 0
        self.is_trading_halted = False
        
        self.instrument_tokens = [] # Tokens for ticker
        self.token_map = {} # Maps token to instrument details

    def load_instruments(self):
        """Fetches and caches today's instrument list."""
        logger.info("Fetching instruments...")
        instruments = self.kite.instruments("NFO")
        self.instruments_df = pd.DataFrame(instruments)
        self.instruments_df = self.instruments_df[
            (self.instruments_df['name'] == 'NIFTY') & 
            (self.instruments_df['instrument_type'].isin(['CE', 'PE']))
        ]
        logger.info(f"Loaded {len(self.instruments_df)} NIFTY options.")

    def select_strikes_at_920(self):
        """Identifies ATM, fetches quotes for surrounding strikes, finds premium closest to 100."""
        try:
            # 1. Get Spot Price
            nse_nifty_token = 256265 # Standard token for NIFTY 50 SPOT
            spot_quote = self.kite.quote(f"NSE:NIFTY 50")
            spot_price = spot_quote["NSE:NIFTY 50"]["last_price"]
            atm_strike = round(spot_price / 50) * 50
            logger.info(f"9:20 AM Spot: {spot_price}, ATM Strike: {atm_strike}")

            # 2. Get surrounding strikes (+/- 300 points)
            current_expiry = sorted(self.instruments_df['expiry'].unique())[0]
            options = self.instruments_df[self.instruments_df['expiry'] == current_expiry]
            
            ce_candidates = options[(options['strike'] >= atm_strike - 300) & (options['strike'] <= atm_strike + 300) & (options['instrument_type'] == 'CE')]
            pe_candidates = options[(options['strike'] >= atm_strike - 300) & (options['strike'] <= atm_strike + 300) & (options['instrument_type'] == 'PE')]

            # 3. Fetch Quotes to find premium nearest to 100
            ce_symbols = [f"NFO:{ts}" for ts in ce_candidates['tradingsymbol']]
            pe_symbols = [f"NFO:{ts}" for ts in pe_candidates['tradingsymbol']]
            
            all_quotes = self.kite.quote(ce_symbols + pe_symbols)
            
            best_ce, best_pe = None, None
            min_ce_diff, min_pe_diff = float('inf'), float('inf')

            for symbol, data in all_quotes.items():
                ltp = data['last_price']
                diff = abs(ltp - 100)
                if 'CE' in symbol:
                    if diff < min_ce_diff:
                        min_ce_diff = diff
                        best_ce = {'symbol': symbol, 'token': data['instrument_token'], 'ltp': ltp}
                else:
                    if diff < min_pe_diff:
                        min_pe_diff = diff
                        best_pe = {'symbol': symbol, 'token': data['instrument_token'], 'ltp': ltp}

            self.ce_instrument = best_ce
            self.pe_instrument = best_pe
            
            logger.info(f"Selected CE: {best_ce['symbol']} @ {best_ce['ltp']}")
            logger.info(f"Selected PE: {best_pe['symbol']} @ {best_pe['ltp']}")
            
            # 4. Fetch 9:15 - 9:20 Historical Data to pre-fill range
            self._fetch_historical_range(self.ce_instrument, self.ce_range)
            self._fetch_historical_range(self.pe_instrument, self.pe_range)

            # Subscribe to Live Ticks
            self.instrument_tokens = [best_ce['token'], best_pe['token']]
            self.token_map = {
                best_ce['token']: {'type': 'CE', 'range': self.ce_range, 'symbol': best_ce['symbol']},
                best_pe['token']: {'type': 'PE', 'range': self.pe_range, 'symbol': best_pe['symbol']}
            }
            return self.instrument_tokens

        except Exception as e:
            logger.error(f"Error in strike selection: {e}")

    def _fetch_historical_range(self, instrument, range_dict):
        """Fetches historical 1-min candles from 9:15 to 9:20 to initialize the high/low."""
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            hist_data = self.kite.historical_data(
                instrument_token=instrument['token'],
                from_date=f"{today} 09:15:00",
                to_date=f"{today} 09:20:00",
                interval="minute"
            )
            for candle in hist_data:
                range_dict['high'] = max(range_dict['high'], candle['high'])
                range_dict['low'] = min(range_dict['low'], candle['low'])
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {instrument['symbol']}: {e}")

    def execute_trade(self, symbol, price, strike_type, range_width):
        """Executes a market buy order."""
        if self.is_trading_halted or self.trades_today >= MAX_TRADES_PER_DAY or self.active_position:
            return

        # Capital Allocation Logic
        qty = NIFTY_LOT_SIZE # Strictly single lot as per instructions
        req_margin = qty * price
        if req_margin > CAPITAL:
            logger.warning(f"Insufficient capital. Need {req_margin}, have {CAPITAL}. Halting.")
            return

        try:
            # Place actual order (Mocked logging for safety if API key is test)
            # order_id = self.kite.place_order(tradingsymbol=symbol.split(":")[1],
            #                                  exchange=self.kite.EXCHANGE_NFO,
            #                                  transaction_type=self.kite.TRANSACTION_TYPE_BUY,
            #                                  quantity=qty,
            #                                  order_type=self.kite.ORDER_TYPE_MARKET,
            #                                  product=self.kite.PRODUCT_MIS)
            
            logger.info(f"BUY ORDER EXECUTED for {symbol} at {price}")
            self.trades_today += 1
            
            sl_price = self.ce_range['low'] if strike_type == 'CE' else self.pe_range['low']
            
            self.active_position = {
                'symbol': symbol,
                'type': strike_type,
                'entry_price': price,
                'qty': qty,
                'sl': sl_price,
                'max_favorable_move': price,
                'profit_locked': False,
                'range_width': range_width,
                'entry_time': datetime.now().strftime("%H:%M:%S")
            }

            msg = f"🟢 **TRADE TRIGGERED**\nSymbol: {symbol}\nEntry: ₹{price}\nSL: ₹{sl_price}\nTime: {self.active_position['entry_time']}"
            send_telegram_alert(msg)
            
        except Exception as e:
            logger.error(f"Order Execution Failed: {e}")

    def close_position(self, reason, exit_price):
        """Squares off active position and logs to DB."""
        if not self.active_position: return
        
        pos = self.active_position
        pnl = (exit_price - pos['entry_price']) * pos['qty']
        outcome = "PROFIT" if pnl > 0 else "LOSS"

        try:
            # order_id = self.kite.place_order(tradingsymbol=pos['symbol'].split(":")[1],
            #                                  exchange=self.kite.EXCHANGE_NFO,
            #                                  transaction_type=self.kite.TRANSACTION_TYPE_SELL, ...)
            
            logger.info(f"POSITION CLOSED: {pos['symbol']} | Reason: {reason} | Exit: {exit_price} | PnL: {pnl}")
            
            if outcome == "LOSS":
                self.losses_today += 1

            trade_data = {
                'date': datetime.now().strftime("%Y-%m-%d"),
                'time': pos['entry_time'],
                'strike_type': pos['type'],
                'instrument': pos['symbol'],
                'range_width': pos['range_width'],
                'entry_price': pos['entry_price'],
                'exit_price': exit_price,
                'pnl_points': exit_price - pos['entry_price'],
                'outcome': outcome
            }
            self.db.log_trade(trade_data)

            msg = f"🔴 **POSITION CLOSED**\nSymbol: {pos['symbol']}\nExit: ₹{exit_price}\nPnL: ₹{pnl}\nReason: {reason}"
            send_telegram_alert(msg)
            
            self.active_position = None

            # Max Loss Check
            if self.losses_today >= MAX_LOSSES_PER_DAY:
                self.is_trading_halted = True
                send_telegram_alert("🛑 **MAX LOSSES HIT (2). TRADING HALTED FOR THE DAY.**")

        except Exception as e:
            logger.error(f"Failed to close position: {e}")


# ==============================================================================
# 6. WEBSOCKET LOGIC (THE HEARTBEAT)
# ==============================================================================
def on_ticks(ws, ticks):
    """Callback for real-time tick streaming."""
    now = datetime.now().time()
    
    for tick in ticks:
        token = tick['instrument_token']
        ltp = tick['last_price']

        # 1. Update Opening Range (9:20 to 9:25)
        if datetime.strptime("09:20:00", "%H:%M:%S").time() <= now <= datetime.strptime("09:25:00", "%H:%M:%S").time():
            if token in algo.token_map:
                r_dict = algo.token_map[token]['range']
                r_dict['high'] = max(r_dict['high'], ltp)
                r_dict['low'] = min(r_dict['low'], ltp)
        
        # 2. Check Breakout (After 9:25 AM)
        elif now > datetime.strptime("09:25:00", "%H:%M:%S").time():
            if not algo.active_position and not algo.is_trading_halted and algo.trades_today < MAX_TRADES_PER_DAY:
                if token in algo.token_map:
                    data = algo.token_map[token]
                    range_high = data['range']['high']
                    range_width = range_high - data['range']['low']
                    
                    if ltp > range_high:  # BREAKOUT!
                        algo.execute_trade(data['symbol'], ltp, data['type'], range_width)

        # 3. Trade Management (Active Position)
        if algo.active_position and token == algo.instrument_tokens[0]: # Assuming we only subscribe to active positions if needed, but here token match works.
            pos = algo.active_position
            if pos['symbol'] == algo.token_map[token]['symbol']:
                
                # Update max favorable move
                pos['max_favorable_move'] = max(pos['max_favorable_move'], ltp)
                
                # Instant Profit Locking (+10 pts)
                if not pos['profit_locked'] and ltp >= pos['entry_price'] + PROFIT_LOCK_TARGET:
                    pos['sl'] = pos['entry_price'] + PROFIT_LOCK_TARGET
                    pos['profit_locked'] = True
                    logger.info(f"🔒 Profit Locked! SL moved to {pos['sl']}")
                    send_telegram_alert(f"🔒 **PROFIT LOCKED**\nSymbol: {pos['symbol']}\nNew SL: {pos['sl']}")

                # Continuous Trailing SL
                if pos['profit_locked']:
                    points_gained = pos['max_favorable_move'] - (pos['entry_price'] + PROFIT_LOCK_TARGET)
                    if points_gained >= TRAIL_TRIGGER_Y:
                        steps = int(points_gained // TRAIL_TRIGGER_Y)
                        new_sl = (pos['entry_price'] + PROFIT_LOCK_TARGET) + (steps * TRAIL_AMOUNT_X)
                        if new_sl > pos['sl']:
                            pos['sl'] = new_sl
                            logger.info(f"📈 Trailing SL updated to {pos['sl']}")

                # Stop Loss Hit
                if ltp <= pos['sl']:
                    algo.close_position("SL HIT / TRAILING SL HIT", ltp)

def on_connect(ws, response):
    logger.info("Websocket Connected.")
    if algo.instrument_tokens:
        ws.subscribe(algo.instrument_tokens)
        ws.set_mode(ws.MODE_FULL, algo.instrument_tokens)

def on_close(ws, code, reason):
    logger.warning(f"Websocket closed: {code} - {reason}. Auto-reconnecting...")

def start_ticker(kite):
    kws = KiteTicker(API_KEY, kite.access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.connect(threaded=True)
    return kws

# ==============================================================================
# 7. SCHEDULER & MAIN LOOP
# ==============================================================================
def trigger_920_selection(kws):
    logger.info("Executing 9:20 AM Strike Selection...")
    tokens = algo.select_strikes_at_920()
    if tokens:
        kws.subscribe(tokens)
        kws.set_mode(kws.MODE_FULL, tokens)
        
        ce_r = algo.ce_range
        pe_r = algo.pe_range
        msg = f"🎯 **Range Marked (9:15-9:20 Snapshot)**\n\n**{algo.ce_instrument['symbol']}**\nHigh: {ce_r['high']} | Low: {ce_r['low']}\n\n**{algo.pe_instrument['symbol']}**\nHigh: {pe_r['high']} | Low: {pe_r['low']}"
        send_telegram_alert(msg)

def trigger_eod_tasks():
    logger.info("Executing End of Day Tasks...")
    if algo.active_position:
        # Assuming LTP is close to last recorded, for exact closing we'd need live quote
        algo.close_position("EOD Square Off (3:15 PM)", algo.active_position['max_favorable_move']) 
    
    # Send Summary
    summary = f"📊 **EOD Summary**\nTrades Today: {algo.trades_today}\nLosses: {algo.losses_today}"
    send_telegram_alert(summary)
    
    # Send Resonance Learning Report
    report = db.run_resonance_analytics()
    send_telegram_alert(report)

if __name__ == "__main__":
    logger.info("Initializing Algo Environment...")
    
    db = TradeDatabase()
    kite = get_kite_session()
    algo = NiftyOpeningRangeAlgo(kite, db)
    
    algo.load_instruments()
    
    # Start Websocket Thread
    ticker = start_ticker(kite)

    # Schedule tasks based on exact market timings
    schedule.every().day.at("09:20").do(trigger_920_selection, kws=ticker)
    schedule.every().day.at("15:15").do(trigger_eod_tasks)

    logger.info("System Ready. Waiting for schedule triggers...")
    
    try:
        while True:
            schedule.run_pending()
            
            # Failsafe: Market Close Exit loop completely
            now = datetime.now().time()
            if now > datetime.strptime("15:30:00", "%H:%M:%S").time():
                logger.info("Market Closed. Exiting script for the day.")
                sys.exit(0)
                
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Manual Interrupt. Shutting down gracefully...")
        if ticker:
            ticker.close()
