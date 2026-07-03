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
import pyotp
from functools import wraps
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect, KiteTicker
from dotenv import load_dotenv

# ==============================================================================
# 1. CONFIGURATION & CREDENTIALS
# ==============================================================================
load_dotenv()

API_KEY = os.getenv("API_KEY", "")
API_SECRET = os.getenv("API_SECRET", "")
KITE_USER_ID = os.getenv("KITE_USER_ID", "")
KITE_PASSWORD = os.getenv("KITE_PASSWORD", "")
KITE_TOTP_SECRET = os.getenv("KITE_TOTP_SECRET", "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
RUN_MODE = os.getenv("RUN_MODE", "PAPER").upper() # LIVE, PAPER, BACKTEST

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
# 2. UTILITY DECORATORS & STATE MANAGEMENT
# ==============================================================================
def retry_api_call(max_retries=3, backoff_factor=2):
    """Exponential backoff decorator for Kite API calls."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            while retry_count < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"API Call failed in {func.__name__}: {e}. Retrying in {backoff_factor ** retry_count} seconds...")
                    time.sleep(backoff_factor ** retry_count)
                    retry_count += 1
            logger.error(f"Max retries reached for {func.__name__}")
            raise Exception(f"Max retries reached for {func.__name__}")
        return wrapper
    return decorator


class StateManager:
    def __init__(self, filename="state.json"):
        self.filename = filename

    def save_state(self, state_dict):
        try:
            with open(self.filename, 'w') as f:
                json.dump(state_dict, f)
            logger.info("State saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def load_state(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    state = json.load(f)
                logger.info("Recovered previous state.")
                return state
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
        return None

    def clear_state(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)
            logger.info("State cleared.")


# ==============================================================================
# 3. TELEGRAM INTEGRATION
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
# 4. DATABASE & RESONANCE LEARNING MODULE
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

        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce')
        df = df.dropna(subset=['time'])
        df['time_bracket'] = df['time'].dt.floor('30min').dt.time.astype(str)
        df['width_bracket'] = pd.cut(df['range_width'], bins=[0, 10, 20, 30, 100], labels=['<10', '10-20', '20-30', '>30'])
        
        df['is_win'] = df['pnl_points'] > 0
        pattern_summary = df.groupby(['strike_type', 'time_bracket', 'width_bracket'], observed=False).agg(
            total_trades=('id', 'count'),
            win_rate=('is_win', 'mean'),
            avg_pnl=('pnl_points', 'mean')
        ).reset_index()

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
# 5. PRODUCTION-GRADE ZERODHA AUTHENTICATION & SESSION RECOVERY
# ==============================================================================
import json
import urllib.parse
from typing import Tuple, Optional, Any

try:
    from kiteconnect.exceptions import (
        TokenException, 
        NetworkException, 
        DataException, 
        GeneralException
    )
except ImportError:
    pass

TOKEN_FILE = "kite_auth.json"

def _load_saved_token() -> Optional[str]:
    """Loads the persistent access token from local JSON config."""
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                return data.get("access_token")
        except Exception as e:
            logger.warning(f"Could not read saved token file: {e}")
    return None

def _save_token(token: str) -> None:
    """Persistently stores the access token securely."""
    try:
        with open(TOKEN_FILE, 'w') as f:
            json.dump({"access_token": token}, f)
        logger.info(f"📝 Access token saved securely to {TOKEN_FILE}")
    except Exception as e:
        logger.error(f"Failed to save token to {TOKEN_FILE}: {e}")

def _delete_saved_token() -> None:
    """Removes the persistent token upon session expiration."""
    if os.path.exists(TOKEN_FILE):
        try:
            os.remove(TOKEN_FILE)
            logger.info("🗑️ Invalid/Expired token removed from storage.")
        except Exception:
            pass

def execute_manual_login_flow(api_key: str, api_secret: str, kite_client: KiteConnect) -> Tuple[Optional[str], bool]:
    """
    Handles the interactive manual login flow via the official KiteConnect SDK.
    Loops gracefully if the user provides an invalid URL.
    """
    if not api_key or not api_secret:
        logger.error("API Key or Secret missing from environment variables.")
        return None, False

    # 1. Use Official SDK Login URL
    login_url = kite_client.login_url()

    print("\n" + "="*80)
    print(" 🔒 ZERODHA AUTHENTICATION REQUIRED")
    print("="*80)
    print("ZERODHA AUTHENTICATION")
    print("Generating Login URL...")
    print(f"1. Open this URL in your web browser:\n\n   {login_url}\n")
    print("2. Log in to your Zerodha account using your credentials and TOTP.")
    print("3. After a successful login, you will be redirected.")
    print("4. Copy the ENTIRE URL from your browser's address bar.")
    print("   (Example: https://127.0.0.1/?action=login&status=success&request_token=YOUR_TOKEN)")
    print("="*80)

    while True:
        try:
            print("Waiting for User Login...")
            full_url = input("\nPaste the FULL redirect URL here (or type 'exit' to quit): ").strip()
            
            if full_url.lower() == 'exit':
                logger.info("Authentication aborted by user.")
                return None, False
                
            # 2. Extract Token via Standard Libraries
            logger.info("Extracting Request Token...")
            parsed_url = urllib.parse.urlparse(full_url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            request_token_list = query_params.get('request_token')
            if not request_token_list or not request_token_list[0]:
                logger.warning("Could not find 'request_token' in the provided URL. Please ensure you copied the entire URL.")
                continue
                
            request_token = request_token_list[0]
            
            # 3. Exchange request_token for access_token using official SDK
            logger.info("Generating Access Token...")
            session = kite_client.generate_session(request_token, api_secret=api_secret)
            access_token = session.get("access_token")
            
            if access_token:
                return access_token, True
            else:
                logger.error("Access token missing from Zerodha API response.")
                
        except Exception as e:
            error_str = str(e).lower()
            if "token" in error_str or "expired" in error_str or "invalid" in error_str:
                logger.error(f"The request token is invalid or has expired: {e}")
            else:
                logger.exception(f"Unexpected error during token exchange: {e}")
            
        logger.info("Please generate a fresh login URL and try again.")


class RobustKiteWrapper:
    """
    A Proxy Wrapper for KiteConnect that monitors API calls.
    If an Authentication or Session error occurs mid-trading, it pauses execution, 
    deletes the dead token, triggers manual re-login, and replays the failed API call.
    Maintains 100% compatibility with existing execution engines.
    """
    def __init__(self, api_key: str, api_secret: str, base_kite_class: Any):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_kite_class = base_kite_class
        self._kite = self._initialize_and_validate()

    def _initialize_and_validate(self) -> KiteConnect:
        """Loads persistent token, validates it, or drops to manual login."""
        kite_client = self.base_kite_class(api_key=self.api_key)
        saved_token = _load_saved_token()
        
        if saved_token:
            logger.info("🔄 Found persistent access token. Validating Session...")
            kite_client.set_access_token(saved_token)
            try:
                profile = kite_client.profile()
                logger.info(f"✓ Authentication Successful. Logged in as: {profile.get('user_name', 'Unknown')}")
                return kite_client
            except Exception as e:
                logger.warning(f"Saved session invalid or expired: {e}")
                _delete_saved_token()
                # Create a fresh client to clear old headers
                kite_client = self.base_kite_class(api_key=self.api_key)
                
        # Drop to manual login
        access_token, success = execute_manual_login_flow(self.api_key, self.api_secret, kite_client)
        if success and access_token:
            kite_client.set_access_token(access_token)
            try:
                logger.info("Validating Session...")
                profile = kite_client.profile()
                logger.info(f"✓ Authentication Successful. Logged in as: {profile.get('user_name', 'Unknown')}")
                _save_token(access_token)
                send_telegram_alert("🟢 **Algo Bot Login Successful (Manual)**")
                return kite_client
            except Exception as e:
                logger.error(f"Profile validation failed after fresh login: {e}")
                send_telegram_alert("🔴 **Algo Bot Login Failed**")
                
        logger.critical("System cannot proceed without valid Zerodha authentication.")
        sys.exit(1)

    def _trigger_session_recovery(self) -> None:
        """Pauses the engine to force a re-login after an active session drops."""
        logger.critical("🚨 KITE SESSION EXPIRED MID-EXECUTION! Initiating automatic recovery...")
        send_telegram_alert("🚨 **Session Expired! System paused for manual re-authentication.**")
        _delete_saved_token()
        self._kite = self._initialize_and_validate()
        logger.info("🟢 Session recovery complete. Resuming trading execution...")
        send_telegram_alert("🟢 **Session Recovered Successfully. Trading Resumed.**")

    def __getattr__(self, name: str) -> Any:
        """
        Delegates attribute access to the underlying KiteConnect instance.
        Wraps callable methods in an error-handling try/except block.
        """
        attr = getattr(self._kite, name)
        
        if callable(attr):
            def wrapper(*args, **kwargs):
                try:
                    return attr(*args, **kwargs)
                except Exception as e:
                    error_msg = str(e).lower()
                    # Detect Authentication/Token drops from Zerodha
                    if "token" in error_msg or "auth" in error_msg or "session" in error_msg or getattr(e, 'code', 0) in [403, 401]:
                        self._trigger_session_recovery()
                        # Replay the API call transparently
                        new_attr = getattr(self._kite, name)
                        return new_attr(*args, **kwargs)
                    
                    # Raise standard exceptions (Network, Margin, etc) for normal engine handling
                    raise
            return wrapper
        return attr

# ==============================================================================
# 6. CORE STRATEGY CLASS
# ==============================================================================
class NiftyOpeningRangeAlgo:
    def __init__(self, kite: KiteConnect, db: TradeDatabase, state_manager: StateManager):
        self.kite = kite
        self.db = db
        self.state_manager = state_manager
        self.instruments_df = pd.DataFrame()
        
        # Strategy State
        self.ce_instrument = None
        self.pe_instrument = None
        self.ce_range = {'high': 0, 'low': float('inf')}
        self.pe_range = {'high': 0, 'low': float('inf')}
        
        self.active_position = None
        self.trades_today = 0
        self.losses_today = 0
        self.is_trading_halted = False
        
        self.instrument_tokens = []
        self.token_map = {}

        self._recover_state()

    def _recover_state(self):
        state = self.state_manager.load_state()
        if state and state.get('date') == datetime.now().strftime("%Y-%m-%d"):
            self.active_position = state.get('active_position')
            self.trades_today = state.get('trades_today', 0)
            self.losses_today = state.get('losses_today', 0)
            self.is_trading_halted = state.get('is_trading_halted', False)
            self.ce_range = state.get('ce_range', {'high': 0, 'low': float('inf')})
            self.pe_range = state.get('pe_range', {'high': 0, 'low': float('inf')})
            self.ce_instrument = state.get('ce_instrument')
            self.pe_instrument = state.get('pe_instrument')
            if self.ce_instrument and self.pe_instrument:
                self.instrument_tokens = [self.ce_instrument['token'], self.pe_instrument['token']]
                self.token_map = {
                    self.ce_instrument['token']: {'type': 'CE', 'range': self.ce_range, 'symbol': self.ce_instrument['symbol']},
                    self.pe_instrument['token']: {'type': 'PE', 'range': self.pe_range, 'symbol': self.pe_instrument['symbol']}
                }
            logger.info("Successfully recovered state for today.")
        else:
            self.state_manager.clear_state()

    def _save_current_state(self):
        state = {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'active_position': self.active_position,
            'trades_today': self.trades_today,
            'losses_today': self.losses_today,
            'is_trading_halted': self.is_trading_halted,
            'ce_range': self.ce_range,
            'pe_range': self.pe_range,
            'ce_instrument': self.ce_instrument,
            'pe_instrument': self.pe_instrument
        }
        self.state_manager.save_state(state)

    @retry_api_call(max_retries=3)
    def load_instruments(self):
        """Fetches and caches today's instrument list."""
        logger.info("Fetching instruments...")

        try:
            instruments = self.kite.instruments("NFO")
        except Exception as e:
            logger.warning(f"Failed to fetch live instruments: {e}. If BACKTEST, ensure mock data is provided.")
            if RUN_MODE == "BACKTEST":
                return # Allow mock to bypass if real API fails during test script execution
            raise
        self.instruments_df = pd.DataFrame(instruments)
        self.instruments_df = self.instruments_df[
            (self.instruments_df['name'] == 'NIFTY') & 
            (self.instruments_df['instrument_type'].isin(['CE', 'PE']))
        ]
        logger.info(f"Loaded {len(self.instruments_df)} NIFTY options.")

    @retry_api_call(max_retries=3)
    def select_strikes_at_920(self, backtest_date=None):
        if RUN_MODE == "BACKTEST":
            # In backtest mode, we fetch historical NIFTY spot data around 9:20 for the given date
            spot_hist = self.kite.historical_data(
                instrument_token=256265, # NIFTY 50 SPOT
                from_date=f"{backtest_date} 09:19:00",
                to_date=f"{backtest_date} 09:20:00",
                interval="minute"
            )
            if not spot_hist:
                logger.warning(f"No spot data found for {backtest_date}")
                return []
            spot_price = spot_hist[-1]['close']
            logger.info(f"BACKTEST {backtest_date} 9:20 AM Spot: {spot_price}")
        else:
            spot_quote = self.kite.quote(f"NSE:NIFTY 50")
            spot_price = spot_quote["NSE:NIFTY 50"]["last_price"]

        atm_strike = round(spot_price / 50) * 50
        if RUN_MODE != "BACKTEST":
            logger.info(f"9:20 AM Spot: {spot_price}, ATM Strike: {atm_strike}")

            current_expiry = sorted(self.instruments_df['expiry'].unique())[0]
            options = self.instruments_df[self.instruments_df['expiry'] == current_expiry]
            
            ce_candidates = options[(options['strike'] >= atm_strike - 300) & (options['strike'] <= atm_strike + 300) & (options['instrument_type'] == 'CE')]
            pe_candidates = options[(options['strike'] >= atm_strike - 300) & (options['strike'] <= atm_strike + 300) & (options['instrument_type'] == 'PE')]

            if RUN_MODE == "BACKTEST":
                # Find instrument with price closest to 100 using historical data at 9:20
                best_ce, best_pe = None, None
                min_ce_diff, min_pe_diff = float('inf'), float('inf')

                for _, row in ce_candidates.iterrows():
                    hist = self.kite.historical_data(row['instrument_token'], f"{backtest_date} 09:19:00", f"{backtest_date} 09:20:00", "minute")
                    if hist:
                        ltp = hist[-1]['close']
                        diff = abs(ltp - 100)
                        if diff < min_ce_diff:
                            min_ce_diff = diff
                            best_ce = {'symbol': f"NFO:{row['tradingsymbol']}", 'token': row['instrument_token'], 'ltp': ltp}

                for _, row in pe_candidates.iterrows():
                    hist = self.kite.historical_data(row['instrument_token'], f"{backtest_date} 09:19:00", f"{backtest_date} 09:20:00", "minute")
                    if hist:
                        ltp = hist[-1]['close']
                        diff = abs(ltp - 100)
                        if diff < min_pe_diff:
                            min_pe_diff = diff
                            best_pe = {'symbol': f"NFO:{row['tradingsymbol']}", 'token': row['instrument_token'], 'ltp': ltp}
            else:
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
            
            if best_ce and best_pe:
                logger.info(f"Selected CE: {best_ce['symbol']} @ {best_ce['ltp']}")
                logger.info(f"Selected PE: {best_pe['symbol']} @ {best_pe['ltp']}")
            else:
                logger.warning("Could not select strikes properly.")
                return []
            
            fetch_date = backtest_date if RUN_MODE == "BACKTEST" else datetime.now().strftime('%Y-%m-%d')
            self._fetch_historical_range(self.ce_instrument, self.ce_range, fetch_date)
            self._fetch_historical_range(self.pe_instrument, self.pe_range, fetch_date)

        self.instrument_tokens = [best_ce['token'], best_pe['token']]
        self.token_map = {
            best_ce['token']: {'type': 'CE', 'range': self.ce_range, 'symbol': best_ce['symbol']},
            best_pe['token']: {'type': 'PE', 'range': self.pe_range, 'symbol': best_pe['symbol']}
        }

        self._save_current_state()
        return self.instrument_tokens

    @retry_api_call(max_retries=3)
    def _fetch_historical_range(self, instrument, range_dict, date_str):
        hist_data = self.kite.historical_data(
            instrument_token=instrument['token'],
            from_date=f"{date_str} 09:15:00",
            to_date=f"{date_str} 09:20:00",
            interval="minute"
        )
        for candle in hist_data:
            range_dict['high'] = max(range_dict['high'], candle['high'])
            range_dict['low'] = min(range_dict['low'], candle['low'])

    def execute_trade(self, symbol, price, strike_type, range_width):
        """Executes a market buy order dynamically allocating based on CAPITAL."""
        if self.is_trading_halted or self.trades_today >= MAX_TRADES_PER_DAY or self.active_position:
            return

        # Dynamic Lot Sizing
        lots = max(1, int(CAPITAL // (price * NIFTY_LOT_SIZE)))
        qty = lots * NIFTY_LOT_SIZE
        req_margin = qty * price

        if req_margin > CAPITAL:
            logger.warning(f"Insufficient capital. Need {req_margin}, have {CAPITAL}. Skipping trade.")
            return

        try:
            if RUN_MODE == "LIVE":
                order_id = self.kite.place_order(tradingsymbol=symbol.split(":")[1],
                                                 exchange=self.kite.EXCHANGE_NFO,
                                                 transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                                                 quantity=qty,
                                                 order_type=self.kite.ORDER_TYPE_MARKET,
                                                 product=self.kite.PRODUCT_MIS)
                logger.info(f"LIVE BUY ORDER EXECUTED: ID {order_id}")
            elif RUN_MODE in ["PAPER", "BACKTEST"]:
                logger.info(f"{RUN_MODE} BUY MOCKED for {symbol} at {price}")
            
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

            self._save_current_state()

            msg = f"🟢 **TRADE TRIGGERED ({RUN_MODE})**\nSymbol: {symbol}\nEntry: ₹{price}\nQty: {qty}\nSL: ₹{sl_price}\nTime: {self.active_position['entry_time']}"
            send_telegram_alert(msg)
            
        except Exception as e:
            logger.error(f"Order Execution Failed: {e}")

    def close_position(self, reason, exit_price):
        if not self.active_position: return
        
        pos = self.active_position
        pnl = (exit_price - pos['entry_price']) * pos['qty']
        outcome = "PROFIT" if pnl > 0 else "LOSS"

        try:
            if RUN_MODE == "LIVE":
                order_id = self.kite.place_order(tradingsymbol=pos['symbol'].split(":")[1],
                                                 exchange=self.kite.EXCHANGE_NFO,
                                                 transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                                                 quantity=pos['qty'],
                                                 order_type=self.kite.ORDER_TYPE_MARKET,
                                                 product=self.kite.PRODUCT_MIS)
                logger.info(f"LIVE SELL ORDER EXECUTED: ID {order_id}")
            elif RUN_MODE in ["PAPER", "BACKTEST"]:
                logger.info(f"{RUN_MODE} SELL MOCKED for {pos['symbol']} at {exit_price}")
            
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

            msg = f"🔴 **POSITION CLOSED ({RUN_MODE})**\nSymbol: {pos['symbol']}\nExit: ₹{exit_price}\nPnL: ₹{pnl}\nReason: {reason}"
            send_telegram_alert(msg)
            
            self.active_position = None
            self._save_current_state()

            if self.losses_today >= MAX_LOSSES_PER_DAY:
                self.is_trading_halted = True
                self._save_current_state()
                send_telegram_alert("🛑 **MAX LOSSES HIT (2). TRADING HALTED FOR THE DAY.**")

        except Exception as e:
            logger.error(f"Failed to close position: {e}")


# ==============================================================================
# 7. WEBSOCKET LOGIC & TICK PROCESSOR
# ==============================================================================
def process_tick(algo, token, ltp, tick_time):
    """Core logic extracted from websocket for easy backtesting."""
    if datetime.strptime("09:20:00", "%H:%M:%S").time() <= tick_time <= datetime.strptime("09:25:00", "%H:%M:%S").time():
        if token in algo.token_map:
            r_dict = algo.token_map[token]['range']
            r_dict['high'] = max(r_dict['high'], ltp)
            r_dict['low'] = min(r_dict['low'], ltp)

    elif tick_time > datetime.strptime("09:25:00", "%H:%M:%S").time():
        if not algo.active_position and not algo.is_trading_halted and algo.trades_today < MAX_TRADES_PER_DAY:
            if token in algo.token_map:
                data = algo.token_map[token]
                range_high = data['range']['high']
                range_width = range_high - data['range']['low']
                
                if ltp > range_high:
                    algo.execute_trade(data['symbol'], ltp, data['type'], range_width)

    if algo.active_position:
        pos = algo.active_position
        # Find token of active position
        active_token = next((t for t, v in algo.token_map.items() if v['symbol'] == pos['symbol']), None)
        if active_token and token == active_token:
            pos['max_favorable_move'] = max(pos['max_favorable_move'], ltp)
            algo._save_current_state()

            if not pos['profit_locked'] and ltp >= pos['entry_price'] + PROFIT_LOCK_TARGET:
                pos['sl'] = pos['entry_price'] + PROFIT_LOCK_TARGET
                pos['profit_locked'] = True
                logger.info(f"🔒 Profit Locked! SL moved to {pos['sl']}")
                send_telegram_alert(f"🔒 **PROFIT LOCKED**\nSymbol: {pos['symbol']}\nNew SL: {pos['sl']}")
                algo._save_current_state()

            if pos['profit_locked']:
                points_gained = pos['max_favorable_move'] - (pos['entry_price'] + PROFIT_LOCK_TARGET)
                if points_gained >= TRAIL_TRIGGER_Y:
                    steps = int(points_gained // TRAIL_TRIGGER_Y)
                    new_sl = (pos['entry_price'] + PROFIT_LOCK_TARGET) + (steps * TRAIL_AMOUNT_X)
                    if new_sl > pos['sl']:
                        pos['sl'] = new_sl
                        logger.info(f"📈 Trailing SL updated to {pos['sl']}")
                        algo._save_current_state()

            if ltp <= pos['sl']:
                algo.close_position("SL HIT / TRAILING SL HIT", ltp)

def on_ticks(ws, ticks):
    now = datetime.now().time()
    for tick in ticks:
        process_tick(algo, tick['instrument_token'], tick['last_price'], now)

def on_connect(ws, response):
    logger.info("Websocket Connected.")
    if algo.instrument_tokens:
        logger.info(f"Subscribing to recovered/active tokens: {algo.instrument_tokens}")
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
# 8. SCHEDULER & MAIN LOOP
# ==============================================================================
def trigger_920_selection(kws):
    logger.info("Executing 9:20 AM Strike Selection...")
    tokens = algo.select_strikes_at_920()
    if tokens and RUN_MODE != "BACKTEST":
        kws.subscribe(tokens)
        kws.set_mode(kws.MODE_FULL, tokens)
        
        ce_r = algo.ce_range
        pe_r = algo.pe_range
        msg = f"🎯 **Range Marked (9:15-9:20 Snapshot)**\n\n**{algo.ce_instrument['symbol']}**\nHigh: {ce_r['high']} | Low: {ce_r['low']}\n\n**{algo.pe_instrument['symbol']}**\nHigh: {pe_r['high']} | Low: {pe_r['low']}"
        send_telegram_alert(msg)

def trigger_eod_tasks():
    logger.info("Executing End of Day Tasks...")
    if algo.active_position:
        algo.close_position("EOD Square Off (3:15 PM)", algo.active_position['max_favorable_move']) 
    
    summary = f"📊 **EOD Summary ({RUN_MODE})**\nTrades Today: {algo.trades_today}\nLosses: {algo.losses_today}"
    send_telegram_alert(summary)
    
    report = db.run_resonance_analytics()
    send_telegram_alert(report)


def run_backtest_simulation():
    """Simulates backtest mode using historical data ingestion to verify flow."""
    logger.info("Running Backtest Simulation over last 5 trading days...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    trading_days = pd.bdate_range(start=start_date, end=end_date).tolist()
    
    for day in trading_days:
        date_str = day.strftime('%Y-%m-%d')
        logger.info(f"--- BACKTESTING FOR DATE: {date_str} ---")

        # Reset algo state for the new day
        algo.active_position = None
        algo.trades_today = 0
        algo.losses_today = 0
        algo.is_trading_halted = False
        algo.ce_range = {'high': 0, 'low': float('inf')}
        algo.pe_range = {'high': 0, 'low': float('inf')}

        tokens = algo.select_strikes_at_920(backtest_date=date_str)
        if not tokens:
            logger.info(f"Skipping {date_str} due to strike selection failure.")
            continue

        logger.info(f"Selected Tokens: {tokens}. Fetching tick simulation data...")

        # Ingest 1 min candles for the selected strikes from 9:20 to 15:15
        all_ticks = []
        for token in tokens:
            hist = algo.kite.historical_data(
                instrument_token=token,
                from_date=f"{date_str} 09:20:00",
                to_date=f"{date_str} 15:15:00",
                interval="minute"
            )
            for candle in hist:
                all_ticks.append({
                    'time': candle['date'].time(),
                    'token': token,
                    'ltp': candle['close'] # simulate using candle close
                })

        # Sort ticks chronologically to simulate streaming
        all_ticks.sort(key=lambda x: x['time'])

        for tick in all_ticks:
            process_tick(algo, tick['token'], tick['ltp'], tick['time'])

        trigger_eod_tasks()

    logger.info("Backtest Simulation Complete.")

if __name__ == "__main__":
    logger.info(f"Initializing Algo Environment... RUN_MODE={RUN_MODE}")
    
    db = TradeDatabase()
    state_manager = StateManager()

    if RUN_MODE in ["LIVE", "PAPER"]:
        # Initializes Kite, validates token, and prompts manual login if needed
        kite = RobustKiteWrapper(
            api_key=API_KEY, 
            api_secret=API_SECRET, 
            base_kite_class=KiteConnect
        )
        algo = NiftyOpeningRangeAlgo(kite, db, state_manager)
        algo.load_instruments()
        ticker = start_ticker(kite)

        schedule.every().day.at("09:20").do(trigger_920_selection, kws=ticker)
        schedule.every().day.at("15:15").do(trigger_eod_tasks)

        # NOTE: Removed 08:30 scheduled automated login since authentication requires manual URL pasting.
        # The wrapper handles initialization seamlessly on startup.

        logger.info("System Ready. Waiting for schedule triggers...")
        try:
            while True:
                schedule.run_pending()
                now = datetime.now().time()
                if now > datetime.strptime("15:30:00", "%H:%M:%S").time():
                    logger.info("Market Closed. Exiting script for the day.")
                    sys.exit(0)
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Manual Interrupt. Shutting down gracefully...")
            if ticker:
                ticker.close()
                
    elif RUN_MODE == "BACKTEST":
        # We need kite for historical data ingestion
        kite = RobustKiteWrapper(
            api_key=API_KEY, 
            api_secret=API_SECRET, 
            base_kite_class=KiteConnect
        )
        algo = NiftyOpeningRangeAlgo(kite, db, state_manager)
        algo.load_instruments()
        run_backtest_simulation()
