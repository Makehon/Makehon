import os
from fastapi import FastAPI, Request, Response, Depends
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import uvicorn
import uuid
import yfinance as yf
import asyncio
import hashlib
from contextlib import asynccontextmanager
import psycopg2
from psycopg2.extras import RealDictCursor

# Securely grab the Render environment variable, or fallback to your specific Neon URL


def get_db_connection():
    """Establishes a connection to the Neon PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

async def market_monitor():
    """Background loop checking live prices against active TP/SL and pending orders."""
    while True:
        await asyncio.sleep(15)
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # --- 1. CHECK PENDING ORDERS (Limit/Stop Entries) ---
            cursor.execute("SELECT * FROM pending_orders")
            pending = cursor.fetchall()

            for p in pending:
                current_price = get_live_price(p['ticker'])
                if current_price == 0.0: continue

                trigger = False
                if p['order_type'] == 'limit':
                    if p['side'] == 'BUY' and current_price <= p['target_price']: trigger = True
                    elif p['side'] == 'SELL' and current_price >= p['target_price']: trigger = True
                else:  # Stop or Stop Limit
                    if p['side'] == 'BUY' and current_price >= p['target_price']: trigger = True
                    elif p['side'] == 'SELL' and current_price <= p['target_price']: trigger = True

                if trigger:
                    borrowed = (p['shares'] * p['target_price']) - p['margin_locked']
                    cursor.execute("""
                        INSERT INTO positions (account_id, ticker, side, shares, entry_price, leverage, margin_locked, borrowed_amount, take_profit, stop_loss)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (p['account_id'], p['ticker'], p['side'], p['shares'], p['target_price'], p['leverage'], p['margin_locked'], borrowed, p['take_profit'], p['stop_loss']))
                    cursor.execute("DELETE FROM pending_orders WHERE id = %s", (p['id'],))
                    conn.commit()

            # --- 2. CHECK ACTIVE POSITIONS (Take Profit / Stop Loss Exits) ---
            cursor.execute("SELECT * FROM positions WHERE take_profit IS NOT NULL OR stop_loss IS NOT NULL")
            positions = cursor.fetchall()

            for pos in positions:
                current_price = get_live_price(pos['ticker'])
                if current_price == 0.0: continue

                trigger_close = False
                if pos['side'] == 'BUY':
                    if pos['take_profit'] and current_price >= pos['take_profit']: trigger_close = True
                    elif pos['stop_loss'] and current_price <= pos['stop_loss']: trigger_close = True
                elif pos['side'] == 'SELL':
                    if pos['take_profit'] and current_price <= pos['take_profit']: trigger_close = True
                    elif pos['stop_loss'] and current_price >= pos['stop_loss']: trigger_close = True

                if trigger_close:
                    pnl = (current_price - pos['entry_price']) * pos['shares'] if pos['side'] == 'BUY' else (pos['entry_price'] - current_price) * pos['shares']
                    cursor.execute("SELECT cash_balance FROM accounts WHERE id = %s", (pos['account_id'],))
                    cash = cursor.fetchone()['cash_balance']
                    cursor.execute("UPDATE accounts SET cash_balance = %s WHERE id = %s", (cash + pos['margin_locked'] + pnl, pos['account_id']))
                    cursor.execute(
                        "INSERT INTO trade_history (account_id, ticker, side, shares, entry_price, close_price, pnl) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (pos['account_id'], pos['ticker'], pos['side'], pos['shares'], pos['entry_price'], current_price, pnl)
                    )
                    cursor.execute("DELETE FROM positions WHERE id = %s", (pos['id'],))
                    conn.commit()

            conn.close()
        except Exception:
            pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create tables using PostgreSQL syntax (SERIAL instead of AUTOINCREMENT)
    cursor.execute("CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, is_guest INTEGER, cash_balance REAL, username TEXT, password_hash TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS positions (id SERIAL PRIMARY KEY, account_id TEXT, ticker TEXT, side TEXT DEFAULT 'BUY', shares INTEGER, entry_price REAL, leverage INTEGER, margin_locked REAL, borrowed_amount REAL, take_profit REAL NULL, stop_loss REAL NULL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS trade_history (id SERIAL PRIMARY KEY, account_id TEXT, ticker TEXT, side TEXT, shares INTEGER, entry_price REAL, close_price REAL, pnl REAL, closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_orders (
            id SERIAL PRIMARY KEY, account_id TEXT, ticker TEXT, side TEXT, shares INTEGER, target_price REAL, 
            leverage INTEGER, margin_locked REAL, take_profit REAL NULL, stop_loss REAL NULL, order_type TEXT
        )
    """)
    conn.commit()
    conn.close()

    monitor_task = asyncio.create_task(market_monitor())
    yield
    monitor_task.cancel()

app = FastAPI(title="TraderForge API", lifespan=lifespan)

class OrderRequest(BaseModel):
    ticker: str
    action: str
    shares: int
    leverage: int
    order_type: str
    entry_price: Optional[float] = None
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None

class CloseRequest(BaseModel):
    position_id: int
    target_price: Optional[float] = None

class CancelPendingRequest(BaseModel):
    order_id: int

class AuthRequest(BaseModel):
    username: str
    password: str

def get_account(request: Request, response: Response) -> str:
    account_id = request.cookies.get("tf_session")
    conn = get_db_connection()
    cursor = conn.cursor()
    if account_id:
        cursor.execute("SELECT id FROM accounts WHERE id = %s", (account_id,))
        if cursor.fetchone():
            conn.close()
            return account_id
    new_id = str(uuid.uuid4())
    cursor.execute("INSERT INTO accounts (id, is_guest, cash_balance) VALUES (%s, 1, 100000.0)", (new_id,))
    conn.commit()
    conn.close()
    response.set_cookie(key="tf_session", value=new_id, httponly=True)
    return new_id

def get_live_price(ticker: str) -> float:
    try: return float(yf.Ticker(ticker).fast_info['lastPrice'])
    except Exception: return 0.0

@app.get("/")
def serve_frontend(): return FileResponse("index.html")

@app.post("/api/register")
def register_account(auth: AuthRequest, response: Response, account_id: str = Depends(get_account)):
    if not auth.username or not auth.password: return {"success": False, "message": "Username and password required."}
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM accounts WHERE username = %s", (auth.username,))
    if cursor.fetchone():
        conn.close()
        return {"success": False, "message": "Username already taken."}
    cursor.execute("UPDATE accounts SET is_guest = 0, username = %s, password_hash = %s WHERE id = %s", (auth.username, hash_password(auth.password), account_id))
    conn.commit()
    conn.close()
    return {"success": True, "message": f"Account claimed! Welcome, {auth.username}."}

@app.post("/api/login")
def login_account(auth: AuthRequest, response: Response):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM accounts WHERE username = %s AND password_hash = %s", (auth.username, hash_password(auth.password)))
    row = cursor.fetchone()
    conn.close()
    if row:
        response.set_cookie(key="tf_session", value=row['id'], httponly=True)
        return {"success": True, "message": f"Logged in successfully."}
    return {"success": False, "message": "Invalid credentials."}

@app.post("/api/logout")
def logout_account(response: Response):
    response.delete_cookie("tf_session")
    return {"success": True, "message": "Logged out successfully."}

@app.get("/api/chart/{ticker}")
def get_chart_data(ticker: str, interval: str = "15m"):
    try:
        stock = yf.Ticker(ticker)
        interval = interval.lower()
        if interval == "1m": period = "5d"
        elif interval in ["5m", "15m"]: period = "1mo"
        elif interval == "1h": interval = "60m"; period = "3mo"
        else: period = "1y"
        
        df = stock.history(period=period, interval=interval)
        if df.empty: return {"success": False, "message": f"No data for {ticker}."}
        
        chart_data = [{"time": int(index.timestamp()), "open": round(row["Open"], 2), "high": round(row["High"], 2), "low": round(row["Low"], 2), "close": round(row["Close"], 2)} for index, row in df.iterrows()]
        return {"success": True, "data": chart_data, "current_price": round(df["Close"].iloc[-1], 2), "company_name": stock.info.get('shortName', ticker.upper())}
    except Exception as e: return {"success": False, "message": str(e)}

@app.get("/api/quote/{ticker}")
def get_live_quote(ticker: str):
    price = get_live_price(ticker)
    return {"success": True, "price": price} if price > 0.0 else {"success": False}

@app.get("/api/account")
def get_account_stats(account_id: str = Depends(get_account)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accounts WHERE id = %s", (account_id,))
    account = cursor.fetchone()

    cursor.execute("SELECT SUM(margin_locked) as m FROM positions WHERE account_id = %s", (account_id,))
    pos_margin = cursor.fetchone()['m'] or 0.0
    cursor.execute("SELECT SUM(margin_locked) as m FROM pending_orders WHERE account_id = %s", (account_id,))
    pend_margin = cursor.fetchone()['m'] or 0.0
    conn.close()
    
    return {"success": True, "cash": account['cash_balance'], "margin_used": pos_margin + pend_margin, "is_guest": bool(account['is_guest']), "username": account['username']}

@app.post("/api/order")
def place_order(order: OrderRequest, account_id: str = Depends(get_account)):
    ticker = order.ticker.upper()
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Route 1: Pending Order (Limit/Stop)
        if order.order_type != "market":
            if not order.entry_price: return {"success": False, "message": "Target entry price is required."}
            margin_locked = (order.shares * order.entry_price) / order.leverage
            cursor.execute("SELECT cash_balance FROM accounts WHERE id = %s", (account_id,))
            if cursor.fetchone()['cash_balance'] < margin_locked: return {"success": False, "message": "Insufficient cash."}

            cursor.execute("UPDATE accounts SET cash_balance = cash_balance - %s WHERE id = %s", (margin_locked, account_id))
            cursor.execute("""
                INSERT INTO pending_orders (account_id, ticker, side, shares, target_price, leverage, margin_locked, take_profit, stop_loss, order_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (account_id, ticker, order.action, order.shares, order.entry_price, order.leverage, margin_locked, order.take_profit, order.stop_loss, order.order_type))
            conn.commit()
            return {"success": True, "message": f"Pending {order.action} Limit placed for {order.shares} shares at ${order.entry_price:.2f}."}

        # Route 2: Active Market Order
        live_price = get_live_price(ticker)
        if live_price == 0.0: return {"success": False, "message": f"Could not fetch price for {ticker}."}
        margin_locked = (order.shares * live_price) / order.leverage

        cursor.execute("SELECT cash_balance FROM accounts WHERE id = %s", (account_id,))
        if cursor.fetchone()['cash_balance'] < margin_locked: return {"success": False, "message": "Insufficient cash."}

        cursor.execute("UPDATE accounts SET cash_balance = cash_balance - %s WHERE id = %s", (margin_locked, account_id))
        cursor.execute("""
            INSERT INTO positions (account_id, ticker, side, shares, entry_price, leverage, margin_locked, borrowed_amount, take_profit, stop_loss)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (account_id, ticker, order.action, order.shares, live_price, order.leverage, margin_locked, (order.shares * live_price) - margin_locked, order.take_profit, order.stop_loss))
        conn.commit()
        return {"success": True, "message": f"Market {order.action} executed: {order.shares} shares of {ticker} at ${live_price:.2f}."}
    except Exception as e:
        conn.rollback()
        return {"success": False, "message": str(e)}
    finally:
        conn.close()

@app.get("/api/positions")
def get_positions(account_id: str = Depends(get_account)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM positions WHERE account_id = %s", (account_id,))
    positions = [dict(row) for row in cursor.fetchall()]
    conn.close()
    for pos in positions: pos['live_price'] = get_live_price(pos['ticker'])
    return {"success": True, "positions": positions}

@app.get("/api/pending")
def get_pending(account_id: str = Depends(get_account)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pending_orders WHERE account_id = %s", (account_id,))
    pending = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"success": True, "pending": pending}

@app.get("/api/history")
def get_history(account_id: str = Depends(get_account)):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trade_history WHERE account_id = %s ORDER BY closed_at DESC", (account_id,))
    history = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"success": True, "history": history}

@app.post("/api/close")
def close_position(req: CloseRequest, account_id: str = Depends(get_account)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM positions WHERE id = %s AND account_id = %s", (req.position_id, account_id))
        pos = cursor.fetchone()
        if not pos: return {"success": False, "message": "Position not found."}

        if req.target_price:
            if pos['side'] == 'BUY': cursor.execute("UPDATE positions SET take_profit = %s WHERE id = %s", (req.target_price, req.position_id))
            else: cursor.execute("UPDATE positions SET stop_loss = %s WHERE id = %s", (req.target_price, req.position_id))
            conn.commit()
            return {"success": True, "message": f"Limit Close Order set at ${req.target_price:.2f}."}

        current_price = get_live_price(pos['ticker'])
        if current_price == 0.0: return {"success": False, "message": "Failed to fetch live price."}
        
        pnl = (current_price - pos['entry_price']) * pos['shares'] if pos['side'] == 'BUY' else (pos['entry_price'] - current_price) * pos['shares']
        cursor.execute("UPDATE accounts SET cash_balance = cash_balance + %s + %s WHERE id = %s", (pos['margin_locked'], pnl, account_id))
        cursor.execute("INSERT INTO trade_history (account_id, ticker, side, shares, entry_price, close_price, pnl) VALUES (%s, %s, %s, %s, %s, %s, %s)", (account_id, pos['ticker'], pos['side'], pos['shares'], pos['entry_price'], current_price, pnl))
        cursor.execute("DELETE FROM positions WHERE id = %s", (req.position_id,))
        conn.commit()
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        return {"success": True, "message": f"Closed {pos['ticker']} at ${current_price:.2f}. Profit: {pnl_str}"}
    except Exception as e:
        conn.rollback()
        return {"success": False, "message": str(e)}
    finally:
        conn.close()

@app.post("/api/cancel_pending")
def cancel_pending(req: CancelPendingRequest, account_id: str = Depends(get_account)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM pending_orders WHERE id = %s AND account_id = %s", (req.order_id, account_id))
        order = cursor.fetchone()
        if not order: return {"success": False, "message": "Order not found."}
        cursor.execute("UPDATE accounts SET cash_balance = cash_balance + %s WHERE id = %s", (order['margin_locked'], account_id))
        cursor.execute("DELETE FROM pending_orders WHERE id = %s", (req.order_id,))
        conn.commit()
        return {"success": True, "message": "Pending order canceled. Margin refunded."}
    except Exception as e:
        conn.rollback()
        return {"success": False, "message": str(e)}
    finally:
        conn.close()

@app.post("/api/reset")
def reset_account(account_id: str = Depends(get_account)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM positions WHERE account_id = %s", (account_id,))
        cursor.execute("DELETE FROM pending_orders WHERE account_id = %s", (account_id,))
        cursor.execute("DELETE FROM trade_history WHERE account_id = %s", (account_id,))
        cursor.execute("UPDATE accounts SET cash_balance = 100000.0 WHERE id = %s", (account_id,))
        conn.commit()
        return {"success": True, "message": "Account fully reset to $100,000."}
    except Exception:
        conn.rollback()
        return {"success": False, "message": "Failed to reset account."}
    finally:
        conn.close()

@app.get("/api/leaderboard")
def get_leaderboard(period: str = "month"):
    conn = get_db_connection()
    cursor = conn.cursor()
    time_filter = "30 days" if period == "month" else "365 days"
    
    query = f"""
        SELECT 
            a.username,
            ROUND(SUM(th.pnl)::numeric, 2) AS total_pnl,
            COUNT(th.id) AS total_trades,
            ROUND((CAST(SUM(CASE WHEN th.pnl > 0 THEN 1 ELSE 0 END) AS NUMERIC) / COUNT(th.id)) * 100, 1) AS win_rate
        FROM accounts a
        JOIN trade_history th ON a.id = th.account_id
        WHERE a.username IS NOT NULL 
          AND th.closed_at >= NOW() - INTERVAL '{time_filter}'
        GROUP BY a.id, a.username
        HAVING COUNT(th.id) > 0
        ORDER BY total_pnl DESC
        LIMIT 25
    """
    cursor.execute(query)
    leaders = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"success": True, "leaders": leaders}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
