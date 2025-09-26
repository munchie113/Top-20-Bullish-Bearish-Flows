import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np
import time
import requests
import json
import pytz
import yfinance as yf

# Load environment variables
load_dotenv()
API_KEY = os.getenv("UNUSUAL_WHALES_API_KEY")

# Check if API key is set
if not API_KEY:
    print("❌ UNUSUAL_WHALES_API_KEY not found in .env file! Create .env with: UNUSUAL_WHALES_API_KEY=your_key_here | Get key from: https://unusualwhales.com/")
    exit(1)

# Define current time in Eastern Time
et_tz = pytz.timezone('US/Eastern')
now_et = datetime.now(et_tz)
current_time_decimal = (now_et - now_et.replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() / 3600

# Global SPY ticker used for market data
spy = yf.Ticker("SPY")

# Determine if market is open using yfinance
def is_market_open():
    try:
        info = spy.info
        market_state = info.get('regularMarketState', 'UNKNOWN')
        is_open = market_state == 'REGULAR'
        return is_open, f"Market {market_state.lower()}"
    except Exception as e:
        return False, "API error"

#determine the last trading session, used when the market is closed
def get_last_trading_session():
    try:
        hist = spy.history(start=now_et.date() - timedelta(days=5), end=now_et.date(), interval="1d")
        return hist.index[-1].date().strftime("%Y-%m-%d") if not hist.empty else None
    except:
        return None

# Print market status and time
market_status = is_market_open()
is_open, state = market_status
print(f"Current Time: {now_et.strftime('%I:%M %p ET')} | Market: {'Open' if is_open else 'Closed'}")

 # Get the appropriate date for analysis based on market status.
def get_analysis_date():
    if is_open:
        return now_et.strftime("%Y-%m-%d")
    else:
        last_session = get_last_trading_session()
        return last_session if last_session else now_et.strftime("%Y-%m-%d")

def filter_stocks_by_market_cap(stocks_list):
    if not stocks_list:
        return []
    
    filtered_stocks = []
    
    for stock in stocks_list:
        try:
            ticker = stock.get('ticker')
            if not ticker:
                continue
            
            market_cap = float(stock.get('marketcap', 0))
            
            # Filter and categorize by market cap
            if market_cap >= 200_000_000_000:  # Mega-cap (>= $200B)
                category = 'mega_cap'
                min_open_interest = 1000
                min_premium_value = 100000
            elif market_cap >= 10_000_000_000:  # Large-cap ($10B-$200B)
                category = 'large_cap'
                min_open_interest = 500
                min_premium_value = 50000
            elif market_cap >= 2_000_000_000:  # Mid-cap ($2B-$10B)
                category = 'mid_cap'
                min_open_interest = 200
                min_premium_value = 20000
            elif market_cap >= 1_000_000_000:  # Small-cap ($1B-$2B)
                category = 'small_cap'
                min_open_interest = 100
                min_premium_value = 10000
            elif market_cap >= 300_000_000:  # Micro-cap ($300M-$1B)
                category = 'micro_cap'
                min_open_interest = 50
                min_premium_value = 5000
            else:
                continue  # Skip stocks below $300M market cap
            
            filtered_stocks.append({
                'ticker': ticker,
                'market_cap': market_cap,
                'category': category,
                'min_open_interest': min_open_interest,
                'min_premium_value': min_premium_value
            })
            
        except:
            continue
    
    return filtered_stocks
    
#Fetch all analysis data: flows, market caps, and stock info
def fetch_analysis_data(date=None, tickers=None):
    if not date:
        date = get_analysis_date()
    
    # Initialize return data
    flows_data = []
    market_caps = {}
    stock_info = {}
    stocks_universe = []
    filtered_stocks = []
    
    try:
        # 1. Get stocks universe from screener
        screener_url = "https://api.unusualwhales.com/api/screener/stocks"
        headers = {"Authorization": f"Bearer {API_KEY}"}
        response = requests.get(screener_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            stocks = data.get('data', [])
            stocks_universe = stocks  # Store for get_stock_universe functionality
            print(f"📈 Found {len(stocks)} stocks in universe")
            
            # 2. Filter stocks by market cap 
            filtered_stocks = filter_stocks_by_market_cap(stocks)
            print(f"📈 Filtered to {len(filtered_stocks)} qualifying stocks")
            
            # Build market caps dict from filtered stocks
            market_caps = {stock['ticker']: stock['market_cap'] for stock in filtered_stocks}
        else:
            print(f"❌ Screener API failed: {response.status_code}")
        
        # 3. Get options flows ONLY for filtered stocks
        if filtered_stocks:
            qualifying_tickers = [stock['ticker'] for stock in filtered_stocks]
            
            for ticker in qualifying_tickers:
                try:
                    url = f"https://api.unusualwhales.com/api/option-trades/flow-alerts?ticker={ticker}&date={date}"
                    response = requests.get(url, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        flows = data.get('data', [])
                        if flows:
                            print(f"📊 Found {len(flows)} flows for {ticker}")
                        
                        for flow in flows:
                            flow_data = {
                                'ticker': flow.get('ticker'),
                                'call_premium_ask_side': flow.get('call_premium_ask_side', 0),
                                'call_premium_bid_side': flow.get('call_premium_bid_side', 0),
                                'put_premium_ask_side': flow.get('put_premium_ask_side', 0),
                                'put_premium_bid_side': flow.get('put_premium_bid_side', 0),
                                'expiry': flow.get('expiry', ''),
                                'date': flow.get('date', date),
                                'volume': flow.get('volume', 0),
                                'open_interest': flow.get('open_interest', 0),
                                'total_premium': flow.get('total_premium', 0)
                            }
                            flows_data.append(flow_data)
                    
                    time.sleep(0.1)  # Rate limiting
                    
                except Exception as e:
                    continue
        else:
            print("❌ No qualifying stocks found")
        
        # 4. Get stock info for tickers with flow
        flow_tickers = list(set([flow['ticker'] for flow in flows_data]))
        
        for ticker in flow_tickers:
            try:
                url = f"https://api.unusualwhales.com/api/stock/{ticker}/info"
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    stock_info[ticker] = data.get('data', {})
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                continue
            
    except Exception as e:
        pass
    
    return {
        'flows': flows_data,
        'market_caps': market_caps,
        'stock_info': stock_info,
        'stocks_universe': stocks_universe,
        'filtered_stocks': filtered_stocks
    }

def calculate_dte_weight(expiry_date, current_date):
    """Calculate DTE weight based on days to expiration."""
    if not expiry_date or not current_date:
        return 1.0  # Default weight if dates are missing
    
    try:
        # Parse dates
        if isinstance(expiry_date, str):
            expiry = datetime.strptime(expiry_date, '%Y-%m-%d').date()
        else:
            expiry = expiry_date
        
        if isinstance(current_date, str):
            current = datetime.strptime(current_date, '%Y-%m-%d').date()
        else:
            current = current_date
        
        # Calculate DTE
        dte = (expiry - current).days
        
        # Apply weighting based on DTE ranges
        if dte <= 4:  # 0-4 DTE: 1.0
            return 1.0
        elif dte <= 7:  # 5-7 DTE: 0.95
            return 0.95
        elif dte <= 14:  # 8-14 DTE: 0.9
            return 0.9
        elif dte <= 28:  # 15-28 DTE: 0.85
            return 0.85
        elif dte <= 84:  # 29-84 DTE: 0.8
            return 0.8
        elif dte <= 170:  # 85-170 DTE: 0.75
            return 0.75
        elif dte <= 365:  # 171-365 DTE: 0.7
            return 0.7
        else:  # 366+ DTE: 0.65
            return 0.65
            
    except Exception:
        return 1.0  # Default weight if date parsing fails

def robust_zscore(x):
    """Calculate robust z-score using median and MAD."""
    if len(x) == 0:
        return np.zeros_like(x)
    
    median = np.median(x)
    mad = np.median(np.abs(x - median))
    
    if mad == 0:
        return np.zeros_like(x)
    
    return (x - median) / (1.4826 * mad)

def calculate_flow_rankings(data, market_caps=None, top_n=20):
    """Calculate flow rankings from options data."""
    if not data:
        return None, None
    
    # Group by ticker and calculate flows
    flow_data = {}
    
    for option in data:
        try:
            ticker = option.get('ticker')
            if not ticker:
                continue
            
            # Extract flow data
            call_premium_ask_side = option.get('call_premium_ask_side', 0)
            call_premium_bid_side = option.get('call_premium_bid_side', 0)
            put_premium_ask_side = option.get('put_premium_ask_side', 0)
            put_premium_bid_side = option.get('put_premium_bid_side', 0)
            expiry = option.get('expiry', '')
            date = option.get('date', '')
            volume = option.get('volume', 0)
            open_interest = option.get('open_interest', 0)
            
            if ticker not in flow_data:
                flow_data[ticker] = {
                    'bullish_flow': 0,
                    'bearish_flow': 0,
                    'total_volume': 0,
                    'total_open_interest': 0,
                    'market_cap': market_caps.get(ticker, 0) if market_caps else 0
                }
            
            # Calculate DTE weight
            dte_weight = calculate_dte_weight(expiry, date)
            
            # Calculate flows using FS = Σ(Premium × Volume × w(DTE))
            # Bullish = ask side calls + bid side puts
            # Bearish = ask side puts + bid side calls
            bullish_flow = (call_premium_ask_side + put_premium_bid_side) * volume * dte_weight
            bearish_flow = (put_premium_ask_side + call_premium_bid_side) * volume * dte_weight
            
            flow_data[ticker]['bullish_flow'] += bullish_flow
            flow_data[ticker]['bearish_flow'] += bearish_flow
            flow_data[ticker]['total_volume'] += volume
            flow_data[ticker]['total_open_interest'] += open_interest
            
        except Exception as e:
            continue
    
    # Convert to DataFrame
    results = []
    for ticker, data in flow_data.items():
        bullish_flow = data['bullish_flow']
        bearish_flow = data['bearish_flow']
        net_flow = bullish_flow - bearish_flow
        total_volume = data['total_volume']
        total_open_interest = data['total_open_interest']
        market_cap = data['market_cap']
        
        # Calculate relative flow (premium relative to market cap)
        relative_flow = net_flow / market_cap if market_cap > 0 else 0
        
        # Calculate standardized score
        if total_volume > 0:
            standardized_score = (net_flow / total_volume) * np.sqrt(total_volume)
        else:
            standardized_score = 0
        
        results.append({
            'ticker': ticker,
            'bullish_flow': bullish_flow,
            'bearish_flow': bearish_flow,
            'net_flow': net_flow,
            'total_volume': total_volume,
            'total_open_interest': total_open_interest,
            'market_cap': market_cap,
            'relative_flow': relative_flow,
            'standardized_score': standardized_score
        })
    
    # Create DataFrames
    df = pd.DataFrame(results)
    
    if df.empty:
        return None, None
    
    # Separate bullish and bearish
    bullish_df = df[df['net_flow'] > 0].nlargest(top_n, 'standardized_score')
    bearish_df = df[df['net_flow'] < 0].nsmallest(top_n, 'standardized_score')
    
    return bullish_df, bearish_df


def display_market_cap_breakdown(bullish_df, bearish_df, filtered_stocks):
    """Display results broken down by market cap categories."""
    if bullish_df is None or bearish_df is None:
        return
    
    print("\nMARKET CAP BREAKDOWN")
    print("=" * 30)
    
    # Group stocks by category
    categories = {}
    for stock in filtered_stocks:
        cat = stock['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(stock['ticker'])
    
    # Display each category
    for cat in ['micro_cap', 'small_cap', 'mid_cap', 'large_cap', 'mega_cap']:
        tickers = categories.get(cat, [])
        if not tickers:
            continue
            
        bullish_cat = bullish_df[bullish_df['ticker'].isin(tickers)] if not bullish_df.empty else pd.DataFrame()
        bearish_cat = bearish_df[bearish_df['ticker'].isin(tickers)] if not bearish_df.empty else pd.DataFrame()
        
        print(f"\n{cat.replace('_', ' ').title()}: {len(bullish_cat)} bullish, {len(bearish_cat)} bearish")
        
        if not bullish_cat.empty:
            print("Top Bullish:", bullish_cat[['ticker', 'net_flow']].head(2).to_string(index=False))
        if not bearish_cat.empty:
            print("Top Bearish:", bearish_cat[['ticker', 'net_flow']].head(2).to_string(index=False))

def display_rankings(bullish_df, bearish_df):
    """Display the top bullish and bearish flow rankings."""
    if bullish_df is None or bearish_df is None:
        print("No data to display")
        return
    
    print("\nTOP 20 BULLISH FLOW STOCKS")
    print("=" * 50)
    if not bullish_df.empty:
        display_cols = ['ticker', 'bullish_flow', 'bearish_flow', 'net_flow', 'total_volume', 'market_cap', 'relative_flow', 'standardized_score']
        print(bullish_df[display_cols].to_string(index=False, formatters={
            'bullish_flow': '{:,.2f}'.format,
            'bearish_flow': '{:,.2f}'.format,
            'net_flow': '{:,.2f}'.format,
            'total_volume': '{:,.0f}'.format,
            'market_cap': '{:,.2f}'.format,
            'relative_flow': '{:.6f}'.format,
            'standardized_score': '{:.2f}'.format
        }))
    else:
        print("No bullish flows found")
    
    print("\nTOP 20 BEARISH FLOW STOCKS")
    print("=" * 50)
    if not bearish_df.empty:
        display_cols = ['ticker', 'bullish_flow', 'bearish_flow', 'net_flow', 'total_volume', 'market_cap', 'relative_flow', 'standardized_score']
        print(bearish_df[display_cols].to_string(index=False, formatters={
            'bullish_flow': '{:,.2f}'.format,
            'bearish_flow': '{:,.2f}'.format,
            'net_flow': '{:,.2f}'.format,
            'total_volume': '{:,.0f}'.format,
            'market_cap': '{:,.2f}'.format,
            'relative_flow': '{:.6f}'.format,
            'standardized_score': '{:.2f}'.format
        }))
    else:
        print("No bearish flows found")


# Options Flow Analysis - Top 20 Bullish/Bearish Stocks
print("Options Flow Analysis - Top 20 Bullish/Bearish Stocks")

if not API_KEY:
    print("ERROR: No Unusual Whales API key found!")
    exit()

# Get analysis data
analysis_date = get_analysis_date()
analysis_data = fetch_analysis_data(analysis_date)

print(f"📊 Retrieved {len(analysis_data['flows'])} flow records")
print(f"📊 Filtered {len(analysis_data['filtered_stocks'])} stocks")

if not analysis_data['flows']:
    print("❌ No options flow data retrieved")
    print("This could be due to:")
    print("1. API key issues")
    print("2. No trading data for the selected date")
    print("3. Market closed (no options activity)")
    exit()

# Calculate and display results
bullish_df, bearish_df = calculate_flow_rankings(analysis_data['flows'], market_caps=analysis_data['market_caps'])
display_rankings(bullish_df, bearish_df)
display_market_cap_breakdown(bullish_df, bearish_df, analysis_data['filtered_stocks'])

# Save results
if bullish_df is not None and not bullish_df.empty:
    filename = f'bullish_flow_rankings_{analysis_date}.csv'
    bullish_df.to_csv(filename, index=False)
    print(f"💾 Bullish rankings saved to: {filename}")

if bearish_df is not None and not bearish_df.empty:
    filename = f'bearish_flow_rankings_{analysis_date}.csv'
    bearish_df.to_csv(filename, index=False)
    print(f"💾 Bearish rankings saved to: {filename}")

print(f"✅ Analysis completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
