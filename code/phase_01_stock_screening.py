"""
Leo Howard, Copyright 2025.

Set-up virtual venv:
python3 -m venv finance_llm_python_venv

Activate virtual env in Windows:
finance_llm_python_venv/Scripts/activate
python -c "import sys; print(sys.executable)"

Install libs:
pip install pandas yfinance requests python-edgar numpy lxml html5lib

python code/phase_01_stock_screening.py
"""

import pandas as pd
import yfinance as yf
import numpy as np
import time
from datetime import datetime, timedelta
import os

# Screening weights configuration
SCREENING_WEIGHTS = {
    'volume_score': 0.20,
    'price_change_score': 0.25,
    'relative_strength_score': 0.20,
    'historical_vol_score': 0.15,    # Higher vol = more potential for big moves
    'options_score': 0.10,
    'short_squeeze_score': 0.10
}

# Time delay to respect API limits
def time_delay():
    time.sleep(0.1)

# Function to fetch S&P 500 tickers
def get_sp500_tickers():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        print("✅ Wikipedia tables found:", len(tables))
        df = tables[0]

        # Handle possible column name differences
        if 'Symbol' in df.columns:
            col = 'Symbol'
        elif 'Ticker symbol' in df.columns:
            col = 'Ticker symbol'
        else:
            raise ValueError("No ticker column found in Wikipedia table")

        return df[col].tolist()

    except Exception as e:
        print("❌ Failed to fetch S&P 500 tickers:", e)
        return []

# Create the initial DataFrame and save it to a CSV file
def create_dataframe_with_tickers(test_mode=False, test_count=10):
    tickers = get_sp500_tickers()
    if not tickers:
        return
    
    # Ensure data directory exists
    if not os.path.exists("data"):
        os.makedirs("data")
    
    # Use subset for testing if requested
    if test_mode:
        tickers = tickers[:test_count]
    
    df = pd.DataFrame(tickers, columns=["Ticker"])
    output_path = "data/01_va_sp500_tickers.csv"
    df.to_csv(output_path, index=False)

# Filter by net income:
def filter_by_net_income():
    """
    Filters tickers from a CSV file based on two criteria for net income over the last 3 years:
    1. Positive net income each year.
    2. Non-decreasing net income.
    The filtered tickers are saved to a new CSV file: data/va_sp500_filtered_by_net_income.csv
    """
    input_path = "data/01_va_sp500_tickers.csv"
    output_path = "data/02_va_sp500_filtered_by_net_income.csv"

    if not os.path.exists(input_path):
        print("❌ Input file not found:", input_path)
        # Create an empty CSV if the input file doesn't exist to prevent further errors
        pd.DataFrame(columns=['Ticker']).to_csv(output_path, index=False)
        return

    df = pd.read_csv(input_path)
    tickers_to_keep = []
    print("⏳ Filtering stocks by net income for the last 3 years...")

    for idx, ticker in enumerate(df['Ticker']):
        # Using a try-except block to gracefully handle any API or data fetching issues
        try:
            time_delay() # Delay to respect API limits
            stock = yf.Ticker(ticker)
            financials_df = stock.financials

            # Check if 'Net Income' exists in the index and if there's enough data
            if 'Net Income' not in financials_df.index or len(financials_df.columns) < 3:
                continue

            # Get the net income for the last 3 fiscal periods (most recent 3 columns)
            net_income_series = financials_df.loc['Net Income'].iloc[:3]

            # Condition 1: All net income values must be positive
            is_positive = (net_income_series > 0).all()

            # Condition 2: Net income must not decrease (from oldest to newest of the three)
            # yfinance financials are usually in descending order of year, so [0] is most recent, [2] is oldest
            is_non_decreasing = (net_income_series.iloc[2] <= net_income_series.iloc[1]) and \
                                (net_income_series.iloc[1] <= net_income_series.iloc[0])

            if is_positive and is_non_decreasing:
                tickers_to_keep.append(ticker)

        except Exception as e:
            # Print a message for any specific ticker that causes an error, then continue
            print(f"⚠️ Skipping {ticker} due to an error during net income check: {e}")
            continue

    # Save the filtered tickers to a new CSV file
    filtered_df = pd.DataFrame(tickers_to_keep, columns=['Ticker'])
    filtered_df.to_csv(output_path, index=False)
    print(f"✅ Filtered down to {len(tickers_to_keep)} tickers, saved to {output_path}")

# Function to get all screening metrics in one API call
def get_all_screening_metrics(ticker):
    """
    Get all screening metrics for a ticker in a single function to minimize API calls:
    - Historical volatility (1 year)
    - Volume metrics (current vs 20-day average)
    - Price changes (1d, 5d, 20d)
    - Options metrics (simplified)
    - Short interest metrics
    """
    try:
        time_delay()
        stock = yf.Ticker(ticker)
        
        # Get both 1 year and 30 day history in one call each
        hist_1y = stock.history(period="1y")
        hist_30d = stock.history(period="30d") 
        info = stock.info
        
        # Initialize return values
        results = {}
        
        # 1. Historical Volatility (1 year)
        if not hist_1y.empty and len(hist_1y) > 50:
            hist_1y['Log Returns'] = np.log(hist_1y['Close'] / hist_1y['Close'].shift(1))
            hist_vol = hist_1y['Log Returns'].std() * np.sqrt(252)
            results['hist_vol_1y'] = round(hist_vol, 2)
        else:
            results['hist_vol_1y'] = np.nan
        
        # 2. Volume Metrics (30 day)
        if not hist_30d.empty and len(hist_30d) >= 20:
            current_volume = hist_30d['Volume'].iloc[-1]
            avg_volume_20d = hist_30d['Volume'].rolling(20).mean().iloc[-1]
            volume_ratio = current_volume / avg_volume_20d if avg_volume_20d > 0 else 0
            results['volume_ratio'] = round(volume_ratio, 2)
            results['avg_volume_20d'] = int(avg_volume_20d)
        else:
            results['volume_ratio'] = np.nan
            results['avg_volume_20d'] = np.nan
        
        # 3. Price Change Metrics (30 day)
        if not hist_30d.empty and len(hist_30d) >= 21:
            current_price = hist_30d['Close'].iloc[-1]
            price_1d = hist_30d['Close'].iloc[-2] if len(hist_30d) >= 2 else current_price
            price_5d = hist_30d['Close'].iloc[-6] if len(hist_30d) >= 6 else current_price
            price_20d = hist_30d['Close'].iloc[-21] if len(hist_30d) >= 21 else current_price
            
            results['change_1d'] = round(((current_price - price_1d) / price_1d * 100), 2) if price_1d > 0 else 0
            results['change_5d'] = round(((current_price - price_5d) / price_5d * 100), 2) if price_5d > 0 else 0
            results['change_20d'] = round(((current_price - price_20d) / price_20d * 100), 2) if price_20d > 0 else 0
        else:
            results['change_1d'] = np.nan
            results['change_5d'] = np.nan
            results['change_20d'] = np.nan
        
        # 4. Options Metrics (from info) - FIXED VERSION
        try:
            stock = yf.Ticker(ticker)
            # Pick nearest expiry (first available)
            expirations = stock.options
            if expirations:
                exp_date = expirations[0]
                chain = stock.option_chain(exp_date)
                
                # Handle NaN volumes properly
                call_volume = chain.calls['volume'].fillna(0).sum()
                put_volume = chain.puts['volume'].fillna(0).sum()
                total_opt_volume = call_volume + put_volume
                
                # Get average stock volume
                hist = stock.history(period="5d")
                avg_stock_volume = hist['Volume'].mean() if not hist.empty else 1
                
                # Only calculate if there's actual volume
                if total_opt_volume > 0:
                    options_proxy = total_opt_volume / max(avg_stock_volume, 1)
                    put_call_proxy = put_volume / max(call_volume, 1)
                else:
                    options_proxy = 0
                    put_call_proxy = 1  # neutral ratio
            else:
                options_proxy = None
                put_call_proxy = None
        except Exception as e:
            options_proxy = None
            put_call_proxy = None
            results['options_error'] = str(e)

        # Save into results dict
        results['options_proxy'] = round(options_proxy, 4) if options_proxy is not None else None
        results['put_call_proxy'] = round(put_call_proxy, 4) if put_call_proxy is not None else None
        
        # 5. Short Interest Metrics (from info)
        results['short_ratio'] = round(info.get('shortRatio', 0), 2)
        results['short_percent'] = round(info.get('shortPercentOfFloat', 0), 2)
        
        return results
        
    except Exception:
        return {
            'hist_vol_1y': np.nan, 'volume_ratio': np.nan, 'avg_volume_20d': np.nan,
            'change_1d': np.nan, 'change_5d': np.nan, 'change_20d': np.nan,
            'options_proxy': np.nan, 'put_call_proxy': np.nan,
            'short_ratio': np.nan, 'short_percent': np.nan
        }

# Function to normalize scores using percentile ranking
def normalize_score(values):
    """Convert values to 0-100 percentile scores"""
    values = pd.Series(values)
    return values.rank(pct=True) * 100

# Function to collect all screening metrics in one pass
def add_all_screening_metrics():
    input_path = "data/02_va_sp500_filtered_by_net_income.csv"
    
    if not os.path.exists(input_path):
        return
    
    df = pd.read_csv(input_path)
    
    # Initialize all columns
    all_columns = [
        'Historical Vol Past 1 Year', 'Volume_Ratio', 'Avg_Volume_20d', 
        'Price_Change_1d', 'Price_Change_5d', 'Price_Change_20d',
        'Options_Proxy', 'Put_Call_Proxy', 'Short_Ratio', 'Short_Percent'
    ]
    
    for col in all_columns:
        df[col] = np.nan
    
    # Fetch all metrics for each ticker in one function call
    for idx, ticker in enumerate(df['Ticker']):
        # Get all metrics in one call
        metrics = get_all_screening_metrics(ticker)
        
        # Assign all values
        df.at[idx, 'Historical Vol Past 1 Year'] = metrics['hist_vol_1y']
        df.at[idx, 'Volume_Ratio'] = metrics['volume_ratio']
        df.at[idx, 'Avg_Volume_20d'] = metrics['avg_volume_20d']
        df.at[idx, 'Price_Change_1d'] = metrics['change_1d']
        df.at[idx, 'Price_Change_5d'] = metrics['change_5d']
        df.at[idx, 'Price_Change_20d'] = metrics['change_20d']
        df.at[idx, 'Options_Proxy'] = metrics['options_proxy']
        df.at[idx, 'Put_Call_Proxy'] = metrics['put_call_proxy']
        df.at[idx, 'Short_Ratio'] = metrics['short_ratio']
        df.at[idx, 'Short_Percent'] = metrics['short_percent']
    
    output_path = "data/03_va_sp500_raw_screening_data.csv"
    df.to_csv(output_path, index=False)

# Function to calculate weighted composite scores
def calculate_screening_scores():
    """
    Calculate screening scores using the single weight configuration.
    """
    input_path = "data/03_va_sp500_raw_screening_data.csv"
    
    if not os.path.exists(input_path):
        return
    
    df = pd.read_csv(input_path)
    
    # Get S&P 500 benchmark return (simplified - 20 day change)
    try:
        spy = yf.Ticker("SPY")
        spy_hist = spy.history(period="30d")
        spy_return_20d = ((spy_hist['Close'].iloc[-1] - spy_hist['Close'].iloc[-21]) / spy_hist['Close'].iloc[-21] * 100)
    except:
        spy_return_20d = 2.0  # Default benchmark
    
    # Calculate individual scores (0-100 scale)
    df['Volume_Score'] = normalize_score(df['Volume_Ratio'].fillna(0))
    
    # Price momentum score (weighted combination of timeframes)
    price_composite = (df['Price_Change_1d'].fillna(0) * 0.5 + 
                      df['Price_Change_5d'].fillna(0) * 0.3 + 
                      df['Price_Change_20d'].fillna(0) * 0.2)
    df['Price_Change_Score'] = normalize_score(price_composite)
    
    # Relative strength score
    relative_strength = df['Price_Change_20d'].fillna(0) - spy_return_20d
    df['Relative_Strength_Score'] = normalize_score(relative_strength)
    
    # Historical volatility score (higher vol = higher score for potential big moves)
    df['Historical_Vol_Score'] = normalize_score(df['Historical Vol Past 1 Year'].fillna(0))
    
    # Options score (simplified)
    options_composite = (normalize_score(df['Options_Proxy'].fillna(0)) * 0.7 +
                    normalize_score(df['Put_Call_Proxy'].fillna(1)) * 0.3)
    df['Options_Score'] = options_composite
    
    # Short squeeze score
    squeeze_composite = (normalize_score(df['Short_Percent'].fillna(0)) * 0.5 + 
                        normalize_score(df['Short_Ratio'].fillna(0)) * 0.3 +
                        normalize_score(df['Price_Change_5d'].fillna(0)) * 0.2)
    df['Short_Squeeze_Score'] = squeeze_composite
    
    # Calculate weighted composite score
    df['Composite_Score'] = (
        df['Volume_Score'] * SCREENING_WEIGHTS['volume_score'] +
        df['Price_Change_Score'] * SCREENING_WEIGHTS['price_change_score'] +
        df['Relative_Strength_Score'] * SCREENING_WEIGHTS['relative_strength_score'] +
        df['Historical_Vol_Score'] * SCREENING_WEIGHTS['historical_vol_score'] +
        df['Options_Score'] * SCREENING_WEIGHTS['options_score'] +
        df['Short_Squeeze_Score'] * SCREENING_WEIGHTS['short_squeeze_score']
    )
    
    # Rank stocks by composite score
    df['Rank'] = df['Composite_Score'].rank(ascending=False)
    df = df.sort_values('Composite_Score', ascending=False)
    
    output_path = "data/04_va_sp500_screening_results.csv"
    df.to_csv(output_path, index=False)

# Add a historical volatility column next to tickers (DEPRECATED - now done in add_all_screening_metrics):
def add_historical_vol():
    pass

def main(test_mode=False, test_count=10):
    print("---------------- Started... ----------------")
    current_time = datetime.now().strftime("%H:%M:%S")
    print("Current Time:", current_time)

    # Step 1: Create ticker list
    create_dataframe_with_tickers(test_mode=test_mode, test_count=test_count)

    # Step 2: Filter tickers based on net income
    filter_by_net_income()
    
    # Step 3: Collect ALL metrics for the filtered list
    add_all_screening_metrics()

    # Step 4: Calculate composite scores
    calculate_screening_scores()

    current_time = datetime.now().strftime("%H:%M:%S")
    print("Current Time:", current_time)
    print("---------------- Completed.  ----------------")

if __name__ == "__main__":
    # Change these settings as needed:
    
    # For testing with just 10 stocks:
    # main(test_mode=True, test_count=10)
    
    # For full S&P 500 screening:
    main(test_mode=False)
