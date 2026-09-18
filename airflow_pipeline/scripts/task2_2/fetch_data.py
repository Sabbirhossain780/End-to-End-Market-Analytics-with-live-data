import os
import yfinance as yf

COMPANIES = ['AAPL', 'GOOGL', 'META', 'MSFT', 'AMZN']
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def fetch_all():
    os.makedirs(DATA_DIR, exist_ok=True)
    for ticker in COMPANIES:
        df = yf.Ticker(ticker).history(period='max')
        df = df.reset_index()
        out_path = os.path.join(DATA_DIR, f'{ticker}.csv')
        df.to_csv(out_path, index=False)
        print(f"{ticker}: {len(df)} rows saved to {out_path}")

if __name__ == '__main__':
    fetch_all()
