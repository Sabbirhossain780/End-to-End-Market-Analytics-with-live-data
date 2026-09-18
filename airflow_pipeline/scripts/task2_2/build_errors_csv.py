import os
import pandas as pd
from train_predict import walk_forward_errors

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'data')
COMPANIES = ['AAPL', 'GOOGL', 'META', 'MSFT', 'AMZN']
COLUMN_NAMES = {
    'AAPL': 'APPLE',
    'GOOGL': 'GOOGLE',
    'META': 'META',
    'MSFT': 'MICROSOFT',
    'AMZN': 'AMAZON',
}

def build_errors_csv():
    all_errors = {}  # ticker -> {date: error}
    for ticker in COMPANIES:
        all_errors[ticker] = walk_forward_errors(ticker)

    # Union of all dates across tickers, sorted
    all_dates = sorted(set(d for errs in all_errors.values() for d in errs.keys()))

    rows = []
    for d in all_dates:
        row = {'Date': d}
        for ticker in COMPANIES:
            row[COLUMN_NAMES[ticker]] = all_errors[ticker].get(d, None)
        rows.append(row)

    df = pd.DataFrame(rows)
    out_path = os.path.join(OUTPUT_DIR, 'errors.csv')
    df.to_csv(out_path, index=True, index_label='')
    print(f"Errors CSV written to {out_path}")
    print(df)
    return df

if __name__ == '__main__':
    build_errors_csv()
