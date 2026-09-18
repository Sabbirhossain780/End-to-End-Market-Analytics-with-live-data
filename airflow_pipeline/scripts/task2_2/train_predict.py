import os
import pandas as pd
from sklearn.linear_model import Ridge

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
FEATURES = ['Open', 'High', 'Low', 'Close', 'Volume']
RIDGE_ALPHA = 1.0  # small L2 regularization to counter OHLC multicollinearity

def load_company_df(ticker):
    path = os.path.join(DATA_DIR, f'{ticker}.csv')
    df = pd.read_csv(path)
    df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.date
    df = df[['Date'] + FEATURES].sort_values('Date').reset_index(drop=True)
    return df

def build_xy(df):
    X = df[FEATURES].iloc[:-1].reset_index(drop=True)
    y = df['High'].iloc[1:].reset_index(drop=True)
    dates_for_y = df['Date'].iloc[1:].reset_index(drop=True)
    return X, y, dates_for_y

def walk_forward_errors(ticker):
    df = load_company_df(ticker)
    X, y, dates_for_y = build_xy(df)

    n = len(X)
    errors = {}

    for i in [5, 4, 3, 2, 1]:
        train_end = n - i
        test_idx = n - i
        if train_end <= 0 or test_idx >= n:
            print(f"{ticker}: not enough data for i={i}, skipping")
            continue

        X_train, y_train = X.iloc[:train_end], y.iloc[:train_end]
        model = Ridge(alpha=RIDGE_ALPHA, solver='svd')
        model.fit(X_train, y_train)

        X_test = X.iloc[[test_idx]]
        y_actual = y.iloc[test_idx]
        y_pred = model.predict(X_test)[0]

        rel_error = (y_pred - y_actual) / y_actual
        pred_date = dates_for_y.iloc[test_idx]
        errors[pred_date] = rel_error

        print(f"{ticker} i={i}: trained on {train_end} rows, predicted {pred_date} -> pred={y_pred:.4f} actual={y_actual:.4f} rel_error={rel_error:.6f}")

    return errors

if __name__ == '__main__':
    for ticker in ['AAPL', 'GOOGL', 'META', 'MSFT', 'AMZN']:
        walk_forward_errors(ticker)
        print()
