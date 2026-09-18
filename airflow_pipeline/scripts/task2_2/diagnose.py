import os
import numpy as np
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
FEATURES = ['Open', 'High', 'Low', 'Close', 'Volume']

for ticker in ['AAPL', 'GOOGL', 'META', 'MSFT', 'AMZN']:
    path = os.path.join(DATA_DIR, f'{ticker}.csv')
    df = pd.read_csv(path)
    X = df[FEATURES].iloc[:-1].reset_index(drop=True)

    # Correlation among OHLC (excluding Volume)
    corr = X[['Open', 'High', 'Low', 'Close']].corr()
    min_corr = corr.values[corr.values < 0.999999].min() if (corr.values < 0.999999).any() else corr.values.min()

    # Condition number of design matrix (with intercept column)
    X_design = np.column_stack([np.ones(len(X)), X.values])
    cond_number = np.linalg.cond(X_design)

    print(f"{ticker}: rows={len(X)}, min pairwise OHLC corr={min_corr:.6f}, design matrix condition number={cond_number:.3e}")
