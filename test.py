import pandas as pd

df = pd.read_csv("timeseries.csv")
### converts the date column to datetime
df["date"] = pd.to_datetime(df["date"])

print(df.info())
print(df.head())
