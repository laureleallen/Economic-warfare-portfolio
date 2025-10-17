import pandas as pd

#clean semiconductor data
comtrade_raw = "/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/raw/semiconductor_trade.csv"
df1 = pd.read_csv(comtrade_raw)

df1["Reporter"] = "China"
df1["Partner"] = "USA"
df1["Flow"] = "Imports"
df1["Commodity"] = "Semiconductors"

output_path = "/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/processed/semiconductor_trade.csv"
df1.to_csv(output_path, index=False)
print(f"Saved Comtrade Data to {output_path}")



