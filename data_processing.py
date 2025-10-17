import pandas as pd

#process semiconductor data
# raw columns: typeCode,freqCode,refPeriodId,refYear,refMonth,period,reporterCode,reporterISO,reporterDesc,flowCode,flowDesc,partnerCode,partnerISO,partnerDesc,partner2Code,partner2ISO,partner2Desc,classificationCode,classificationSearchCode,isOriginalClassification,cmdCode,cmdDesc,aggrLevel,isLeaf,customsCode,customsDesc,mosCode,motCode,motDesc,qtyUnitCode,qtyUnitAbbr,qty,isQtyEstimated,altQtyUnitCode,altQtyUnitAbbr,altQty,isAltQtyEstimated,netWgt,isNetWgtEstimated,grossWgt,isGrossWgtEstimated,cifvalue,fobvalue,primaryValue,legacyEstimationFlag,isReported,isAggregate

comtrade_raw = "/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/raw/semiconductor_trade.csv"
df1 = pd.read_csv(comtrade_raw)

keep_cols = [
    "refYear", "reporterDesc", "partnerDesc", "flowDesc", "cmdCode", "cmdDesc", "primaryValue", "qty", "qtyUnitAbbr", "isReported", "isAggregate"
]

df_processed = df1[keep_cols].copy()

#rename columns 
df_processed = df_processed.rename(columns={
    "refYear": "Year",
    "reporterDesc": "Reporter",
    "partnerDesc": "Partner",
    "flowDesc": "Flow",
    "cmdCode": "CommodityCode",
    "cmdDesc": "Commodity",
    "primaryValue": "TradeValueUSD",
    "qty": "Quantity",
    "isReported": "ReportedFlag",
    "isAggregate": "AggregateFlag"
})

#fill static columns
df_processed["Reporter"] = "China"
df_processed["Parnter"] = "USA"
df_processed["Flow"] = "Imports"
df_processed["Commodity"] = "Semiconductors"


df1_processed = df_processed.groupby("Year").agg({
    "TradeValueUSD": "sum",
    "Quantity": "sum",
    "Reporter": "first",
    "Partner": "first",
    "Flow": "first",
    "Commodity": "first",
    "ReportedFlag": "first",
    "AggregateFlag": "first"
}).reset_index()

#save
output_path = "/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/processed/semiconductor_trade.csv"
df1_processed.to_csv(output_path, index=False)
print(f"Saved Comtrade Data to {output_path}")

#process macro data
macro_csv = "/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/raw/worldbank_macro.csv" 
df2 = pd.read_csv(macro_csv)

import pandas as pd

df2 = df2.pivot(index=["year", "country"], 
                columns="indicator", 
                values="value"
).reset_index() 

china = df2[df2["country"] == "China"].copy()
usa = df2[df2["country"] == "USA"].copy()

china = china.rename(columns={
    "year": "Year",
    "GDP": "GDP_China",
    "GDP_growth": "GDP_growth_China",
    "Inflation": "Inflation_China"
}).drop(columns=["country"])

usa = usa.rename(columns={
    "year": "Year",
    "GDP": "GDP_USA",
    "GDP_growth": "GDP_growth_USA",
    "Inflation": "Inflation_USA"
}).drop(columns=["country"])

df2 = pd.merge(china, usa, on="Year", how="outer")

output_path = "/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/processed/worldbank_macro_wide.csv"
df2.to_csv(output_path, index=False)

print(f"Saved processed macro data to: {output_path}")

# merge two datasets 

df1 = pd.read_csv("/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/processed/semiconductor_trade.csv")
df2 = pd.read_csv("/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/processed/worldbank_macro_wide.csv")

merged_dataset = pd.merge(df1, df2, how="left", on="Year")

output_path = "/Users/laurelallen/Desktop/Economic-warfare-portfolio/data/processed/merged_dataset.csv"
merged_dataset.to_csv(output_path, index=False)

print(f"Merged dataset saved to {output_path}")