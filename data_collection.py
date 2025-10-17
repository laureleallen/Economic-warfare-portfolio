import requests
import pandas as pd
import time

### COMTRADE SEMICONDUCTOR DATA ###
comtrade_url = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

years = list(range(2010, 2025))
headers = {"Ocp-Apim-Subscription-Key": "e583fa5e946048adbbf90d58493582c1"}
all_data = []

print("Retrieving Comtrade data...")

for year in years:
    params = {
        "reporterCode": "156",  # China
        "partnerCode": "842",   # USA
        "period": str(year),
        "flowCode": "M",        # Imports (from US to China)
        "cmdCode": "8542",       # Semiconductors
        "typeCode": "C",
        "motCode": "0" #hopefully to eliminate duplicate entries
    }

    print(f"Retrieving data for {year}...")
    response = requests.get(comtrade_url, params=params, headers=headers, timeout=60)

    if response.status_code == 200:
        data = response.json()
        #only proceed if if 'data' exists AND its not empty
        if "data" in data and data["data"]:
            all_data.extend(data["data"])
            print(f"{year}: {len(data['data'])} records retrieved")
        else:
            print(f"{year}: No data found.")
    else:
        print(f"{year}: Error {response.status_code}")
    time.sleep(1)

# Save raw data
if all_data:
    df = pd.DataFrame(all_data)
    print(f"\nTotal records collected: {len(df)}")

    output_path = "data/raw/semiconductor_trade.csv"
    df.to_csv(output_path, index=False)
    print(f"Raw semiconductor data saved to {output_path}")
else:
    print("No data collected.")

### WORLD BANK: GDP, GDP GROWTH, INFLATION ###

worldbank_url = "https://api.worldbank.org/v2/country/{}/indicator/{}?format=json&date=2010:2024&per_page=1000"

countries = {"China": "CN", "USA": "US"}
indicators = {
    "GDP": "NY.GDP.MKTP.CD",
    "GDP_growth": "NY.GDP.MKTP.KD.ZG",
    "Inflation": "FP.CPI.TOTL.ZG"
}

rows = []

print("\nRetrieving World Bank data...")
for country_name, country_code in countries.items():
    for label, indicator_code in indicators.items():
        url2 = worldbank_url.format(country_code, indicator_code)
        response_wb = requests.get(url2, timeout=60)
        if response_wb.status_code == 200:
            data2 = response_wb.json()
            if len(data2) > 1 and data2[1] is not None:
                for entry in data2[1]:
                    rows.append({
                        "country": country_name,
                        "indicator": label,
                        "year": entry.get("date"),
                        "value": entry.get("value")
                    })
            else:
                print(f"No data found for {country_name} - {label}")
        else:
            print(f"Error retrieving {label} for {country_name}: {response_wb.status_code}")

if len(rows) > 0:
    df2 = pd.DataFrame(rows)
    output2_path = "data/processed/worldbank_macro.csv"
    df2.to_csv(output2_path, index=False)
    print(f"World Bank data saved to {output2_path}")
else:
    print("No World Bank data collected.")
