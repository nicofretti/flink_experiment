import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv("../datasets/2005.csv", chunksize=20000)
    # Print the column WeatherDelay and DepDelay
    for chunk in df:
        chunk['Delay'] = chunk["ActualElapsedTime"] - chunk["CRSElapsedTime"]
        chunk = chunk[chunk["TailNum"] == "N901DE"]
        chunk = chunk[chunk["DayofMonth"] == 14]
        chunk = chunk[chunk["Month"] == 12]
        print(chunk[['Year', 'Month', 'TailNum', 'DayofMonth', 'DepTime', 'Origin', 'Dest', 'Delay']])
