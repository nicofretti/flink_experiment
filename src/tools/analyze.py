import pandas as pd

if __name__=="__main__":
    df = pd.read_csv("../datasets/2005.csv", chunksize=20000)
    # Print the column WeatherDelay and DepDelay
    for chunk in df:
        chunk['Delay'] = chunk["ActualElapsedTime"] - chunk["CRSElapsedTime"]
        chunk = chunk[chunk["TailNum"]=="N935UA"]
        chunk = chunk[chunk["Delay"]<0]
        print(chunk[['Year','Month','TailNum','DayofMonth','Origin','Dest','Delay']])
        exit(0)