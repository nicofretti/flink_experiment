import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import re


def plot_q2_result(path):
    f, ax = plt.subplots(figsize=(18, 10))
    sns.set_theme(style="white")
    df = pd.read_csv(path, names=["year", "month", "state", "count"])
    # Concat year and month
    df["year_month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    # Remove where state is 0
    df = df[df["state"] != "0"]
    # See uniques states
    data = df.pivot("year_month", "state", "count")

    # Order the year_month column
    data = data.reindex(sorted(data.columns), axis=1)
    cmap = sns.diverging_palette(df["count"].max(), df["count"].min(), as_cmap=True)
    sns.heatmap(data, cmap=cmap, center=0, ax=ax, square=True, linewidths=.2)
    # Normalize each column by the max value
    f, ax = plt.subplots(figsize=(18, 10))
    data = data.div(data.max(axis=0), axis=1)
    # Make a heatmap of the number of flights on x-axis the month and year and on y-axis the state
    sns.heatmap(data, center=0, ax=ax, square=True, linewidths=.2)


def plot_q1_result(path):
    f, ax = plt.subplots(figsize=(7, 5))
    df = pd.read_csv(path, names=["day", "avg_delay_minutes"])
    df["avg_delay_minutes"] = df["avg_delay_minutes"] * -1
    # Order the day column
    df = df.reindex(sorted(df.columns), axis=1)
    # Renaming the day column to be more readable
    df["day"] = df["day"].replace(
        {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"})

    # Make a barplot of the average delay on x-axis the day and on y-axis the average delay
    sns.barplot(x="day", y="avg_delay_minutes", data=df, ax=ax)


def plot_q4_result(path):
    df = pd.read_csv(path, names=["tailnum", "year", "month", "day", "result"])
    # Make the column result as an array split by space
    df["result"] = df["result"].str.split(" ")
    # Remove all element that are negative numbers with regex
    df["result"] = df["result"].apply(lambda x: [i for i in x if not re.match(r"-\d+", i)])
    # Take only unique values from result column
    df["result"] = df["result"].apply(lambda x: list(set(x)))
    # Create a map with the count of each unique value in result column
    df["result"] = df["result"].apply(lambda x: {i: x.count(i) for i in x})
    # Sum all the values in map grouped by year
    map = {}
    for index, row in df.iterrows():
        if row["year"] in map:
            for key, value in row["result"].items():
                if key == "":
                    continue
                map[row["year"]][key] = map[row["year"]].get(key, 0) + value
        else:
            map[row["year"]] = row["result"]
    # For each year get the top 10 highest values
    for key, value in map.items():
        map[key] = sorted(value.items(), key=lambda x: x[1], reverse=True)[:10]
    # Make colors for each key
    keys = set()
    for key, value in map.items():
        for k, v in value:
            keys.add(k)
    colors = sns.color_palette("hls", len(keys))
    # Transform the set to array
    keys = list(keys)
    # Make a for each year a barplot of the top 10 highest key values at y-axis and the key values at x-axis
    for key, value in map.items():
        f, ax = plt.subplots()
        # Make the bar plot using the same color for each key
        sns.barplot(x=[i[0] for i in value], y=[i[1] for i in value], ax=ax,
                    palette=[colors[keys.index(i[0])] for i in value])
        # Set the title of the plot
        ax.set_title("Year " + str(key))

if __name__ == "__main__":
    # Read the data
    plot = "q4"
    # Plot the data
    if plot == "q1":
        plot_q1_result("q1.csv")
    if plot == "q3":
        plot_q2_result("q3.csv")
    if plot == "q4":
        plot_q4_result("q4.csv")

    plt.show()
