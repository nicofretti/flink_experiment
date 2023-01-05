import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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
    df["avg_delay_minutes"] = df["avg_delay_minutes"]*-1
    # Order the day column
    df = df.reindex(sorted(df.columns), axis=1)
    # Renaming the day column to be more readable
    df["day"] = df["day"].replace({1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"})

    # Make a barplot of the average delay on x-axis the day and on y-axis the average delay
    sns.barplot(x="day", y="avg_delay_minutes", data=df, ax=ax)


if __name__ == "__main__":
    # Read the data
    plot = "q1"
    # Plot the data
    if plot == "q1":
        plot_q1_result("q1.csv")
    if plot == "q2":
        plot_q2_result("q3.csv")

    plt.show()
