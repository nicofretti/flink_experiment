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


if __name__ == "__main__":
    # Read the data
    plot = "q2"
    # Plot the data
    if plot == "q2":
        plot_q2_result("q2.csv")
    plt.show()
