import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def plot_q2_result(path):
    f, ax = plt.subplots(figsize=(18, 10))
    sns.set_theme(style="white")
    df = pd.read_csv(path, names=["year", "month", "state", "count"])
    # Concat year and month
    df["year_month"] = df["year"].astype(str) + "-" + df["month"].astype(str)
    # Remove where state is 0
    df = df[df["state"] != "0"]
    # Normalize the count
    df["count"] = df["count"] / df["count"].max()
    # See uniques states
    print(len(df["state"].unique()))
    data = df.pivot("year_month", "state", "count")
    # Make a heatmap of the number of flights on x-axis the month and year and on y-axis the state
    cmap = sns.diverging_palette(df["count"].max(), df["count"].min(), as_cmap=True)
    sns.heatmap(data,
                cmap=cmap,
                center=0,
                square=True, linewidths=.2)
    print(data)


if __name__ == "__main__":
    # Read the data
    plot = "q2"
    # Plot the data
    if plot == "q2":
        plot_q2_result("q2.csv")
    plt.show()
