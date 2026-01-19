#plik odpowiedzialny za wizualizacje
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_monthly_pm25(
    df: pd.DataFrame,
    title: str = "Średnie miesięczne stężenie PM2.5"
):
    """
    Wykres średnich miesięcznych PM2.5
    (oś X: miesiąc, oś Y: średnia PM2.5).
    """
    plt.figure(figsize=(10, 6))

    for (miasto, rok), sub in df.groupby(["Miasto", "Rok"]):
        sub = sub.sort_values("Miesiac")

        plt.plot(
            sub["Miesiac"],
            sub["PM25"],
            marker="o",
            label=f"{miasto} {rok}"
        )

    plt.xlabel("Miesiąc")
    plt.ylabel("Średnie miesięczne PM2.5 [µg/m³]")
    plt.title(title)
    plt.xticks(range(1, 13))
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_pm25_heatmaps(city_month: pd.DataFrame):
    """
    Heatmapy PM2.5:
    oś X – miesiąc
    oś Y – rok
    panele – miasta
    """

    g = sns.FacetGrid(
        city_month,
        col="Miasto",
        col_wrap=6,
        height=2
    )

    def draw_heatmap(data, **kwargs):
        miasto = data["Miasto"].iloc[0]

        df = data.copy()
        df["Miesiac"] = pd.to_numeric(df["Miesiac"])
        df["PM25"] = pd.to_numeric(df["PM25"])

        pivot = df.pivot_table(
            index="Rok",
            columns="Miesiac",
            values="PM25",
            aggfunc="mean"
        )

        sns.heatmap(
            pivot,
            cmap="viridis",
            **kwargs
        )

        plt.title(miasto)

    g.map_dataframe(draw_heatmap)

    plt.tight_layout()
    plt.show()

