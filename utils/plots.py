from utils.loader import Loader
import os

import plotly.graph_objects as go
import plotly.io as pio

# change theme template for every graph below
pio.templates.default = "plotly_white"


def multi_barplot(
    year1,
    year2,
    colors,
    loader: Loader,
):
    # create a empty plotly.Figure object
    fig = go.Figure()
    # compute the batch number
    n_batch = len(os.listdir("data"))
    # test the color list feed in argument
    # fit well with the batch number
    if n_batch > len(colors):
        raise Exception(
            "The colors list size({})doesn't ".format(len(colors))
            + "fit with the number of data".format(n_batch)
        )
    for i in range(1, n_batch + 1):
        fig.add_traces((add_bar(i, year1, year2, colors[i - 1], loader=loader)))
    fig.update_layout(
        title="Data distribution over years ({} - {})".format(year1, year2),
        xaxis_title="years",
        yaxis_title="title",
        legend_title="Data batch",
    )
    return fig


def barplot_by_decade(df):

    # groupby decade
    df_d = df.groupby(["decade"]).size().reset_index(name="count")

    # create the figure
    fig = go.Figure()

    fig.add_bar(x=df_d.decade, y=df_d["count"], showlegend=False)

    fig.add_scatter(
        x=df_d.decade,
        y=df_d["count"],
        mode="markers+lines",
        name="trend",
        showlegend=False,
    )

    fig.update_layout(
        title="Music release over years", xaxis_title="decade", yaxis_title="release"
    )
    return fig


def add_bar(i, y1, y2, color, loader: Loader):
    df = loader.load_pickle(i)
    df = df[(df.year >= y1) & (df.year <= y2)]
    df_year = df.groupby(["year"]).size().reset_index(name="count")
    new_bar = go.Bar(
        x=df_year.year.values,
        y=df_year["count"].values,
        name="data_" + str(i),
        marker={"color": color},
    )
    new_trend = go.Scatter(
        x=df_year.year.values,
        y=df_year["count"].values,
        mode="lines",
        line={"color": color, "width": 0.5},
        showlegend=False,
    )
    del df_year, df
    return new_bar, new_trend
