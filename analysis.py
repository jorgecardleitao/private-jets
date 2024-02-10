import duckdb
import matplotlib.pyplot as plt


def set_size(width: float, fraction=1):
    """Set figure dimensions to avoid scaling in LaTeX.

    Parameters
    ----------
    width: float
            Document textwidth or columnwidth in pts
    fraction: float, optional
            Fraction of the width which you wish the figure to occupy

    Returns
    -------
    fig_dim: tuple
            Dimensions of figure in inches
    """
    # Width of figure (in pts)
    fig_width_pt = width * fraction

    # Convert from pt to inches
    inches_per_pt = 1 / 72.27

    # Golden ratio to set aesthetic figure height
    # https://disq.us/p/2940ij3
    golden_ratio = (5**0.5 - 1) / 2

    # Figure width in inches
    fig_width_in = fig_width_pt * inches_per_pt
    return (fig_width_in, fig_width_in * golden_ratio)


FIG_SIZE = set_size(600)


VERSION = "v1"
STATUS_PATH = f"https://private-jets.fra1.digitaloceanspaces.com/leg/{VERSION}/status.json"
LEGS_PATH = f"https://private-jets.fra1.digitaloceanspaces.com/leg/{VERSION}/all.csv"
AIRCRAFT_PATH = "https://private-jets.fra1.digitaloceanspaces.com/private_jets/all.csv"
duckdb.sql(
    f"""
CREATE TEMP TABLE "legs" AS (
SELECT
    *
FROM
    read_csv_auto('{LEGS_PATH}', header = true)
WHERE
    date_part('year', "start") = 2023
)
"""
)


def aircraft():
    x = duckdb.sql(
        f"""
    SELECT
        model,COUNT(*) AS aircraft
    FROM read_csv_auto('{AIRCRAFT_PATH}', header = true)
    GROUP BY model
    ORDER BY aircraft DESC
    """
    ).fetchall()

    t = list(range(len(x)))
    number = [x[1] for x in x]
    percentage = [x[1] / sum(number) * 100 for x in x]

    fig, ax1 = plt.subplots(figsize=FIG_SIZE)

    ax1.set_xlabel("Aircraft model (rank by number)")
    ax1.grid(linestyle="dotted")

    color = "black"
    ax1.set_ylabel("Number of aircraft", color=color)
    ax1.annotate(x[0][0], xy=(1, number[0]))
    ax1.plot(t, number, "o")
    ax1.tick_params(axis="y", labelcolor=color)
    ax1.set_ylim(ymin=0)

    ax2 = ax1.twinx()

    color = "red"
    ax2.set_ylabel("Market share (%)")
    ax2.plot(t, percentage, "o", alpha=1.0)
    ax2.set_ylim(ymin=0)

    ax1.set_zorder(1)  # default zorder is 0 for ax1 and ax2
    ax1.patch.set_visible(False)  # prevents ax1 from hiding ax2
    fig.tight_layout()
    fig.savefig("results/aircraft.png", dpi=300)


def aircraft_by_country():
    x = duckdb.sql(
        f"""
    SELECT
        country,COUNT(*) AS aircraft
    FROM read_csv_auto('{AIRCRAFT_PATH}', header = true)
    GROUP BY country
    ORDER BY aircraft DESC
    """
    ).fetchall()

    t = list(range(len(x)))
    number = [x[1] for x in x]
    percentage = [x[1] / sum(number) * 100 for x in x]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=FIG_SIZE, sharex=True)

    #fig.supylabel("Number of aircraft")
    ax2.set_xlabel("Country (rank by number)")
    ax1.grid(linestyle="dotted")
    ax2.grid(linestyle="dotted")

    ax1.annotate(x[0][0], xy=(t[0] + 1, number[0]))
    for i in range(1, 5):
        ax2.annotate(x[i][0], xy=(t[i] + 1, number[i]))
    ax1.plot(t, number, "o")
    ax2.plot(t, number, "o")
    # ax1.tick_params(axis="y", labelcolor=color)
    ax1.set_ylim(0.6 * sum(number), 1.0 * sum(number))
    ax2.set_ylim(0, 0.04 * sum(number))

    # hide the spines between ax and ax2
    ax1.spines.bottom.set_visible(False)
    ax2.spines.top.set_visible(False)
    ax1.xaxis.tick_top()
    ax1.tick_params(labeltop=False)  # don't put tick labels at the top
    ax2.xaxis.tick_bottom()

    d = 0.5  # proportion of vertical to horizontal extent of the slanted line
    kwargs = dict(
        marker=[(-1, -d), (1, d)],
        markersize=12,
        linestyle="none",
        color="k",
        mec="k",
        mew=1,
        clip_on=False,
    )
    ax1.plot([0, 1], [0, 0], transform=ax1.transAxes, **kwargs)
    ax2.plot([0, 1], [1, 1], transform=ax2.transAxes, **kwargs)

    ax3 = ax1.twinx()

    fig.text(
            0.03,
            0.5,
            "Number of aircraft",
            ha="center",
            va="center",
            rotation="vertical",
        )
    fig.text(
        0.97,
        0.5,
        "Market share (%)",
        ha="center",
        va="center",
        rotation="vertical",
    )

    ax3.plot(t, percentage, "o")
    ax3.set_ylim(60, 100)

    ax4 = ax2.twinx()

    ax4.plot(t, percentage, "o")
    ax4.set_ylim(0.0, 4)

    ax4.set_zorder(1)  # default zorder is 0 for ax1 and ax2
    ax4.patch.set_visible(False)  # prevents ax1 from hiding ax2

    fig.subplots_adjust(hspace=0.05)
    fig.savefig("results/country.png", dpi=300, bbox_inches="tight")

    return


def ranked_legs():
    x = duckdb.sql(
        f"""
    SELECT
        tail_number,COUNT(*)
    FROM "legs"
    GROUP BY tail_number
    ORDER BY COUNT(*) DESC
    """
    ).fetchall()

    plt.figure(figsize=FIG_SIZE)
    plt.plot([x[1] for x in x], ".")
    plt.xlabel("Aircraft (rank)")
    plt.ylabel("number of legs")
    plt.grid(linestyle="dotted")
    plt.tight_layout()
    plt.savefig("results/legs.png", dpi=300)


def ranked_hours():
    x = duckdb.sql(
        f"""
    SELECT
        tail_number,SUM(epoch("end" - "start") / 60 / 60) AS "flying_time_hour"
    FROM "legs"
    GROUP BY tail_number
    ORDER BY "flying_time_hour" DESC
    """
    ).fetchall()

    print("top most hours:", x[:5])
    plt.figure(figsize=FIG_SIZE)
    plt.plot([x[1] for x in x], ".")
    plt.xlabel("Aircraft (rank)")
    plt.ylabel("number of hours")
    plt.grid(linestyle="dotted")

    plt.tight_layout()
    plt.savefig("results/hours.png", dpi=300)


def histogram_hours():

    x = duckdb.sql(
        f"""
    SELECT
        epoch("end" - "start") / 60 / 60 AS "flying_time_hour"
    FROM "legs"
    """
    ).fetchall()

    fig = plt.figure(figsize=FIG_SIZE)
    plt.hist([x[0] for x in x], bins=40)
    plt.xlabel("number of flying hours per leg")
    plt.ylabel("number of legs")
    plt.grid(linestyle="dotted")
    plt.tight_layout()

    ins = fig.axes[0].inset_axes([0.5, 0.5, 0.4, 0.4])
    ins.hist([x[0] for x in x], bins=40)
    ins.set_yscale("log")
    ins.set_xlabel("number of flying hours per leg")
    ins.set_ylabel("number of legs")
    plt.tight_layout()

    plt.savefig("results/hours_histogram.png", dpi=300)


def histogram_emissions():
    x = duckdb.sql(
        f"""
    SELECT
        emissions_kg / 1000
    FROM "legs"
    """
    ).fetchall()

    fig = plt.figure(figsize=FIG_SIZE)
    plt.hist([x[0] for x in x], bins=40)
    plt.xlabel("Emissions (tons of CO2e)")
    plt.ylabel("number of legs")
    plt.grid(linestyle="dotted")

    ins = fig.axes[0].inset_axes([0.5, 0.5, 0.4, 0.4])
    ins.hist([x[0] for x in x], bins=40)
    ins.set_yscale("log")
    ins.set_xlabel("Emissions (tons of CO2e)")
    ins.set_ylabel("number of legs")
    plt.tight_layout()

    plt.savefig("results/emissions_histogram.png", dpi=300)


def other_stats():
    to_process, completed = duckdb.sql(
        f"""SELECT icao_months_to_process,icao_months_processed FROM '{STATUS_PATH}'"""
    ).fetchall()[0]
    percentage = "{:.2f}".format(completed / to_process * 100)

    total_jets = duckdb.sql(
        f"""
    SELECT
        COUNT(*) AS aircrafts
    FROM read_csv_auto('{AIRCRAFT_PATH}', header = true)
    """
    ).fetchall()[0][0]

    counts = duckdb.sql(
        f"""
    SELECT
        COUNT(DISTINCT tail_number), COUNT(*)
    FROM "legs"
    """
    ).fetchall()[0]

    with open("results/state.tex", "w") as f:
        f.write(
            f"""\
\\newcommand{{\\percentagedone}}[0]{{{percentage}}}
\\newcommand{{\\totalaircraft}}[0]{{{counts[0]}}}
\\newcommand{{\\totallegs}}[0]{{{counts[1]}}}
\\newcommand{{\\totaljets}}[0]{{{total_jets}}}
    """
        )


def per_month():
    x = duckdb.sql(
        f"""
    SELECT
        date_trunc('month', "start") AS "month"
        , SUM("emissions_kg") / 1000 / 1000 / 1000 AS "emissions_mega_tons"
    FROM "legs"
    GROUP BY "month"
    ORDER BY "month"
    """
    ).fetchall()

    t = [x[0] for x in x]
    emissions = [x[1] for x in x]

    fig, ax1 = plt.subplots(figsize=FIG_SIZE)

    ax1.set_xlabel("Time (month)")
    ax1.xaxis_date()
    ax1.set_axisbelow(True)
    ax1.grid(linestyle="dotted", axis="y")

    ax1.set_ylabel("Emissions (Mt of CO2e)")
    ax1.bar(t, emissions, width=10)
    ax1.set_ylim(ymin=0)

    ax2 = ax1.twinx()

    ax2.set_ylabel("Emissions (Mt of CO2)")
    ax2.bar(t, list(map(lambda x: x / 3.0 / 1.68, emissions)), width=10)
    ax2.set_ylim(ymin=0)

    fig.tight_layout()
    fig.savefig("results/timeseries_emissions.png", dpi=300)
    print(f"{sum(emissions)} kt of CO2e")
    print(f"{sum(emissions) / 3.0 / 1.68} Mt of CO2")


def hours_per_model():
    x = duckdb.sql(
        f"""
    SELECT
        "model"
        , SUM(epoch("end" - "start") / 60 / 60) / 1000 AS "flying_time_hour"
        , SUM("emissions_kg") / 1000 / 1000 / 1000 AS "emissions_mega_tons"
    FROM "legs"
    GROUP BY "model"
    ORDER BY "emissions_mega_tons" DESC
    """
    ).fetchall()

    t = list(range(len(x)))
    hours = [x[1] for x in x]
    emissions = [x[2] for x in x]

    fig, ax1 = plt.subplots(figsize=FIG_SIZE)

    ax1.set_xlabel("Aircraft model (rank by emissions)")
    ax1.grid(linestyle="dotted", axis="x")

    color = "black"
    ax1.set_ylabel("Emissions (Mt of CO2e)", color=color)
    ax1.annotate(x[0][0], xy=(1, emissions[0]))
    ax1.annotate(x[3][0], xy=(4, emissions[3]))
    ax1.annotate(x[11][0], xy=(12, emissions[11]))
    ax1.plot(t, emissions, "o", color=color)
    ax1.tick_params(axis="y", labelcolor=color)
    ax1.set_ylim(ymin=0)

    ax2 = ax1.twinx()

    color = "red"
    ax2.set_ylabel("Flying time (thousands of hours)", color=color)
    ax2.plot(t, hours, "o", color=color, alpha=0.5)
    ax2.tick_params(axis="y", labelcolor=color)
    ax2.set_ylim(ymin=0)

    ax1.set_zorder(1)  # default zorder is 0 for ax1 and ax2
    ax1.patch.set_visible(False)  # prevents ax1 from hiding ax2
    fig.tight_layout()
    fig.savefig("results/model_agg.png", dpi=300)


def base_analysis():
    aircraft()
    aircraft_by_country()
    ranked_legs()
    ranked_hours()
    histogram_hours()
    histogram_emissions()
    other_stats()
    hours_per_model()
    per_month()


def distribution():
    x = duckdb.sql(
        f"""
SELECT
    epoch("end" - "start") / 60 / 60 AS "flying_time_k_hours"
    , distance
FROM "legs"
USING SAMPLE 10%
    """
    ).fetchall()

    y = [x[1] for x in x]
    x = [x[0] for x in x]

    plt.figure(figsize=FIG_SIZE)
    plt.plot(x, y, ".")
    plt.xlabel("Flying time (hours)")
    plt.ylabel("Distance (km)")
    plt.grid(linestyle="dotted")
    plt.tight_layout()
    plt.savefig("results/dist.png", dpi=300)


distribution()
base_analysis()
