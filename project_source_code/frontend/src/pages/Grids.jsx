import { useEffect, useMemo, useState } from "react";

import { Link, useSearchParams } from "react-router-dom";

import {
  Activity,
  BrainCircuit,
  CalendarDays,
  Clock3,
  GitCompareArrows,
  Globe2,
  Grid3X3,
  Hash,
  PieChart as PieChartIcon,
  RefreshCw,
  Search,
  Smartphone,
} from "lucide-react";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { getGridActivity } from "../services/api";


/*
 * =========================================
 * CHART COLORS
 * =========================================
 *
 * Keep the chart visually clean while making
 * every activity type immediately distinct.
 */

const CHART_COLORS = {
  total: "#39d98a",
  calls: "#4f8cff",
  sms: "#f0b35b",
  internet: "#9b7cff",
};


export default function Grids() {
  const [searchParams] = useSearchParams();

  const [gridInput, setGridInput] = useState(
    searchParams.get("gridId") || ""
  );
  const [selectedGrid, setSelectedGrid] = useState(null);

  const [activity, setActivity] = useState(null);

  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [error, setError] = useState(null);

  const [date, setDate] = useState(
    searchParams.get("date") || ""
  );



  /*
   * NEW:
   * Hour filter.
   *
   * Empty string = all hours
   * 0-23       = selected hour
   */
  const [hour, setHour] = useState(
    searchParams.get("hour") ?? ""
  );


  /*
   * -----------------------------------------
   * AUTO-LOAD FROM CROSS-LINK
   *
   * When arriving from Alerts, Risk or Compare
   * with a gridId query parameter, load it
   * immediately using the pre-filled filters.
   * -----------------------------------------
   */

  useEffect(() => {
    if (gridInput) {
      loadGrid(gridInput);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);


  /*
   * -----------------------------------------
   * LOAD GRID
   * -----------------------------------------
   */

  async function loadGrid(gridId = gridInput) {
    const value = String(gridId).trim();

    if (!value) {
      setError("Enter a grid ID to continue.");
      setSearched(true);
      return;
    }

    if (!/^\d+$/.test(value)) {
      setError("Grid ID must be a numeric value.");
      setSearched(true);
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSearched(true);

      /*
       * IMPORTANT:
       *
       * Send both date AND hour when selected.
       * The backend is responsible for interpreting
       * the requested filtering combination.
       */

      const data = await getGridActivity(
  Number(value),
  {
    date: date
      ? `${date}T00:00:00`
      : undefined,

    hour:
      hour !== ""
        ? Number(hour)
        : undefined,
  }
);

      setActivity(data);
      setSelectedGrid(Number(value));

    } catch (err) {
      setActivity(null);
      setSelectedGrid(Number(value));

      if (err?.status === 404) {
        setError(
          `No network activity data was found for grid ${value} with the selected filters.`
        );
      } else {
        setError(
          "We couldn't retrieve activity for this grid. Please try again."
        );
      }

    } finally {
      setLoading(false);
    }
  }


  /*
   * -----------------------------------------
   * SEARCH SUBMIT
   * -----------------------------------------
   */

  function handleSubmit(event) {
    event.preventDefault();
    loadGrid();
  }


  /*
   * -----------------------------------------
   * FILTER MODE
   * -----------------------------------------
   *
   * recent:
   *   Grid only
   *
   * day:
   *   Grid + Date
   *
   * hour-across-days:
   *   Grid + Hour
   *
   * hour-from-date:
   *   Grid + Date + Hour
   */

  const filterMode = useMemo(() => {
    if (date && hour !== "") {
      return "hour-from-date";
    }

    if (date) {
      return "day";
    }

    if (hour !== "") {
      return "hour-across-days";
    }

    return "recent";
  }, [date, hour]);


  /*
   * -----------------------------------------
   * CHART DATA
   * -----------------------------------------
   */

  const chartData = useMemo(() => {
    if (!activity?.data) {
      return [];
    }

    return activity.data.map((item) => ({
      timestamp: item.timestamp,

      hour: item.hour_of_day,

      /*
       * For normal hourly views:
       *   00:00, 01:00, 02:00...
       *
       * For hour-based views:
       *   07 Nov, 08 Nov, 09 Nov...
       */

      label:
        filterMode === "recent" ||
        filterMode === "day"
          ? formatHour(item.hour_of_day)
          : formatShortDate(item.timestamp),

      calls:
        Number(item.total_calls) || 0,

      sms:
        Number(item.total_sms) || 0,

      internet:
        Number(item.internet_activity) || 0,

      total:
        Number(item.total_activity) || 0,
    }));

  }, [activity, filterMode]);


  /*
   * -----------------------------------------
   * DERIVED SUMMARY
   * -----------------------------------------
   */

  const totals = useMemo(() => {
    if (!activity?.data?.length) {
      return {
        total: 0,
        calls: 0,
        sms: 0,
        internet: 0,
      };
    }

    return activity.data.reduce(
      (acc, item) => ({
        total:
          acc.total +
          (Number(item.total_activity) || 0),

        calls:
          acc.calls +
          (Number(item.total_calls) || 0),

        sms:
          acc.sms +
          (Number(item.total_sms) || 0),

        internet:
          acc.internet +
          (Number(item.internet_activity) || 0),
      }),
      {
        total: 0,
        calls: 0,
        sms: 0,
        internet: 0,
      }
    );

  }, [activity]);


  /*
   * -----------------------------------------
   * PEAK
   * -----------------------------------------
   */

  const peak = useMemo(() => {
    if (!activity?.data?.length) {
      return null;
    }

    return activity.data.reduce(
      (highest, current) => {
        if (
          Number(current.total_activity) >
          Number(highest.total_activity)
        ) {
          return current;
        }

        return highest;
      }
    );

  }, [activity]);


  /*
   * -----------------------------------------
   * TRAFFIC COMPOSITION
   * -----------------------------------------
   *
   * Share of Calls / SMS / Internet within the
   * selected window's total activity.
   */

  const compositionData = useMemo(() => {
    if (!activity?.data?.length || totals.total <= 0) {
      return [];
    }

    return [
      {
        name: "Calls",
        value: totals.calls,
        color: CHART_COLORS.calls,
      },
      {
        name: "SMS",
        value: totals.sms,
        color: CHART_COLORS.sms,
      },
      {
        name: "Internet",
        value: totals.internet,
        color: CHART_COLORS.internet,
      },
    ].filter((item) => item.value > 0);

  }, [activity, totals]);


  /*
   * -----------------------------------------
   * PEAK VS AVERAGE
   * -----------------------------------------
   *
   * Compares the average and peak value reached
   * by each activity type within the window.
   */

  const peakVsAverage = useMemo(() => {
    if (!activity?.data?.length) {
      return [];
    }

    const count = activity.data.length;

    const maxOf = (key) =>
      activity.data.reduce(
        (highest, item) =>
          Math.max(highest, Number(item[key]) || 0),
        0
      );

    return [
      {
        metric: "Calls",
        average: totals.calls / count,
        peak: maxOf("total_calls"),
      },
      {
        metric: "SMS",
        average: totals.sms / count,
        peak: maxOf("total_sms"),
      },
      {
        metric: "Internet",
        average: totals.internet / count,
        peak: maxOf("internet_activity"),
      },
      {
        metric: "Total",
        average: totals.total / count,
        peak: maxOf("total_activity"),
      },
    ];

  }, [activity, totals]);


  /*
   * -----------------------------------------
   * CHART TITLE
   * -----------------------------------------
   */

  function getChartTitle() {
    switch (filterMode) {
      case "day":
        return "Activity over time";

      case "hour-across-days":
        return "Activity across days";

      case "hour-from-date":
        return "Activity trend from selected date";

      default:
        return "Activity over time";
    }
  }


  /*
   * -----------------------------------------
   * CHART DESCRIPTION
   * -----------------------------------------
   */

  function getChartDescription() {
    switch (filterMode) {
      case "day":
        return "Hourly distribution of network activity for the selected date.";

      case "hour-across-days":
        return `Network activity at ${formatHour(
          Number(hour)
        )} across available dates.`;

      case "hour-from-date":
        return `Network activity at ${formatHour(
          Number(hour)
        )} from the selected date through the reporting period.`;

      default:
        return "Hourly distribution of calls, SMS, internet and total activity.";
    }
  }


  /*
   * -----------------------------------------
   * TABLE TIME LABEL
   * -----------------------------------------
   */

  function getTableTime(item) {
    if (
      filterMode === "hour-across-days" ||
      filterMode === "hour-from-date"
    ) {
      return (
        <div className="time-cell">
          <CalendarDays size={13} />

          <div>
            <strong>
              {formatShortDate(item.timestamp)}
            </strong>

            <span>
              {formatHour(item.hour_of_day)}
            </span>
          </div>
        </div>
      );
    }

    return (
      <div className="time-cell">
        <Clock3 size={13} />

        {formatHour(item.hour_of_day)}
      </div>
    );
  }


  return (
    <div className="page grid-explorer-page">

      {/* =====================================
          HEADER
      ===================================== */}

      <section className="page-header grid-page-header">

        <div>

          <div className="eyebrow">
            NETWORK ANALYTICS
          </div>

          <h1>
            Grid Explorer
          </h1>

          <p>
            Explore network activity for an individual
            grid over time.
          </p>

        </div>


        {activity && (
          <div className="grid-reporting">

            <Clock3 size={15} />

            <div>

              <span>
                REPORTING PERIOD
              </span>

              <strong>
                {formatDateRange(
                  activity.start_time,
                  activity.end_time
                )}
              </strong>

            </div>

          </div>
        )}

      </section>


      {/* =====================================
          SEARCH
      ===================================== */}

      <section className="grid-search-panel">

        <div className="grid-search-info">

          <div className="search-icon">
            <Grid3X3 size={19} />
          </div>

          <div>

            <span className="eyebrow">
              GRID SELECTION
            </span>

            <h2>
              Choose a network grid
            </h2>

            <p>
              Enter a grid ID and optional time filters
              to inspect its activity profile.
            </p>

          </div>

        </div>


        <form
          className="grid-search-form"
          onSubmit={handleSubmit}
        >

          {/* GRID ID */}

          <div className="grid-input-wrapper">

            <Hash size={16} />

            <input
              type="text"
              inputMode="numeric"
              placeholder="Enter grid ID"
              value={gridInput}
              onChange={(event) =>
                setGridInput(event.target.value)
              }
              aria-label="Grid ID"
            />

          </div>


          {/* DATE */}

          <div className="date-input-wrapper">

            <CalendarDays size={16} />

            <input
              type="date"
              value={date}
              onChange={(event) =>
                setDate(event.target.value)
              }
              aria-label="Activity date"
            />

          </div>


          {/* HOUR */}

          <div className="hour-input-wrapper">

  <Clock3 size={16} />

  <select
    value={hour}
    onChange={(event) =>
      setHour(event.target.value)
    }
    aria-label="Activity hour"
  >
    <option value="">
      All hours
    </option>

    {Array.from(
      { length: 24 },
      (_, index) => (
        <option
          key={index}
          value={index}
        >
          {formatHour(index)}
        </option>
      )
    )}
  </select>

</div>


          {/* ANALYZE */}

          <button
            type="submit"
            className="analyze-button"
            disabled={loading}
          >

            {loading ? (
              <>
                <span className="button-spinner" />
                Loading
              </>
            ) : (
              <>
                <Search size={15} />
                Analyze
              </>
            )}

          </button>

        </form>

      </section>


      {/* =====================================
          LOADING
      ===================================== */}

      {loading && (
        <GridLoadingState />
      )}


      {/* =====================================
          ERROR / UNKNOWN GRID
      ===================================== */}

      {!loading && error && (
        <section className="grid-empty-state">

          <div className="empty-state-icon">
            <Grid3X3 size={23} />
          </div>

          <div>

            <div className="eyebrow">
              NO DATA AVAILABLE
            </div>

            <h2>
              {selectedGrid
                ? `Grid ${selectedGrid} could not be loaded`
                : "Grid could not be loaded"}
            </h2>

            <p>
              {error}
            </p>

          </div>

          <button
            className="secondary-action"
            onClick={() => {
              setError(null);
              setSearched(false);
              setGridInput("");
            }}
          >

            <RefreshCw size={14} />

            Try another grid

          </button>

        </section>
      )}


      {/* =====================================
          RESULTS
      ===================================== */}

      {!loading &&
        !error &&
        activity && (
          <>

            {/* GRID IDENTIFICATION */}

            <section className="grid-identity">

              <div className="grid-identity-main">

                <div className="grid-id-icon">
                  <Grid3X3 size={22} />
                </div>

                <div>

                  <span>
                    SELECTED GRID
                  </span>

                  <h2>
                    Grid {activity.grid_id}
                  </h2>

                </div>

              </div>


              <div className="grid-as-of">

  <Clock3 size={14} />

  <div>

    <span>
      {date
        ? "SELECTED DATE"
        : hour !== ""
        ? "SELECTED HOUR"
        : "AS OF"}
    </span>

    <strong>
      {date
        ? formatDateOnly(date)
        : hour !== ""
        ? formatHour(Number(hour))
        : formatDate(activity.as_of)}
    </strong>

  </div>

</div>

            </section>


            {/* CROSS-LINKS */}

            <div className="action-link-row" style={{ marginTop: 14 }}>

              <Link
                className="action-link"
                to={`/risk?gridId=${activity.grid_id}${
                  date ? `&date=${date}` : ""
                }${hour !== "" ? `&hour=${hour}` : ""}`}
              >
                <BrainCircuit size={14} />
                Assess predictive risk
              </Link>

              <Link
                className="action-link"
                to={`/compare?gridA=${activity.grid_id}`}
              >
                <GitCompareArrows size={14} />
                Compare this grid
              </Link>

            </div>


            {/* METRICS */}

            <section className="grid-metric-grid">

              <GridMetric
                icon={Activity}
                label="Total Activity"
                value={formatValue(totals.total)}
                description="Across the selected period"
                primary
              />

              <GridMetric
                icon={Smartphone}
                label="Calls"
                value={formatValue(totals.calls)}
                description="Voice activity"
              />

              <GridMetric
                icon={Hash}
                label="SMS"
                value={formatValue(totals.sms)}
                description="Messaging activity"
              />

              <GridMetric
                icon={Globe2}
                label="Internet"
                value={formatValue(totals.internet)}
                description="Internet activity"
              />

            </section>


            {/* =================================
                MAIN CHART
            ================================= */}

            <section className="activity-chart-panel">

              <div className="panel-heading">

                <div>

                  <div className="eyebrow">
                    {filterMode === "recent" &&
                      "RECENT ACTIVITY"}

                    {filterMode === "day" &&
                      "DAILY ACTIVITY"}

                    {filterMode === "hour-across-days" &&
                      "HOURLY COMPARISON"}

                    {filterMode === "hour-from-date" &&
                      "TIME-BASED ACTIVITY"}
                  </div>

                  <h2>
                    {getChartTitle()}
                  </h2>

                  <p>
                    {getChartDescription()}
                  </p>

                </div>


                {peak && (
                  <div className="peak-indicator">

                    <span>
                      PEAK
                    </span>

                    <strong>
                      {formatHour(
                        peak.hour_of_day
                      )}
                    </strong>

                  </div>
                )}

              </div>


              {/* CHART LEGEND */}

              <div className="chart-legend">

                <LegendItem
                  label="Total"
                  color={CHART_COLORS.total}
                />

                <LegendItem
                  label="Calls"
                  color={CHART_COLORS.calls}
                />

                <LegendItem
                  label="SMS"
                  color={CHART_COLORS.sms}
                />

                <LegendItem
                  label="Internet"
                  color={CHART_COLORS.internet}
                />

              </div>


              <div className="activity-chart">

                <ResponsiveContainer
                  width="100%"
                  height="100%"
                >

                  <LineChart
                    data={chartData}
                    margin={{
                      top: 10,
                      right: 15,
                      left: 0,
                      bottom: 5,
                    }}
                  >

                    <CartesianGrid
                      strokeDasharray="3 3"
                      stroke="#202a35"
                      vertical={false}
                    />


                    <XAxis
                      dataKey="label"
                      tick={{
                        fill: "#697786",
                        fontSize: 10,
                      }}
                      axisLine={false}
                      tickLine={false}
                      interval={
                        filterMode === "recent" ||
                        filterMode === "day"
                          ? 2
                          : "preserveStartEnd"
                      }
                    />


                    <YAxis
                      tick={{
                        fill: "#697786",
                        fontSize: 10,
                      }}
                      axisLine={false}
                      tickLine={false}
                      tickFormatter={formatAxisValue}
                      width={48}
                    />


                    <Tooltip
                      content={
                        <ActivityTooltip
                          filterMode={filterMode}
                        />
                      }
                    />


                    {/* TOTAL */}

                    <Line
                      type="monotone"
                      dataKey="total"
                      name="Total"
                      stroke={CHART_COLORS.total}
                      strokeWidth={2.8}
                      dot={false}
                      activeDot={{
                        r: 4,
                      }}
                    />


                    {/* CALLS */}

                    <Line
                      type="monotone"
                      dataKey="calls"
                      name="Calls"
                      stroke={CHART_COLORS.calls}
                      strokeWidth={1.7}
                      dot={false}
                      activeDot={{
                        r: 3,
                      }}
                    />


                    {/* SMS */}

                    <Line
                      type="monotone"
                      dataKey="sms"
                      name="SMS"
                      stroke={CHART_COLORS.sms}
                      strokeWidth={1.7}
                      dot={false}
                      activeDot={{
                        r: 3,
                      }}
                    />


                    {/* INTERNET */}

                    <Line
                      type="monotone"
                      dataKey="internet"
                      name="Internet"
                      stroke={CHART_COLORS.internet}
                      strokeWidth={1.7}
                      dot={false}
                      activeDot={{
                        r: 3,
                      }}
                    />

                  </LineChart>

                </ResponsiveContainer>

              </div>

            </section>


            {/* =================================
                ADDITIONAL VISUALIZATIONS
            ================================= */}

            <section className="grid-insights-grid">

              {/* TRAFFIC COMPOSITION */}

              <div className="insight-panel">

                <div className="panel-heading">
                  <div>
                    <div className="eyebrow">TRAFFIC MIX</div>
                    <h2>Traffic composition</h2>
                    <p>
                      Share of calls, SMS and internet within total
                      activity.
                    </p>
                  </div>
                </div>

                {compositionData.length > 0 ? (
                  <div className="composition-layout">

                    <div className="composition-chart">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={compositionData}
                            dataKey="value"
                            nameKey="name"
                            innerRadius="62%"
                            outerRadius="100%"
                            paddingAngle={2}
                            stroke="none"
                          >
                            {compositionData.map((entry) => (
                              <Cell
                                key={entry.name}
                                fill={entry.color}
                              />
                            ))}
                          </Pie>
                          <Tooltip
                            content={
                              <CompositionTooltip
                                total={totals.total}
                              />
                            }
                          />
                        </PieChart>
                      </ResponsiveContainer>

                      <div className="composition-chart-center">
                        <span>TOTAL</span>
                        <strong>{formatValue(totals.total)}</strong>
                      </div>
                    </div>

                    <div className="composition-legend">
                      {compositionData.map((entry) => (
                        <div
                          className="composition-legend-row"
                          key={entry.name}
                        >
                          <span
                            className="composition-legend-swatch"
                            style={{ backgroundColor: entry.color }}
                          />

                          <div className="composition-legend-text">
                            <span>{entry.name}</span>
                          </div>

                          <div className="composition-legend-value">
                            <strong>
                              {formatValue(entry.value)}
                            </strong>
                            <em>
                              {(
                                (entry.value / totals.total) *
                                100
                              ).toFixed(1)}
                              %
                            </em>
                          </div>
                        </div>
                      ))}
                    </div>

                  </div>
                ) : (
                  <div className="inline-empty">
                    <PieChartIcon
                      size={14}
                      style={{ marginRight: 6 }}
                    />
                    No traffic composition is available for this window.
                  </div>
                )}

              </div>


              {/* PEAK VS AVERAGE */}

              <div className="insight-panel">

                <div className="panel-heading">
                  <div>
                    <div className="eyebrow">PEAK VS AVERAGE</div>
                    <h2>Peak vs average activity</h2>
                    <p>
                      How far each activity type spikes above its own
                      average.
                    </p>
                  </div>
                </div>

                {peakVsAverage.length > 0 ? (
                  <div className="peak-chart">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={peakVsAverage}
                        margin={{
                          top: 10,
                          right: 10,
                          left: 0,
                          bottom: 5,
                        }}
                      >
                        <CartesianGrid
                          strokeDasharray="3 3"
                          stroke="#202a35"
                          vertical={false}
                        />
                        <XAxis
                          dataKey="metric"
                          tick={{ fill: "#697786", fontSize: 10 }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <YAxis
                          tick={{ fill: "#697786", fontSize: 10 }}
                          axisLine={false}
                          tickLine={false}
                          tickFormatter={formatAxisValue}
                          width={44}
                        />
                        <Tooltip
                          content={<PeakAverageTooltip />}
                        />
                        <Legend
                          wrapperStyle={{
                            fontSize: 10,
                            color: "#8b98a7",
                          }}
                        />
                        <Bar
                          dataKey="average"
                          name="Average"
                          fill="#4f8cff"
                          radius={[4, 4, 0, 0]}
                        />
                        <Bar
                          dataKey="peak"
                          name="Peak"
                          fill="#39d98a"
                          radius={[4, 4, 0, 0]}
                        />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <div className="inline-empty">
                    No peak/average data is available for this window.
                  </div>
                )}

              </div>

            </section>


            {/* =================================
                ACTIVITY DETAILS
            ================================= */}

            <section className="activity-table-panel">

              <div className="panel-heading">

                <div>

                  <div className="eyebrow">
                    ACTIVITY DETAILS
                  </div>

                  <h2>
                    Activity breakdown
                  </h2>

                  <p>
                    Detailed network activity for
                    Grid {activity.grid_id}.
                  </p>

                </div>

              </div>


              <div className="activity-table-wrapper">

                <table className="activity-table">

                  <thead>

                    <tr>

                      <th>
                        {filterMode === "hour-across-days" ||
                        filterMode === "hour-from-date"
                          ? "DATE / TIME"
                          : "TIME"}
                      </th>

                      <th>
                        CALLS
                      </th>

                      <th>
                        SMS
                      </th>

                      <th>
                        INTERNET
                      </th>

                      <th>
                        TOTAL ACTIVITY
                      </th>

                    </tr>

                  </thead>


                  <tbody>

                    {activity.data.map(
                      (item) => (
                        <tr
                          key={item.timestamp}
                        >

                          <td>
                            {getTableTime(item)}
                          </td>


                          <td>
                            {formatTableValue(
                              item.total_calls
                            )}
                          </td>


                          <td>
                            {formatTableValue(
                              item.total_sms
                            )}
                          </td>


                          <td>
                            {formatTableValue(
                              item.internet_activity
                            )}
                          </td>


                          <td className="total-cell">
                            {formatTableValue(
                              item.total_activity
                            )}
                          </td>

                        </tr>
                      )
                    )}

                  </tbody>

                </table>

              </div>

            </section>

          </>
        )}

    </div>
  );
}


/* =========================================
   GRID METRIC
========================================= */

function GridMetric({
  icon: Icon,
  label,
  value,
  description,
  primary = false,
}) {
  return (
    <article
      className={`grid-metric-card ${
        primary
          ? "grid-metric-card-primary"
          : ""
      }`}
    >

      <div className="grid-metric-icon">
        <Icon size={18} />
      </div>

      <div className="grid-metric-label">
        {label}
      </div>

      <div className="grid-metric-value">
        {value}
      </div>

      <div className="grid-metric-description">
        {description}
      </div>

    </article>
  );
}


/* =========================================
   LEGEND
========================================= */

function LegendItem({
  label,
  color,
}) {
  return (
    <div className="legend-item">

      <span
        className="legend-line"
        style={{
          backgroundColor: color,
          boxShadow: `0 0 7px ${color}55`,
        }}
      />

      <span>
        {label}
      </span>

    </div>
  );
}


/* =========================================
   TOOLTIP
========================================= */

function ActivityTooltip({
  active,
  payload,
  label,
  filterMode,
}) {
  if (!active || !payload?.length) {
    return null;
  }

  const timestamp =
    payload[0]?.payload?.timestamp;

  const hour =
    payload[0]?.payload?.hour;


  return (
    <div className="activity-tooltip">

      <div className="tooltip-time">

        {filterMode === "recent" ||
        filterMode === "day"
          ? formatHour(hour)
          : formatShortDate(timestamp)}

      </div>


      {payload.map((entry) => (
        <div
          className="tooltip-row"
          key={entry.dataKey}
        >

          <span>

            <i
              style={{
                backgroundColor:
                  entry.color,
              }}
            />

            {entry.name}

          </span>

          <strong>
            {formatTableValue(
              entry.value
            )}
          </strong>

        </div>
      ))}

    </div>
  );
}


/* =========================================
   COMPOSITION TOOLTIP
========================================= */

function CompositionTooltip({ active, payload, total }) {
  if (!active || !payload?.length) {
    return null;
  }

  const entry = payload[0];
  const share = total > 0 ? (entry.value / total) * 100 : 0;

  return (
    <div className="activity-tooltip">
      <div className="tooltip-time">{entry.name}</div>
      <div className="tooltip-row">
        <span>
          <i style={{ backgroundColor: entry.payload.color }} />
          Activity
        </span>
        <strong>
          {formatTableValue(entry.value)} · {share.toFixed(1)}%
        </strong>
      </div>
    </div>
  );
}


/* =========================================
   PEAK / AVERAGE TOOLTIP
========================================= */

function PeakAverageTooltip({ active, payload, label }) {
  if (!active || !payload?.length) {
    return null;
  }

  return (
    <div className="activity-tooltip">
      <div className="tooltip-time">{label}</div>

      {payload.map((entry) => (
        <div className="tooltip-row" key={entry.dataKey}>
          <span>
            <i style={{ backgroundColor: entry.color }} />
            {entry.name}
          </span>
          <strong>{formatTableValue(entry.value)}</strong>
        </div>
      ))}
    </div>
  );
}


/* =========================================
   LOADING
========================================= */

function GridLoadingState() {
  return (
    <div className="grid-loading">

      <div className="grid-loading-header" />

      <div className="grid-loading-metrics">

        <div />
        <div />
        <div />
        <div />

      </div>

      <div className="grid-loading-chart" />

    </div>
  );
}


/* =========================================
   FORMATTERS
========================================= */

function formatHour(hour) {
  return `${String(hour).padStart(2, "0")}:00`;
}


function formatShortDate(value) {
  if (!value) {
    return "—";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleDateString(
    [],
    {
      day: "2-digit",
      month: "short",
    }
  );
}


function formatValue(value) {
  const number = Number(value) || 0;

  if (number >= 1_000_000_000) {
    return `${(
      number / 1_000_000_000
    ).toFixed(2)}B`;
  }

  if (number >= 1_000_000) {
    return `${(
      number / 1_000_000
    ).toFixed(2)}M`;
  }

  if (number >= 1_000) {
    return `${(
      number / 1_000
    ).toFixed(2)}K`;
  }

  return number.toFixed(2);
}


function formatTableValue(value) {
  const number = Number(value) || 0;

  return number.toLocaleString(
    undefined,
    {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }
  );
}


function formatAxisValue(value) {
  const number = Number(value);

  if (number >= 1_000_000) {
    return `${(
      number / 1_000_000
    ).toFixed(1)}M`;
  }

  if (number >= 1_000) {
    return `${(
      number / 1_000
    ).toFixed(1)}K`;
  }

  return number;
}
function formatDateOnly(value) {
  if (!value) {
    return "—";
  }

  const date = new Date(`${value}T00:00:00`);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleDateString(
    [],
    {
      day: "2-digit",
      month: "short",
      year: "numeric",
    }
  );
}

function formatDate(value) {
  if (!value) {
    return "—";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString(
    [],
    {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    }
  );
}


function formatDateRange(
  start,
  end
) {
  if (!start || !end) {
    return "—";
  }

  const startDate = new Date(start);
  const endDate = new Date(end);

  const date = startDate.toLocaleDateString(
    [],
    {
      day: "2-digit",
      month: "short",
      year: "numeric",
    }
  );

  const startTime = startDate.toLocaleTimeString(
    [],
    {
      hour: "2-digit",
      minute: "2-digit",
    }
  );

  const endTime = endDate.toLocaleTimeString(
    [],
    {
      hour: "2-digit",
      minute: "2-digit",
    }
  );

  return `${date} · ${startTime} — ${endTime}`;
}