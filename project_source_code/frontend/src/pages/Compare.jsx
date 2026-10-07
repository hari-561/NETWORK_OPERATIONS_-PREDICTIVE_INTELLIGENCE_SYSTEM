import { useMemo, useState } from "react";

import { useSearchParams } from "react-router-dom";

import {
  AlertTriangle,
  CalendarDays,
  GitCompareArrows,
  Grid3X3,
  Hash,
  Info,
  Search,
} from "lucide-react";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { getGridActivity } from "../services/api";


const COLOR_A = "#4f8cff";
const COLOR_B = "#9b7cff";


export default function Compare() {
  const [searchParams] = useSearchParams();

  const [mode, setMode] = useState("grid");

  return (
    <div className="page compare-page">

      <section className="page-header">
        <div>
          <div className="eyebrow">NETWORK ANALYTICS</div>
          <h1>Compare</h1>
          <p>
            Compare network behavior side by side — across two grids, or
            across two dates for the same grid.
          </p>
        </div>

        <div className="segmented-tabs">
          <button
            type="button"
            className={`segmented-tab ${mode === "grid" ? "active" : ""}`}
            onClick={() => setMode("grid")}
          >
            <GitCompareArrows size={14} />
            Grid vs Grid
          </button>

          <button
            type="button"
            className={`segmented-tab ${mode === "date" ? "active" : ""}`}
            onClick={() => setMode("date")}
          >
            <CalendarDays size={14} />
            Date vs Date
          </button>
        </div>
      </section>

      {mode === "grid" ? (
        <GridVsGrid initialGridA={searchParams.get("gridA")} />
      ) : (
        <DateVsDate initialGrid={searchParams.get("gridA")} />
      )}

    </div>
  );
}


/* =========================================================
   GRID VS GRID
   ========================================================= */

function GridVsGrid({ initialGridA }) {
  const [gridAInput, setGridAInput] = useState(initialGridA || "");
  const [gridBInput, setGridBInput] = useState("");
  const [date, setDate] = useState("");
  const [hour, setHour] = useState("");

  const [resultA, setResultA] = useState(null);
  const [resultB, setResultB] = useState(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [searched, setSearched] = useState(false);

  async function handleSubmit(event) {
    event.preventDefault();

    const gridA = String(gridAInput).trim();
    const gridB = String(gridBInput).trim();

    if (!gridA || !gridB) {
      setError("Enter both a Grid A and Grid B ID to compare.");
      setSearched(true);
      return;
    }

    if (!/^\d+$/.test(gridA) || !/^\d+$/.test(gridB)) {
      setError("Grid IDs must be numeric values.");
      setSearched(true);
      return;
    }

    if (gridA === gridB) {
      setError("Choose two different grids to compare.");
      setSearched(true);
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSearched(true);

      const filters = {
        date: date ? `${date}T00:00:00` : undefined,
        hour: hour !== "" ? Number(hour) : undefined,
      };

      const [dataA, dataB] = await Promise.all([
        getGridActivity(Number(gridA), filters),
        getGridActivity(Number(gridB), filters),
      ]);

      setResultA(dataA);
      setResultB(dataB);
    } catch (err) {
      setResultA(null);
      setResultB(null);
      setError(
        "We couldn't retrieve activity for one or both grids with the selected filters."
      );
    } finally {
      setLoading(false);
    }
  }

  const statsA = useMemo(() => computeStats(resultA), [resultA]);
  const statsB = useMemo(() => computeStats(resultB), [resultB]);

  const chartData = useMemo(() => {
    if (!statsA || !statsB) {
      return [];
    }

    return [
      { metric: "Total", a: statsA.total, b: statsB.total },
      { metric: "Average", a: statsA.average, b: statsB.average },
      { metric: "Peak", a: statsA.peak, b: statsB.peak },
      { metric: "Calls", a: statsA.calls, b: statsB.calls },
      { metric: "SMS", a: statsA.sms, b: statsB.sms },
      { metric: "Internet", a: statsA.internet, b: statsB.internet },
    ];
  }, [statsA, statsB]);

  const verdict = useMemo(() => {
    if (!statsA || !statsB) {
      return null;
    }

    if (statsA.total === 0 && statsB.total === 0) {
      return "Neither grid recorded meaningful activity in this window.";
    }

    const higher = statsA.total >= statsB.total ? "A" : "B";
    const higherTotal = Math.max(statsA.total, statsB.total);
    const lowerTotal = Math.min(statsA.total, statsB.total);

    const diffPct = lowerTotal > 0
      ? ((higherTotal - lowerTotal) / lowerTotal) * 100
      : 100;

    return `Grid ${higher === "A" ? gridAInput : gridBInput} recorded ${diffPct.toFixed(0)}% more total activity than Grid ${higher === "A" ? gridBInput : gridAInput} over the selected window.`;
  }, [statsA, statsB, gridAInput, gridBInput]);

  return (
    <>
      <section className="compare-controls">
        <form className="compare-form" onSubmit={handleSubmit}>

          <div className="compare-field">
            <label htmlFor="grid-a">GRID A</label>
            <div className="compare-input">
              <Hash size={14} />
              <input
                id="grid-a"
                type="text"
                inputMode="numeric"
                placeholder="e.g. 5061"
                value={gridAInput}
                onChange={(e) => setGridAInput(e.target.value)}
              />
            </div>
          </div>

          <div className="compare-field">
            <label htmlFor="grid-b">GRID B</label>
            <div className="compare-input">
              <Hash size={14} />
              <input
                id="grid-b"
                type="text"
                inputMode="numeric"
                placeholder="e.g. 5258"
                value={gridBInput}
                onChange={(e) => setGridBInput(e.target.value)}
              />
            </div>
          </div>

          <div className="compare-field">
            <label htmlFor="compare-date">DATE (OPTIONAL)</label>
            <div className="compare-input">
              <CalendarDays size={14} />
              <input
                id="compare-date"
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
              />
            </div>
          </div>

          <button type="submit" className="analyze-button" disabled={loading}>
            {loading ? (
              <>
                <span className="button-spinner" />
                Comparing
              </>
            ) : (
              <>
                <Search size={15} />
                Compare
              </>
            )}
          </button>

        </form>
      </section>

      {!loading && error && (
        <div className="state-card error-state" style={{ marginTop: 18 }}>
          <div className="state-icon">
            <AlertTriangle size={19} />
          </div>
          <div>
            <h3>Comparison unavailable</h3>
            <p>{error}</p>
          </div>
        </div>
      )}

      {!loading && !error && statsA && statsB && (
        <>
          <section className="compare-columns">

            <div className="compare-column column-a">
              <div className="compare-column-header">
                <h2>Grid {resultA.grid_id}</h2>
                <span className="compare-column-badge">A</span>
              </div>
              <StatRows stats={statsA} />
            </div>

            <div className="compare-vs-divider">
              <span>VS</span>
            </div>

            <div className="compare-column column-b">
              <div className="compare-column-header">
                <h2>Grid {resultB.grid_id}</h2>
                <span className="compare-column-badge">B</span>
              </div>
              <StatRows stats={statsB} />
            </div>

          </section>

          {verdict && (
            <div className="compare-verdict">
              <Info size={16} />
              <p>{verdict}</p>
            </div>
          )}

          <section className="compare-chart-panel">
            <div className="panel-heading">
              <div>
                <div className="eyebrow">COMPARATIVE VIEW</div>
                <h2>Metric comparison</h2>
                <p>Aggregate activity metrics for the selected window.</p>
              </div>
            </div>

            <div className="compare-chart">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 10, right: 15, left: 0, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#202a35" vertical={false} />
                  <XAxis
                    dataKey="metric"
                    tick={{ fill: "#697786", fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fill: "#697786", fontSize: 10 }}
                    axisLine={false}
                    tickLine={false}
                    tickFormatter={formatAxisValue}
                    width={48}
                  />
                  <Tooltip content={<CompareTooltip gridA={resultA.grid_id} gridB={resultB.grid_id} />} />
                  <Legend
                    formatter={(value) =>
                      value === "a" ? `Grid ${resultA.grid_id}` : `Grid ${resultB.grid_id}`
                    }
                    wrapperStyle={{ fontSize: 11, color: "#8b98a7" }}
                  />
                  <Bar dataKey="a" name="a" fill={COLOR_A} radius={[4, 4, 0, 0]} />
                  <Bar dataKey="b" name="b" fill={COLOR_B} radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </section>
        </>
      )}

      {!loading && !error && !statsA && searched && (
        <div className="inline-empty" style={{ marginTop: 18 }}>
          Enter two grid IDs above to begin the comparison.
        </div>
      )}
    </>
  );
}


/* =========================================================
   DATE VS DATE
   ========================================================= */

function DateVsDate({ initialGrid }) {
  const [gridInput, setGridInput] = useState(initialGrid || "");
  const [dateA, setDateA] = useState("");
  const [dateB, setDateB] = useState("");

  const [resultA, setResultA] = useState(null);
  const [resultB, setResultB] = useState(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [searched, setSearched] = useState(false);

  async function handleSubmit(event) {
    event.preventDefault();

    const grid = String(gridInput).trim();

    if (!grid || !dateA || !dateB) {
      setError("Enter a grid ID and both dates to compare.");
      setSearched(true);
      return;
    }

    if (!/^\d+$/.test(grid)) {
      setError("Grid ID must be a numeric value.");
      setSearched(true);
      return;
    }

    if (dateA === dateB) {
      setError("Choose two different dates to compare.");
      setSearched(true);
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSearched(true);

      const [dataA, dataB] = await Promise.all([
        getGridActivity(Number(grid), { date: `${dateA}T00:00:00` }),
        getGridActivity(Number(grid), { date: `${dateB}T00:00:00` }),
      ]);

      setResultA(dataA);
      setResultB(dataB);
    } catch (err) {
      setResultA(null);
      setResultB(null);
      setError(
        "We couldn't retrieve activity for this grid on one or both selected dates."
      );
    } finally {
      setLoading(false);
    }
  }

  const statsA = useMemo(() => computeStats(resultA), [resultA]);
  const statsB = useMemo(() => computeStats(resultB), [resultB]);

  const chartData = useMemo(() => {
    if (!resultA?.data || !resultB?.data) {
      return [];
    }

    const byHourA = new Map(
      resultA.data.map((item) => [item.hour_of_day, Number(item.total_activity)])
    );
    const byHourB = new Map(
      resultB.data.map((item) => [item.hour_of_day, Number(item.total_activity)])
    );

    return Array.from({ length: 24 }, (_, hour) => ({
      hour: formatHour(hour),
      a: byHourA.has(hour) ? byHourA.get(hour) : null,
      b: byHourB.has(hour) ? byHourB.get(hour) : null,
    }));
  }, [resultA, resultB]);

  const verdict = useMemo(() => {
    if (!statsA || !statsB) {
      return null;
    }

    if (statsA.total === 0 && statsB.total === 0) {
      return "No meaningful activity was recorded on either date for this grid.";
    }

    const higherTotal = Math.max(statsA.total, statsB.total);
    const lowerTotal = Math.min(statsA.total, statsB.total);
    const diffPct = lowerTotal > 0
      ? ((higherTotal - lowerTotal) / lowerTotal) * 100
      : 100;

    if (diffPct < 15) {
      return `Activity on ${formatDateOnly(dateA)} and ${formatDateOnly(dateB)} is within ${diffPct.toFixed(0)}% of each other — this looks like a recurring pattern rather than an isolated event.`;
    }

    return `Activity differs by ${diffPct.toFixed(0)}% between ${formatDateOnly(dateA)} and ${formatDateOnly(dateB)} — this looks like an isolated deviation rather than a persistent pattern.`;
  }, [statsA, statsB, dateA, dateB]);

  return (
    <>
      <section className="compare-controls">
        <form className="compare-form" onSubmit={handleSubmit}>

          <div className="compare-field">
            <label htmlFor="trend-grid">GRID</label>
            <div className="compare-input">
              <Grid3X3 size={14} />
              <input
                id="trend-grid"
                type="text"
                inputMode="numeric"
                placeholder="e.g. 5061"
                value={gridInput}
                onChange={(e) => setGridInput(e.target.value)}
              />
            </div>
          </div>

          <div className="compare-field">
            <label htmlFor="date-a">DATE A</label>
            <div className="compare-input">
              <CalendarDays size={14} />
              <input
                id="date-a"
                type="date"
                value={dateA}
                onChange={(e) => setDateA(e.target.value)}
              />
            </div>
          </div>

          <div className="compare-field">
            <label htmlFor="date-b">DATE B</label>
            <div className="compare-input">
              <CalendarDays size={14} />
              <input
                id="date-b"
                type="date"
                value={dateB}
                onChange={(e) => setDateB(e.target.value)}
              />
            </div>
          </div>

          <button type="submit" className="analyze-button" disabled={loading}>
            {loading ? (
              <>
                <span className="button-spinner" />
                Comparing
              </>
            ) : (
              <>
                <Search size={15} />
                Compare
              </>
            )}
          </button>

        </form>
      </section>

      {!loading && error && (
        <div className="state-card error-state" style={{ marginTop: 18 }}>
          <div className="state-icon">
            <AlertTriangle size={19} />
          </div>
          <div>
            <h3>Comparison unavailable</h3>
            <p>{error}</p>
          </div>
        </div>
      )}

      {!loading && !error && statsA && statsB && (
        <>
          <section className="compare-columns">

            <div className="compare-column column-a">
              <div className="compare-column-header">
                <h2>{formatDateOnly(dateA)}</h2>
                <span className="compare-column-badge">A</span>
              </div>
              <StatRows stats={statsA} />
            </div>

            <div className="compare-vs-divider">
              <span>VS</span>
            </div>

            <div className="compare-column column-b">
              <div className="compare-column-header">
                <h2>{formatDateOnly(dateB)}</h2>
                <span className="compare-column-badge">B</span>
              </div>
              <StatRows stats={statsB} />
            </div>

          </section>

          {verdict && (
            <div className="compare-verdict">
              <Info size={16} />
              <p>{verdict}</p>
            </div>
          )}

          <section className="compare-chart-panel">
            <div className="panel-heading">
              <div>
                <div className="eyebrow">HOURLY TREND</div>
                <h2>Grid {gridInput} — hour-by-hour comparison</h2>
                <p>Total activity across each hour of the two selected dates.</p>
              </div>
            </div>

            <div className="compare-chart">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData} margin={{ top: 10, right: 15, left: 0, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#202a35" vertical={false} />
                  <XAxis
                    dataKey="hour"
                    tick={{ fill: "#697786", fontSize: 10 }}
                    axisLine={false}
                    tickLine={false}
                    interval={2}
                  />
                  <YAxis
                    tick={{ fill: "#697786", fontSize: 10 }}
                    axisLine={false}
                    tickLine={false}
                    tickFormatter={formatAxisValue}
                    width={48}
                  />
                  <Tooltip content={<CompareTooltip gridA={formatDateOnly(dateA)} gridB={formatDateOnly(dateB)} />} />
                  <Legend
                    formatter={(value) =>
                      value === "a" ? formatDateOnly(dateA) : formatDateOnly(dateB)
                    }
                    wrapperStyle={{ fontSize: 11, color: "#8b98a7" }}
                  />
                  <Line type="monotone" dataKey="a" name="a" stroke={COLOR_A} strokeWidth={2.4} dot={false} connectNulls />
                  <Line type="monotone" dataKey="b" name="b" stroke={COLOR_B} strokeWidth={2.4} dot={false} connectNulls />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </section>
        </>
      )}

      {!loading && !error && !statsA && searched && (
        <div className="inline-empty" style={{ marginTop: 18 }}>
          Enter a grid ID and two dates above to begin the comparison.
        </div>
      )}
    </>
  );
}


/* =========================================================
   SHARED
   ========================================================= */

function StatRows({ stats }) {
  return (
    <div className="compare-stat-list">
      <div className="compare-stat-row">
        <span>Total activity</span>
        <strong>{formatValue(stats.total)}</strong>
      </div>
      <div className="compare-stat-row">
        <span>Average activity</span>
        <strong>{formatValue(stats.average)}</strong>
      </div>
      <div className="compare-stat-row">
        <span>Peak activity</span>
        <strong>{formatValue(stats.peak)}{stats.peakLabel ? ` · ${stats.peakLabel}` : ""}</strong>
      </div>
      <div className="compare-stat-row">
        <span>Calls</span>
        <strong>{formatValue(stats.calls)}</strong>
      </div>
      <div className="compare-stat-row">
        <span>SMS</span>
        <strong>{formatValue(stats.sms)}</strong>
      </div>
      <div className="compare-stat-row">
        <span>Internet</span>
        <strong>{formatValue(stats.internet)}</strong>
      </div>
    </div>
  );
}

function CompareTooltip({ active, payload, label, gridA, gridB }) {
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
            {entry.dataKey === "a" ? gridA : gridB}
          </span>
          <strong>{entry.value !== null ? formatValue(entry.value) : "—"}</strong>
        </div>
      ))}
    </div>
  );
}

function computeStats(result) {
  if (!result?.data?.length) {
    return null;
  }

  const totals = result.data.reduce(
    (acc, item) => ({
      total: acc.total + (Number(item.total_activity) || 0),
      calls: acc.calls + (Number(item.total_calls) || 0),
      sms: acc.sms + (Number(item.total_sms) || 0),
      internet: acc.internet + (Number(item.internet_activity) || 0),
    }),
    { total: 0, calls: 0, sms: 0, internet: 0 }
  );

  const peakPoint = result.data.reduce((highest, current) =>
    Number(current.total_activity) > Number(highest.total_activity)
      ? current
      : highest
  );

  return {
    total: totals.total,
    calls: totals.calls,
    sms: totals.sms,
    internet: totals.internet,
    average: totals.total / result.data.length,
    peak: Number(peakPoint.total_activity) || 0,
    peakLabel: formatHour(peakPoint.hour_of_day),
  };
}


/* =========================================
   FORMATTERS
========================================= */

function formatHour(hour) {
  return `${String(hour).padStart(2, "0")}:00`;
}

function formatDateOnly(value) {
  if (!value) {
    return "—";
  }

  const date = new Date(`${value}T00:00:00`);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleDateString([], {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
}

function formatValue(value) {
  const number = Number(value) || 0;

  if (number >= 1_000_000_000) {
    return `${(number / 1_000_000_000).toFixed(2)}B`;
  }

  if (number >= 1_000_000) {
    return `${(number / 1_000_000).toFixed(2)}M`;
  }

  if (number >= 1_000) {
    return `${(number / 1_000).toFixed(2)}K`;
  }

  return number.toFixed(2);
}

function formatAxisValue(value) {
  const number = Number(value);

  if (number >= 1_000_000) {
    return `${(number / 1_000_000).toFixed(1)}M`;
  }

  if (number >= 1_000) {
    return `${(number / 1_000).toFixed(1)}K`;
  }

  return number;
}
