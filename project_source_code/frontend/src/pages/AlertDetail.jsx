import { useEffect, useMemo, useState } from "react";

import { Link, useLocation, useParams } from "react-router-dom";

import {
  AlertTriangle,
  ArrowLeft,
  BrainCircuit,
  GitCompareArrows,
  Grid3X3,
  Info,
  TrendingDown,
  TrendingUp,
} from "lucide-react";

import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  getAlerts,
  getGridActivity,
  getGridAnomaly,
  getGridFeatures,
} from "../services/api";


export default function AlertDetail() {
  const { gridId, timestamp } = useParams();
  const location = useLocation();

  const passedAlert = location.state?.alert || null;

  const [alert, setAlert] = useState(null);
  const [activity, setActivity] = useState(null);
  const [features, setFeatures] = useState(null);
  const [featuresAvailable, setFeaturesAvailable] = useState(true);
  const [anomaly, setAnomaly] = useState(null);
  const [anomalyAvailable, setAnomalyAvailable] = useState(true);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);


  /*
   * -----------------------------------------
   * LOAD INVESTIGATION
   * -----------------------------------------
   */

  useEffect(() => {
    loadInvestigation();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [gridId, timestamp]);


  async function loadInvestigation() {
    const gridIdNumber = Number(gridId);

    try {
      setLoading(true);
      setError(null);
      setFeaturesAvailable(true);
      setAnomalyAvailable(true);

      const alertPromise =
        passedAlert &&
        String(passedAlert.grid_id) === String(gridId) &&
        passedAlert.timestamp === timestamp
          ? Promise.resolve(passedAlert)
          : resolveAlert(gridIdNumber, timestamp);

      const [alertResult, activityResult] = await Promise.all([
        alertPromise,
        getGridActivity(gridIdNumber, { asOf: timestamp }),
      ]);

      setAlert(alertResult);
      setActivity(activityResult);

      try {
        const featuresResult = await getGridFeatures(gridIdNumber, {
          asOf: timestamp,
        });
        setFeatures(featuresResult);
      } catch {
        setFeatures(null);
        setFeaturesAvailable(false);
      }

      try {
        const anomalyResult = await getGridAnomaly(gridIdNumber, {
          asOf: timestamp,
        });
        setAnomaly(anomalyResult);
      } catch {
        setAnomaly(null);
        setAnomalyAvailable(false);
      }
    } catch (err) {
      setAlert(null);
      setActivity(null);
      setError(
        "We couldn't reconstruct this alert investigation. The alert record or its supporting activity data may no longer be available."
      );
    } finally {
      setLoading(false);
    }
  }


  async function resolveAlert(gridIdNumber, ts) {
    const response = await getAlerts(100000, { asOf: ts });

    const match = (response?.data || []).find(
      (item) =>
        Number(item.grid_id) === gridIdNumber && item.timestamp === ts
    );

    if (!match) {
      throw new Error(
        `No alert record found for grid ${gridIdNumber} at ${ts}`
      );
    }

    return match;
  }


  /*
   * -----------------------------------------
   * DERIVED EVIDENCE
   * -----------------------------------------
   */

  const trailingMedian = useMemo(() => {
    if (!activity?.data?.length) {
      return null;
    }

    const values = activity.data
      .map((item) => Number(item.total_activity))
      .filter((value) => Number.isFinite(value));

    if (!values.length) {
      return null;
    }

    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);

    return sorted.length % 2 === 0
      ? (sorted[middle - 1] + sorted[middle]) / 2
      : sorted[middle];
  }, [activity]);

  const chartData = useMemo(() => {
    if (!activity?.data) {
      return [];
    }

    return activity.data.map((item) => ({
      timestamp: item.timestamp,
      label: formatHour(item.hour_of_day),
      total: Number(item.total_activity) || 0,
    }));
  }, [activity]);

  const deviation = useMemo(() => {
    if (!alert || alert.baseline_activity === null || alert.baseline_activity === undefined) {
      return null;
    }

    if (!alert.baseline_activity) {
      return null;
    }

    return (
      ((alert.current_activity - alert.baseline_activity) /
        alert.baseline_activity) *
      100
    );
  }, [alert]);

  const isDrop = String(alert?.alert_type || "").toUpperCase().includes("DROP");
  const isHighSeverity = String(alert?.severity || "").toUpperCase() === "HIGH";

  const { datePart, hourPart } = useMemo(
    () => splitTimestamp(timestamp),
    [timestamp]
  );

  const crossLinkParams = `gridId=${gridId}&date=${datePart}&hour=${hourPart}`;


  /*
   * -----------------------------------------
   * NARRATIVE
   * -----------------------------------------
   */

  const narrative = useMemo(() => {
    if (!alert) {
      return "";
    }

    const typeLabel = formatAlertType(alert.alert_type).toLowerCase();
    const direction = isDrop ? "drop below" : "rise above";

    let text =
      `At ${formatDateTime(alert.timestamp)}, Grid ${alert.grid_id} triggered ` +
      `a ${typeLabel} alert (${alert.severity} severity). Activity measured ` +
      `${formatValue(alert.current_activity)}`;

    if (alert.baseline_activity !== null && alert.baseline_activity !== undefined) {
      text +=
        ` against a within-day baseline of ${formatValue(alert.baseline_activity)}, ` +
        `a ${direction} the expected range` +
        (deviation !== null
          ? ` of ${Math.abs(deviation).toFixed(0)}%.`
          : ".");
    } else {
      text += ".";
    }

    if (anomaly) {
      if (anomaly.anomaly_flag) {
        text +=
          ` Independent anomaly scoring corroborates this reading: ${anomaly.reason}`;
      } else {
        text +=
          ` Independent anomaly scoring for this hour did not flag the reading as statistically unusual ` +
          `(${(Number(anomaly.deviation || 0) * 100).toFixed(1)}% deviation from its own historical baseline), ` +
          `suggesting the rule-based alert may reflect a sharper within-day shift rather than a longer-run anomaly.`;
      }
    }

    return text;
  }, [alert, anomaly, isDrop, deviation]);


  /*
   * -----------------------------------------
   * RENDER
   * -----------------------------------------
   */

  return (
    <div className="page investigation-page">

      <Link to="/alerts" className="back-link">
        <ArrowLeft size={13} />
        Back to alerts
      </Link>

      {loading && (
        <div className="state-card">
          <div className="loader" />
          <div>
            <h3>Reconstructing the investigation</h3>
            <p>Pulling activity evidence, grid context and anomaly signals.</p>
          </div>
        </div>
      )}

      {!loading && error && (
        <div className="state-card error-state">
          <div className="state-icon">
            <AlertTriangle size={19} />
          </div>
          <div>
            <h3>Investigation unavailable</h3>
            <p>{error}</p>
          </div>
        </div>
      )}

      {!loading && !error && alert && (
        <>
          {/* =====================================
              BANNER
          ===================================== */}

          <section className="investigation-banner">

            <div className="investigation-banner-main">

              <div
                className={`investigation-banner-icon ${
                  isHighSeverity ? "severity-high" : "severity-medium"
                }`}
              >
                {isDrop ? <TrendingDown size={20} /> : <TrendingUp size={20} />}
              </div>

              <div>
                <span>ALERT INVESTIGATION</span>
                <h1>
                  Grid {alert.grid_id} · {formatAlertType(alert.alert_type)}
                </h1>
                <p>{alert.reason}</p>
              </div>

            </div>

            <div className="investigation-meta">

              <div className="investigation-meta-item">
                <span>SEVERITY</span>
                <strong>{alert.severity}</strong>
              </div>

              <div className="investigation-meta-item">
                <span>ALERT TIME</span>
                <strong>{formatDateTime(alert.timestamp)}</strong>
              </div>

              <div className="investigation-meta-item">
                <span>GRID ID</span>
                <strong>{alert.grid_id}</strong>
              </div>

            </div>

          </section>


          {/* =====================================
              NARRATIVE
          ===================================== */}

          <section className="investigation-narrative">
            <h2>What happened here</h2>
            <p>{narrative}</p>
          </section>


          {/* =====================================
              KEY STATS
          ===================================== */}

          <section className="investigation-grid">

            <div className="investigation-stat is-current">
              <span>CURRENT ACTIVITY</span>
              <strong>{formatValue(alert.current_activity)}</strong>
              <em>At the moment the alert fired</em>
            </div>

            <div className="investigation-stat">
              <span>WITHIN-DAY BASELINE</span>
              <strong>
                {alert.baseline_activity !== null &&
                alert.baseline_activity !== undefined
                  ? formatValue(alert.baseline_activity)
                  : "—"}
              </strong>
              <em>Expected activity for this hour</em>
            </div>

            <div className="investigation-stat">
              <span>DEVIATION</span>
              <strong>
                {deviation !== null ? `${deviation > 0 ? "+" : ""}${deviation.toFixed(1)}%` : "—"}
              </strong>
              <em className={isDrop ? "delta-down" : "delta-up"}>
                {isDrop ? "Below expected baseline" : "Above expected baseline"}
              </em>
            </div>

            <div className="investigation-stat">
              <span>TRAILING 24H MEDIAN</span>
              <strong>
                {trailingMedian !== null ? formatValue(trailingMedian) : "—"}
              </strong>
              <em>Same signal used by the ML3 risk model</em>
            </div>

          </section>


          {/* =====================================
              EVIDENCE + CONTEXT
          ===================================== */}

          <section className="investigation-sections">

            <div className="investigation-panel">
              <div className="panel-heading">
                <div>
                  <div className="eyebrow">ACTIVITY EVIDENCE</div>
                  <h2>Trailing 24 hours ending at the alert</h2>
                  <p>
                    Total activity for Grid {alert.grid_id} through{" "}
                    {formatDateTime(alert.timestamp)}, against the alert&apos;s
                    within-day baseline.
                  </p>
                </div>
              </div>

              <div className="investigation-panel-body">
                {chartData.length > 0 ? (
                  <div className="investigation-chart">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart
                        data={chartData}
                        margin={{ top: 10, right: 15, left: 0, bottom: 5 }}
                      >
                        <CartesianGrid
                          strokeDasharray="3 3"
                          stroke="#202a35"
                          vertical={false}
                        />
                        <XAxis
                          dataKey="label"
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
                        <Tooltip
                          content={<EvidenceTooltip />}
                        />
                        {alert.baseline_activity ? (
                          <ReferenceLine
                            y={alert.baseline_activity}
                            stroke="#f0b35b"
                            strokeDasharray="5 4"
                            label={{
                              value: "Baseline",
                              position: "insideTopLeft",
                              fill: "#f0b35b",
                              fontSize: 9,
                            }}
                          />
                        ) : null}
                        <Line
                          type="monotone"
                          dataKey="total"
                          name="Total activity"
                          stroke="#39d98a"
                          strokeWidth={2.4}
                          dot={false}
                          activeDot={{ r: 4 }}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <div className="inline-empty">
                    No trailing activity data is available for this window.
                  </div>
                )}
              </div>
            </div>


            <div className="investigation-panel">
              <div className="panel-heading">
                <div>
                  <div className="eyebrow">GRID CONTEXT</div>
                  <h2>Feature snapshot</h2>
                  <p>Stored ML feature context nearest this alert time.</p>
                </div>
              </div>

              <div className="investigation-panel-body">
                {featuresAvailable && features ? (
                  <div className="context-list">
                    <div className="context-row">
                      <span>Average activity</span>
                      <strong>{formatValue(features.avg_activity)}</strong>
                    </div>
                    <div className="context-row">
                      <span>Activity growth</span>
                      <strong>
                        {(features.activity_growth * 100).toFixed(1)}%
                      </strong>
                    </div>
                    <div className="context-row">
                      <span>Peak ratio</span>
                      <strong>{features.peak_ratio.toFixed(2)}x</strong>
                    </div>
                    <div className="context-row">
                      <span>Variability</span>
                      <strong>{features.variability.toFixed(2)}</strong>
                    </div>
                    <div className="context-row">
                      <span>Internet share</span>
                      <strong>
                        {(features.internet_share * 100).toFixed(1)}%
                      </strong>
                    </div>
                    <div className="context-row">
                      <span>Active hours</span>
                      <strong>{features.active_hours}</strong>
                    </div>
                    <div className="context-row">
                      <span>Data quality</span>
                      <strong
                        className={
                          features.data_quality === "GOOD"
                            ? "tone-good"
                            : "tone-warning"
                        }
                      >
                        {features.data_quality}
                      </strong>
                    </div>
                    <div className="context-row">
                      <span>Feature freshness</span>
                      <strong
                        className={
                          features.feature_freshness === "STALE"
                            ? "tone-warning"
                            : "tone-good"
                        }
                      >
                        {features.feature_freshness}
                      </strong>
                    </div>
                  </div>
                ) : (
                  <div className="inline-empty">
                    No stored feature context is available for this grid at
                    this time.
                  </div>
                )}
              </div>
            </div>

          </section>


          {/* =====================================
              ANOMALY CONTEXT
          ===================================== */}

          <section className="investigation-panel" style={{ marginTop: 16 }}>
            <div className="panel-heading">
              <div>
                <div className="eyebrow">STATISTICAL ANOMALY CONTEXT</div>
                <h2>Independent anomaly signal</h2>
                <p>
                  A separate historical-baseline comparison for this grid and
                  hour, used to corroborate rule-based alerts.
                </p>
              </div>

              {anomaly && (
                <div
                  className={`status-pill ${
                    anomaly.anomaly_flag
                      ? "status-pill-attention"
                      : "status-pill-healthy"
                  }`}
                >
                  <span className="status-pill-dot" />
                  {anomaly.anomaly_flag ? "FLAGGED" : "NORMAL"}
                </div>
              )}
            </div>

            <div className="investigation-panel-body">
              {anomalyAvailable && anomaly ? (
                <>
                  <div className="investigation-grid" style={{ marginTop: 0 }}>
                    <div className="investigation-stat">
                      <span>HISTORICAL BASELINE</span>
                      <strong>
                        {anomaly.historical_baseline !== null
                          ? formatValue(anomaly.historical_baseline)
                          : "—"}
                      </strong>
                      <em>
                        From {anomaly.baseline_count ?? "—"} comparable
                        historical readings
                      </em>
                    </div>
                    <div className="investigation-stat">
                      <span>DEVIATION</span>
                      <strong>
                        {anomaly.deviation !== null
                          ? `${(anomaly.deviation * 100).toFixed(1)}%`
                          : "—"}
                      </strong>
                      <em>{anomaly.anomaly_direction || "—"}</em>
                    </div>
                    <div className="investigation-stat">
                      <span>ANOMALY SCORE</span>
                      <strong>
                        {anomaly.anomaly_score !== null
                          ? anomaly.anomaly_score.toFixed(3)
                          : "—"}
                      </strong>
                      <em>Higher indicates greater deviation</em>
                    </div>
                    <div className="investigation-stat">
                      <span>RECOMMENDED ACTION</span>
                      <strong>{anomaly.recommended_action || "NONE"}</strong>
                      <em>From the anomaly-detection pipeline</em>
                    </div>
                  </div>

                  {anomaly.reason && (
                    <div className="anomaly-callout">
                      <strong>Detector reasoning</strong>
                      {anomaly.reason}
                    </div>
                  )}
                </>
              ) : (
                <div className="inline-empty">
                  No anomaly-detection record is available for this grid at
                  this time. The alert above is based on the rule-based
                  activity-alert pipeline only.
                </div>
              )}
            </div>
          </section>


          {/* =====================================
              ACTIONS
          ===================================== */}

          <section style={{ marginTop: 18 }}>
            <div className="action-link-row">

              <Link
                className="action-link"
                to={`/grids?${crossLinkParams}`}
              >
                <Grid3X3 size={14} />
                Open in Grid Explorer
              </Link>

              <Link
                className="action-link action-link-primary"
                to={`/risk?${crossLinkParams}`}
              >
                <BrainCircuit size={14} />
                Assess predictive risk
              </Link>

              <Link
                className="action-link"
                to={`/compare?gridA=${gridId}`}
              >
                <GitCompareArrows size={14} />
                Compare with another grid
              </Link>

            </div>
          </section>

        </>
      )}

    </div>
  );
}


/* =========================================
   TOOLTIP
========================================= */

function EvidenceTooltip({ active, payload }) {
  if (!active || !payload?.length) {
    return null;
  }

  const point = payload[0]?.payload;

  return (
    <div className="activity-tooltip">
      <div className="tooltip-time">{point?.label}</div>
      <div className="tooltip-row">
        <span>
          <Info size={11} style={{ marginRight: 4 }} />
          Total activity
        </span>
        <strong>{formatValue(point?.total)}</strong>
      </div>
    </div>
  );
}


/* =========================================
   HELPERS
========================================= */

function splitTimestamp(value) {
  if (!value) {
    return { datePart: "", hourPart: "0" };
  }

  const [datePart, timePart = "00:00:00"] = value.split("T");
  const hourPart = String(Number(timePart.split(":")[0] || 0));

  return { datePart, hourPart };
}


/* =========================================
   FORMATTERS
========================================= */

function formatHour(hour) {
  return `${String(hour).padStart(2, "0")}:00`;
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

function formatAlertType(value) {
  if (!value) {
    return "Unknown alert";
  }

  return String(value)
    .toLowerCase()
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function formatDateTime(value) {
  if (!value) {
    return "—";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString([], {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
