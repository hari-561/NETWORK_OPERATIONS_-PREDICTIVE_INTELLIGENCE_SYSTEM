import { useEffect, useState } from "react";
import {
  Activity,
  Clock3,
  Grid3X3,
  MapPin,
  RefreshCw,
} from "lucide-react";

import { getNetworkSummary } from "../services/api";

export default function Overview() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  async function loadSummary() {
    try {
      setLoading(true);
      setError(false);

      const data = await getNetworkSummary();

      setSummary(data);
    } catch (err) {
      console.error(err);
      setError(true);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadSummary();
  }, []);

  return (
    <div className="page">
      {/* --------------------------------
          PAGE HEADER
      --------------------------------- */}
      <section className="page-header overview-header">
        <div>
          <div className="eyebrow">
            NETWORK OPERATIONS CENTER
          </div>

          <h1>Network Overview</h1>

          <p>
            A high-level view of activity across the
            monitored network.
          </p>
        </div>

        {summary && (
          <ReportingTimestamp
            value={summary.as_of}
          />
        )}
      </section>

      {/* --------------------------------
          ERROR BANNER
      --------------------------------- */}
      {error && (
        <div className="status-banner status-banner-error">
          <div className="status-banner-content">
            <div className="status-banner-indicator" />

            <div>
              <strong>
                Network data is currently unavailable
              </strong>

              <p>
                The latest network overview could not be
                retrieved. Please try again.
              </p>
            </div>
          </div>

          <button
            className="retry-button"
            onClick={loadSummary}
          >
            <RefreshCw size={14} />
            Retry
          </button>
        </div>
      )}

      {/* --------------------------------
          LOADING STATE
      --------------------------------- */}
      {loading && !error && <LoadingState />}

      {/* --------------------------------
          NETWORK KPIs
      --------------------------------- */}
      {!loading && !error && summary && (
        <>
          <section className="metric-grid">
            <MetricCard
              icon={Activity}
              label="Total Activity"
              value={formatActivity(
                summary.total_activity
              )}
              description="Network-wide activity"
              primary
            />

            <MetricCard
              icon={Grid3X3}
              label="Active Grids"
              value={formatNumber(
                summary.active_grids
              )}
              description="Monitored network areas"
            />

            <MetricCard
              icon={Clock3}
              label="Peak Hour"
              value={formatHour(
                summary.peak_hour
              )}
              description="Highest activity period"
            />

            <MetricCard
              icon={MapPin}
              label="Highest Activity Grid"
              value={`Grid ${summary.top_grid}`}
              description="Leading activity area"
            />
          </section>

          {/* --------------------------------
              SUMMARY PANEL
          --------------------------------- */}
          <section className="network-summary-panel">
            <div className="summary-panel-header">
              <div>
                <div className="eyebrow">
                  NETWORK AT A GLANCE
                </div>

                <h2>
                  Current network activity
                </h2>

                <p>
                  A snapshot of activity across the
                  monitored network.
                </p>
              </div>

              <div className="summary-panel-icon">
                <Activity size={20} />
              </div>
            </div>

            <div className="summary-stat-grid">
              <SummaryStat
                label="Total Activity"
                value={formatActivity(
                  summary.total_activity
                )}
              />

              <SummaryStat
                label="Peak Activity"
                value={formatHour(
                  summary.peak_hour
                )}
              />

              <SummaryStat
                label="Leading Grid"
                value={`#${summary.top_grid}`}
              />
            </div>
          </section>
        </>
      )}
    </div>
  );
}


/* =========================================
   METRIC CARD
========================================= */

function MetricCard({
  icon: Icon,
  label,
  value,
  description,
  primary = false,
}) {
  return (
    <article
      className={`metric-card ${
        primary ? "metric-card-primary" : ""
      }`}
    >
      <div className="metric-card-icon">
        <Icon
          size={20}
          strokeWidth={1.8}
        />
      </div>

      <div className="metric-card-label">
        {label}
      </div>

      <div className="metric-card-value">
        {value}
      </div>

      <div className="metric-card-description">
        {description}
      </div>
    </article>
  );
}


/* =========================================
   REPORTING TIMESTAMP
========================================= */

function ReportingTimestamp({ value }) {
  return (
    <div className="reporting-timestamp">
      <Clock3 size={15} />

      <div>
        <span>REPORTING TIMESTAMP</span>

        <strong>
          {formatDate(value)}
        </strong>
      </div>
    </div>
  );
}


/* =========================================
   SUMMARY STAT
========================================= */

function SummaryStat({ label, value }) {
  return (
    <div className="summary-stat">
      <span>{label}</span>

      <strong>{value}</strong>
    </div>
  );
}


/* =========================================
   LOADING STATE
========================================= */

function LoadingState() {
  return (
    <div className="loading-dashboard">
      <div className="skeleton-grid">
        <div />
        <div />
        <div />
        <div />
      </div>

      <div className="skeleton-panel" />
    </div>
  );
}


/* =========================================
   FORMATTERS
========================================= */

function formatActivity(value) {
  const number = Number(value);

  if (number >= 1_000_000_000) {
    return `${(number / 1_000_000_000).toFixed(2)}B`;
  }

  if (number >= 1_000_000) {
    return `${(number / 1_000_000).toFixed(2)}M`;
  }

  if (number >= 1_000) {
    return `${(number / 1_000).toFixed(2)}K`;
  }

  return number.toFixed(0);
}


function formatNumber(value) {
  return Number(value).toLocaleString();
}


function formatHour(hour) {
  return `${String(hour).padStart(2, "0")}:00`;
}


function formatDate(dateString) {
  const date = new Date(dateString);

  return date.toLocaleString([], {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}