import { useEffect, useState } from "react";

import {
  AlertTriangle,
  CheckCircle2,
  Database,
  Radio,
  RefreshCw,
  ServerCog,
  ShieldQuestion,
  Waves,
} from "lucide-react";

import {
  getApiHealth,
  getGridFeatures,
  getPipelineStatus,
} from "../services/api";


/*
 * A representative grid used to probe whether the stored
 * feature pipeline (consumed by Predictive Risk) is actually
 * returning usable rows. This performs a real request against
 * the live feature endpoint rather than fabricating a score.
 */
const PROBE_GRID_ID = 1;


export default function PipelineHealth() {
  const [apiStatus, setApiStatus] = useState("checking");
  const [pipeline, setPipeline] = useState(null);
  const [pipelineError, setPipelineError] = useState(null);
  const [featureStatus, setFeatureStatus] = useState("checking");
  const [loading, setLoading] = useState(true);


  useEffect(() => {
    loadHealth();
  }, []);


  async function loadHealth() {
    setLoading(true);
    setApiStatus("checking");
    setFeatureStatus("checking");
    setPipelineError(null);

    const results = await Promise.allSettled([
      getApiHealth(),
      getPipelineStatus(),
      getGridFeatures(PROBE_GRID_ID),
    ]);

    const [healthResult, pipelineResult, featureResult] = results;

    setApiStatus(healthResult.status === "fulfilled" ? "up" : "down");

    if (pipelineResult.status === "fulfilled") {
      setPipeline(pipelineResult.value);
    } else {
      setPipeline(null);
      setPipelineError(
        pipelineResult.reason?.detail ||
          "The pipeline status record could not be retrieved."
      );
    }

    setFeatureStatus(
      featureResult.status === "fulfilled" ? "operational" : "degraded"
    );

    setLoading(false);
  }


  const overall = deriveOverallStatus(apiStatus, pipeline, pipelineError);


  return (
    <div className="page pipeline-page">

      <section className="page-header">
        <div>
          <div className="eyebrow">DATA OPERATIONS</div>
          <h1>Pipeline Health</h1>
          <p>
            Whether the data behind this dashboard is available, current and
            trustworthy — for NOC operators, not developers.
          </p>
        </div>

        <button
          className="secondary-action"
          onClick={loadHealth}
          disabled={loading}
        >
          <RefreshCw size={14} className={loading ? "spin-icon" : ""} />
          {loading ? "Checking" : "Recheck now"}
        </button>
      </section>


      {/* =====================================
          OVERALL STATUS
      ===================================== */}

      <section className="pipeline-status-banner">
        <div className="pipeline-status-main">
          <div className={`pipeline-status-icon ${overall.tone}`}>
            {overall.tone === "healthy" && <CheckCircle2 size={22} />}
            {overall.tone === "attention" && <AlertTriangle size={22} />}
            {overall.tone === "unavailable" && <ShieldQuestion size={22} />}
          </div>

          <div>
            <h2>{overall.label}</h2>
            <p>{overall.description}</p>
          </div>
        </div>

        <div className={`status-pill status-pill-${overall.tone}`}>
          <span className="status-pill-dot" />
          {overall.tone.toUpperCase()}
        </div>
      </section>

      {pipeline?.reasons?.length > 0 && (
        <div className="pipeline-reasons">
          <strong>Reported issues:</strong> {pipeline.reasons.join(" · ")}
        </div>
      )}


      {/* =====================================
          KEY HEALTH SIGNALS
      ===================================== */}

      <section className="health-grid">

        <HealthCard
          icon={Radio}
          label="API Availability"
          value={apiStatus === "up" ? "Reachable" : apiStatus === "down" ? "Unreachable" : "Checking…"}
          description="Analytics REST API responding to requests"
          tone={apiStatus === "up" ? "good" : apiStatus === "down" ? "bad" : "neutral"}
        />

        <HealthCard
          icon={ServerCog}
          label="Latest Pipeline Run"
          value={pipeline ? pipeline.status : "Unknown"}
          description={
            pipeline
              ? `Run ${shortenRunId(pipeline.run_id)}`
              : pipelineError || "No pipeline record available"
          }
          tone={pipeline?.status === "SUCCESS" ? "good" : pipeline ? "bad" : "neutral"}
        />

        <HealthCard
          icon={Waves}
          label="Data Freshness"
          value={pipeline?.freshness?.indicator || "Unknown"}
          description={
            pipeline?.freshness
              ? `Latest reporting timestamp ${formatDateTime(
                  pipeline.freshness.latest_data_timestamp
                )}`
              : "Freshness could not be determined"
          }
          tone={freshnessTone(pipeline?.freshness?.indicator)}
        />

        <HealthCard
          icon={Database}
          label="Feature Pipeline"
          value={
            featureStatus === "operational"
              ? "Operational"
              : featureStatus === "degraded"
                ? "Degraded"
                : "Checking…"
          }
          description="Live probe of the stored ML feature store used by Predictive Risk"
          tone={
            featureStatus === "operational"
              ? "good"
              : featureStatus === "degraded"
                ? "bad"
                : "neutral"
          }
        />

      </section>


      {/* =====================================
          TASK BREAKDOWN
      ===================================== */}

      {pipeline && (
        <section className="pipeline-section">
          <div className="panel-heading">
            <div>
              <div className="eyebrow">PIPELINE STAGES</div>
              <h2>Latest run task breakdown</h2>
              <p>
                Current as of {formatDateTime(pipeline.current_as_of)} ·
                Evaluated {formatDateTime(pipeline.timestamp)}
              </p>
            </div>
          </div>

          <div className="pipeline-section-body">
            <div className="task-chip-row">
              {Object.entries(pipeline.tasks).map(([task, status]) => (
                <TaskChip key={task} task={task} status={status} />
              ))}
            </div>
          </div>
        </section>
      )}


      {/* =====================================
          DATA VOLUME
      ===================================== */}

      {pipeline && (
        <section className="pipeline-section">
          <div className="panel-heading">
            <div>
              <div className="eyebrow">DATA VOLUME &amp; QUALITY</div>
              <h2>Rows processed in the latest run</h2>
              <p>
                Whether datasets required by this dashboard are complete and
                were ingested without rejection.
              </p>
            </div>
          </div>

          <div className="pipeline-section-body">
            <div className="volume-row">

              <div className="volume-stat">
                <span>ROWS IN</span>
                <strong>{formatCount(pipeline.rows_in)}</strong>
              </div>

              <div className="volume-stat">
                <span>ROWS REJECTED</span>
                <strong className={pipeline.rows_rejected > 0 ? "tone-danger" : ""}>
                  {formatCount(pipeline.rows_rejected)}
                </strong>
              </div>

              <div className="volume-stat">
                <span>ROWS PUBLISHED</span>
                <strong>{formatCount(pipeline.rows_published)}</strong>
              </div>

              <div className="volume-stat">
                <span>REJECTION RATE</span>
                <strong className={
                  computeRejectionRate(pipeline) > 1 ? "tone-danger" : ""
                }>
                  {computeRejectionRate(pipeline).toFixed(2)}%
                </strong>
              </div>

            </div>
          </div>
        </section>
      )}


      {!loading && !pipeline && (
        <div className="state-card error-state" style={{ marginTop: 18 }}>
          <div className="state-icon">
            <AlertTriangle size={19} />
          </div>
          <div>
            <h3>Pipeline status record unavailable</h3>
            <p>
              {pipelineError ||
                "The pipeline status file could not be read. Data operations details cannot be shown."}
            </p>
          </div>
        </div>
      )}

    </div>
  );
}


/* =========================================
   HEALTH CARD
========================================= */

function HealthCard({ icon: Icon, label, value, description, tone }) {
  return (
    <article className="health-card">
      <div className="health-card-label">
        <span>{label}</span>
        <Icon
          size={16}
          color={
            tone === "good"
              ? "var(--accent)"
              : tone === "bad"
                ? "var(--danger)"
                : "var(--text-subtle)"
          }
        />
      </div>
      <div className="health-card-value">{value}</div>
      <div className="health-card-description">{description}</div>
    </article>
  );
}


/* =========================================
   TASK CHIP
========================================= */

function TaskChip({ task, status }) {
  const normalized = String(status || "").toUpperCase();
  const dotClass =
    normalized === "SUCCESS" ? "ok" : normalized ? "fail" : "unknown";

  return (
    <div className="task-chip">
      <span className={`task-chip-dot ${dotClass}`} />
      <div className="task-chip-label">
        <span>{task.replace(/_/g, " ").toUpperCase()}</span>
        <strong>{status}</strong>
      </div>
    </div>
  );
}


/* =========================================
   STATUS DERIVATION
========================================= */

function deriveOverallStatus(apiStatus, pipeline, pipelineError) {
  if (apiStatus === "down") {
    return {
      tone: "unavailable",
      label: "Data operations unavailable",
      description:
        "The analytics API is not responding. Every page relying on it will be showing stale or missing data.",
    };
  }

  if (!pipeline) {
    return {
      tone: "attention",
      label: "Pipeline status could not be verified",
      description:
        pipelineError ||
        "The API is reachable, but the pipeline status record could not be read.",
    };
  }

  const allTasksSucceeded = Object.values(pipeline.tasks || {}).every(
    (status) => String(status).toUpperCase() === "SUCCESS"
  );

  if (pipeline.healthy && allTasksSucceeded && !pipeline.reasons?.length) {
    return {
      tone: "healthy",
      label: "Data operations healthy",
      description:
        "The API is reachable, the latest pipeline run succeeded, and no data-quality issues were reported.",
    };
  }

  return {
    tone: "attention",
    label: "Data operations need attention",
    description:
      "The latest pipeline run reported one or more issues. Review the run details below before relying on downstream figures.",
  };
}

function freshnessTone(indicator) {
  if (indicator === "CURRENT") {
    return "good";
  }

  if (indicator === "RECENT") {
    return "good";
  }

  if (indicator === "STALE") {
    return "neutral";
  }

  return "neutral";
}


/* =========================================
   FORMATTERS
========================================= */

function shortenRunId(runId) {
  if (!runId) {
    return "unknown";
  }

  return runId.length > 28 ? `${runId.slice(0, 28)}…` : runId;
}

function formatCount(value) {
  return Number(value || 0).toLocaleString("en-US");
}

function computeRejectionRate(pipeline) {
  const total = Number(pipeline.rows_in) || 0;
  const rejected = Number(pipeline.rows_rejected) || 0;

  if (total <= 0) {
    return 0;
  }

  return (rejected / total) * 100;
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
