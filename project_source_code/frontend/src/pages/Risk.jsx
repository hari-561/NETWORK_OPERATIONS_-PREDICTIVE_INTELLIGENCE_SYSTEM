import { useState } from "react";

import { Link, useSearchParams } from "react-router-dom";

import {
  AlertTriangle,
  BrainCircuit,
  CalendarDays,
  CheckCircle2,
  Clock3,
  GitCompareArrows,
  Grid3X3,
  Info,
  Search,
  ShieldAlert,
} from "lucide-react";

import {
  getAlerts,
  getGridFeatures,
  getGridActivity,
  predictRisk,
} from "../services/api";


export default function Risk() {
  const [searchParams] = useSearchParams();

  const [gridInput, setGridInput] =
    useState(searchParams.get("gridId") || "");

  const [date, setDate] =
    useState(searchParams.get("date") || "2013-11-07");

  const [hour, setHour] =
    useState(searchParams.get("hour") || "10");

  const [prediction, setPrediction] =
    useState(null);

  const [evidence, setEvidence] =
    useState(null);

  const [loading, setLoading] =
    useState(false);

  const [error, setError] =
    useState(null);

  const [searched, setSearched] =
    useState(false);

async function handleSubmit(event) {
  event.preventDefault();

  const value =
    String(gridInput).trim();

  if (!value) {
    setError(
      "Enter a grid ID to request a prediction."
    );
    setSearched(true);
    return;
  }

  if (!/^\d+$/.test(value)) {
    setError(
      "Grid ID must be a numeric value."
    );
    setSearched(true);
    return;
  }

  try {
    setLoading(true);
    setError(null);
    setPrediction(null);
    setEvidence(null);
    setSearched(true);


    /*
     * -----------------------------------------
     * STEP 1
     * Retrieve the stored ML feature vector.
     * -----------------------------------------
     */

  const selectedTimestamp = `${date}T${String(hour).padStart(2, "0")}:00:00`;

const featureResponse = await getGridFeatures(
  Number(value),
  {
    asOf: selectedTimestamp,
  }
);

    if (!featureResponse?.feature_timestamp) {
      throw new Error(
        "No valid feature timestamp was returned."
      );
    }


    const featureTimestamp =
      featureResponse.feature_timestamp;


    /*
     * -----------------------------------------
     * STEP 2
     * Retrieve the activity window ending
     * at the feature timestamp.
     * -----------------------------------------
     */

    const activityResponse =
      await getGridActivity(
        Number(value),
        {
          asOf: featureTimestamp,
        }
      );


    if (
      !activityResponse?.data?.length
    ) {
      throw new Error(
        "No activity history is available for the selected grid."
      );
    }


    /*
     * -----------------------------------------
     * STEP 3
     * Calculate trailing 24-hour median.
     *
     * This is the same concept used by the
     * ML3 feature engineering pipeline.
     * -----------------------------------------
     */

    const activityValues =
      activityResponse.data
        .map(
          (item) =>
            Number(
              item.total_activity
            )
        )
        .filter(
          (item) =>
            Number.isFinite(item)
        );


    if (!activityValues.length) {
      throw new Error(
        "Unable to calculate the trailing activity baseline."
      );
    }


    const trailingMedian24h =
      calculateMedian(
        activityValues
      );


    /*
     * -----------------------------------------
     * STEP 4
     * Derive time fields from the actual
     * feature timestamp.
     *
     * JavaScript getDay():
     * Sunday = 0
     * Monday = 1
     * ...
     * Saturday = 6
     *
     * This matches the API schema's 0–6 range.
     * -----------------------------------------
     */

    const timestamp =
      parseLocalDateTime(
        featureTimestamp
      );


    const hourOfDay =
      timestamp.getHours();


    const dayOfWeek =
  (timestamp.getDay() + 6) % 7;

    /*
     * -----------------------------------------
     * STEP 5
     * Submit the complete prediction request.
     * -----------------------------------------
     */

    const predictionResponse =
      await predictRisk({
        grid_id:
          Number(value),

        feature_timestamp:
          featureTimestamp,

        avg_activity:
          Number(
            featureResponse.avg_activity
          ),

        activity_growth:
          Number(
            featureResponse.activity_growth
          ),

        peak_ratio:
          Number(
            featureResponse.peak_ratio
          ),

        variability:
          Number(
            featureResponse.variability
          ),

        internet_share:
          Number(
            featureResponse.internet_share
          ),

        trailing_median_24h:
          trailingMedian24h,

        hour_of_day:
          hourOfDay,

        day_of_week:
          dayOfWeek,
      });


    setPrediction(
      predictionResponse
    );


    /*
     * -----------------------------------------
     * STEP 6
     * Gather supporting evidence so the operator
     * can move from "risk result" to "why should
     * I care" to "what evidence supports this".
     *
     * A missing related alert is not an error —
     * most grids will not have one.
     * -----------------------------------------
     */

    let relatedAlert = null;

    try {
      const alertsResponse = await getAlerts(
        500,
        { asOf: featureTimestamp }
      );

      relatedAlert =
        (alertsResponse?.data || []).find(
          (item) => Number(item.grid_id) === Number(value)
        ) || null;

    } catch {
      relatedAlert = null;
    }

    const recentPoint =
      activityResponse.data[
        activityResponse.data.length - 1
      ];

    setEvidence({
      recentActivity: Number(recentPoint?.total_activity) || 0,
      recentTimestamp: recentPoint?.timestamp,
      trailingMedian24h,
      avgActivity: Number(featureResponse.avg_activity),
      activityGrowth: Number(featureResponse.activity_growth),
      peakRatio: Number(featureResponse.peak_ratio),
      variability: Number(featureResponse.variability),
      internetShare: Number(featureResponse.internet_share),
      relatedAlert,
    });

  } catch (err) {

    setPrediction(null);

    setError(
      getRiskErrorMessage(err)
    );

  } finally {
    setLoading(false);
  }
}

  return (
    <div className="page risk-page">

      {/* =====================================
          HEADER
      ===================================== */}

      <section className="page-header risk-page-header">

        <div>

          <div className="eyebrow">
            PREDICTIVE INTELLIGENCE
          </div>

          <h1>
            Predictive Risk
          </h1>

          <p>
            Assess the model-estimated high-activity
            risk for a selected network grid and
            prediction time.
          </p>

        </div>

        {prediction && (
          <div className="grid-reporting">

            <Clock3 size={15} />

            <div>

              <span>
                PREDICTION TIME
              </span>

              <strong>
                {formatDateTime(
                  prediction.feature_timestamp
                )}
              </strong>

            </div>

          </div>
        )}

      </section>


      {/* =====================================
          REQUEST PANEL
      ===================================== */}

      <section className="risk-request-panel">

        <div className="risk-request-info">

          <div className="risk-request-icon">
            <BrainCircuit size={20} />
          </div>

          <div>

            <span className="eyebrow">
              RISK ASSESSMENT
            </span>

            <h2>
              Select a network grid
            </h2>

            <p>
              Choose the grid and prediction time.
              The model features are retrieved from
              the analytics service automatically.
            </p>

          </div>

        </div>


        <form
          className="risk-request-form"
          onSubmit={handleSubmit}
        >

          {/* GRID */}

          <div className="risk-field">

            <label htmlFor="risk-grid">
              GRID ID
            </label>

            <div className="risk-input">

              <Grid3X3 size={15} />

              <input
                id="risk-grid"
                type="text"
                inputMode="numeric"
                placeholder="Enter grid ID"
                value={gridInput}
                onChange={(event) =>
                  setGridInput(
                    event.target.value
                  )
                }
              />

            </div>

          </div>


          {/* DATE */}

          <div className="risk-field">

            <label htmlFor="risk-date">
              PREDICTION DATE
            </label>

            <div className="risk-input">

              <CalendarDays size={15} />

              <input
                id="risk-date"
                type="date"
                value={date}
                onChange={(event) =>
                  setDate(
                    event.target.value
                  )
                }
              />

            </div>

          </div>


          {/* HOUR */}

          <div className="risk-field">

            <label htmlFor="risk-hour">
              PREDICTION HOUR
            </label>

            <div className="risk-input">

              <Clock3 size={15} />

              <select
                id="risk-hour"
                value={hour}
                onChange={(event) =>
                  setHour(
                    event.target.value
                  )
                }
              >

                {Array.from(
                  { length: 24 },
                  (_, index) => (
                    <option
                      key={index}
                      value={index}
                    >
                      {String(index).padStart(
                        2,
                        "0"
                      )}:00
                    </option>
                  )
                )}

              </select>

            </div>

          </div>


          <button
            type="submit"
            className="analyze-button risk-submit"
            disabled={loading}
          >

            {loading ? (
              <>
                <span className="button-spinner" />
                Assessing
              </>
            ) : (
              <>
                <Search size={15} />
                Assess Risk
              </>
            )}

          </button>

        </form>

      </section>


      {/* =====================================
          ERROR
      ===================================== */}

      {!loading && error && (
        <section className="risk-error">

          <AlertTriangle size={17} />

          <div>

            <strong>
              Risk assessment unavailable
            </strong>

            <p>
              {error}
            </p>

          </div>

        </section>
      )}


      {/* =====================================
          RESULTS
      ===================================== */}

      {!loading &&
        !error &&
        prediction && (
          <RiskResult
            prediction={prediction}
            evidence={evidence}
          />
        )}


      {/* =====================================
          INITIAL STATE
      ===================================== */}

      {!loading &&
        !error &&
        !prediction &&
        !searched && (
          <section className="risk-intro">

            <div className="risk-intro-icon">
              <ShieldAlert size={24} />
            </div>

            <div>

              <div className="eyebrow">
                MODEL-BASED SIGNAL
              </div>

              <h2>
                Request a risk assessment
              </h2>

              <p>
                The result is an operational attention
                signal produced by the deployed ML model.
                It does not confirm a network fault.
              </p>

            </div>

          </section>
        )}

    </div>
  );
}


/* =========================================
   RISK RESULT
========================================= */

function RiskResult({
  prediction,
  evidence,
}) {
  const score =
    Number(
      prediction.risk_score
    ) || 0;

  const percentage =
    score * 100;

  const level =
    String(
      prediction.risk_level || ""
    ).toUpperCase();

  const { datePart, hourPart } =
    splitTimestamp(
      prediction.feature_timestamp
    );


  return (
    <>

      {/* MODEL OUTPUT */}

      <section className="risk-output-panel">

        <div className="risk-output-header">

          <div>

            <div className="eyebrow">
              MODEL OUTPUT
            </div>

            <h2>
              High-activity risk assessment
            </h2>

            <p>
              Prediction generated for Grid{" "}
              {prediction.grid_id}.
            </p>

          </div>


          <div
            className={`risk-level risk-level-${level.toLowerCase()}`}
          >

            {level === "LOW" && (
              <CheckCircle2 size={15} />
            )}

            {level !== "LOW" && (
              <ShieldAlert size={15} />
            )}

            <span>
              {level}
            </span>

          </div>

        </div>


        <div className="risk-output-grid">

          {/* SCORE */}

          <div className="risk-score-card">

            <div className="risk-score-label">
              RISK SCORE
            </div>

            <div className="risk-score-value">
              {percentage.toFixed(1)}
              <span>%</span>
            </div>

            <div className="risk-score-bar">

              <span
                style={{
                  width: `${Math.min(
                    Math.max(
                      percentage,
                      0
                    ),
                    100
                  )}%`,
                }}
              />

            </div>

            <p>
              Estimated probability of the
              defined high-activity target.
            </p>

          </div>


          {/* MODEL DETAILS */}

          <div className="risk-details-card">

            <div className="risk-detail">

              <span>
                GRID
              </span>

              <strong>
                {prediction.grid_id}
              </strong>

            </div>


            <div className="risk-detail">

              <span>
                PREDICTION TIME
              </span>

              <strong>
                {formatDateTime(
                  prediction.feature_timestamp
                )}
              </strong>

            </div>


            <div className="risk-detail">

              <span>
                MODEL VERSION
              </span>

              <strong>
                {prediction.model_version}
              </strong>

            </div>

          </div>

        </div>

      </section>


      {/* EXPLANATION */}

      <section className="risk-explanation-panel">

        <div className="risk-explanation-heading">

          <div className="risk-explanation-icon">
            <Info size={17} />
          </div>

          <div>

            <div className="eyebrow">
              OPERATIONAL INTERPRETATION
            </div>

            <h2>
              What this result means
            </h2>

          </div>

        </div>


        <div className="risk-explanation-content">

          <p>
            {prediction.explanation_note}
          </p>

        </div>


        <div className="risk-explanation-footer">

          <div className="risk-disclaimer">

            <Info size={14} />

            <span>
              This model output is an attention
              signal. It should be investigated alongside
              network activity evidence and does not
              confirm a network fault.
            </span>

          </div>


          <button
            type="button"
            className="ai-explain-button"
            disabled
            title="Available in the later Claude phase"
          >
            <BrainCircuit size={15} />
            Explain with AI
            <span className="coming-soon">
              COMING SOON
            </span>
          </button>

        </div>

      </section>


      {/* =====================================
          EVIDENCE
      ===================================== */}

      {evidence && (
        <section
          className="investigation-panel"
          style={{ marginTop: 16 }}
        >
          <div className="panel-heading">
            <div>
              <div className="eyebrow">
                RISK → EVIDENCE
              </div>
              <h2>
                What evidence supports investigating this grid
              </h2>
              <p>
                Recent activity and stored feature signals behind this
                prediction, so the score can be checked against real
                network evidence.
              </p>
            </div>
          </div>

          <div className="investigation-panel-body">

            <div className="investigation-grid" style={{ marginTop: 0 }}>

              <div className="investigation-stat is-current">
                <span>RECENT ACTIVITY</span>
                <strong>
                  {formatValue(evidence.recentActivity)}
                </strong>
                <em>
                  At {formatDateTime(evidence.recentTimestamp)}
                </em>
              </div>

              <div className="investigation-stat">
                <span>TRAILING 24H BASELINE</span>
                <strong>
                  {formatValue(evidence.trailingMedian24h)}
                </strong>
                <em>Median of the trailing window</em>
              </div>

              <div className="investigation-stat">
                <span>ACTIVITY GROWTH</span>
                <strong>
                  {(evidence.activityGrowth * 100).toFixed(1)}%
                </strong>
                <em>Change versus the prior period</em>
              </div>

              <div className="investigation-stat">
                <span>PEAK RATIO</span>
                <strong>
                  {evidence.peakRatio.toFixed(2)}x
                </strong>
                <em>Peak activity vs. average</em>
              </div>

            </div>

            <div className="context-list" style={{ marginTop: 8 }}>

              <div className="context-row">
                <span>Average activity</span>
                <strong>{formatValue(evidence.avgActivity)}</strong>
              </div>

              <div className="context-row">
                <span>Variability</span>
                <strong>{evidence.variability.toFixed(2)}</strong>
              </div>

              <div className="context-row">
                <span>Internet share</span>
                <strong>
                  {(evidence.internetShare * 100).toFixed(1)}%
                </strong>
              </div>

            </div>

            {evidence.relatedAlert ? (
              <div className="anomaly-callout" style={{ marginTop: 14 }}>
                <strong>Related alert on record</strong>
                A {formatAlertTypeLabel(evidence.relatedAlert.alert_type)}{" "}
                alert ({evidence.relatedAlert.severity}) was already raised
                for this grid at{" "}
                {formatDateTime(evidence.relatedAlert.timestamp)}.{" "}
                {evidence.relatedAlert.reason}
              </div>
            ) : (
              <div className="inline-empty" style={{ marginTop: 14 }}>
                No rule-based alert is currently on record for this grid
                around this time.
              </div>
            )}

            <div className="action-link-row" style={{ marginTop: 16 }}>

              <Link
                className="action-link"
                to={`/grids?gridId=${prediction.grid_id}&date=${datePart}&hour=${hourPart}`}
              >
                <Grid3X3 size={14} />
                Open in Grid Explorer
              </Link>

              <Link
                className="action-link"
                to={`/compare?gridA=${prediction.grid_id}`}
              >
                <GitCompareArrows size={14} />
                Compare with historical dates
              </Link>

              {evidence.relatedAlert && (
                <Link
                  className="action-link action-link-primary"
                  to={`/alerts/${prediction.grid_id}/${encodeURIComponent(
                    evidence.relatedAlert.timestamp
                  )}`}
                  state={{ alert: evidence.relatedAlert }}
                >
                  <AlertTriangle size={14} />
                  View alert investigation
                </Link>
              )}

            </div>

          </div>

        </section>
      )}

    </>
  );
}


/* =========================================
   FORMATTERS
========================================= */

function formatDateTime(value) {
  if (!value) {
    return "—";
  }

  const date =
    new Date(value);

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


function calculateMedian(values) {
  if (!values.length) {
    return 0;
  }

  const sorted = [...values]
    .sort(
      (a, b) => a - b
    );

  const middle =
    Math.floor(
      sorted.length / 2
    );


  if (
    sorted.length % 2 === 0
  ) {
    return (
      (sorted[middle - 1] +
        sorted[middle]) /
      2
    );
  }


  return sorted[middle];
}


function parseLocalDateTime(value) {
  if (!value) {
    return new Date(NaN);
  }

  /*
   * Backend timestamps have no timezone:
   * 2013-11-07T23:00:00
   *
   * Remove any accidental timezone suffix
   * so the historical hour is preserved.
   */

  const cleanValue =
    String(value)
      .replace(/Z$/, "")
      .split("+")[0];

  const [
    datePart,
    timePart = "00:00:00",
  ] = cleanValue.split("T");

  const [
    year,
    month,
    day,
  ] = datePart
    .split("-")
    .map(Number);

  const [
    hour = 0,
    minute = 0,
    second = 0,
  ] = timePart
    .split(":")
    .map(Number);

  return new Date(
    year,
    month - 1,
    day,
    hour,
    minute,
    second
  );
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


function splitTimestamp(value) {
  if (!value) {
    return { datePart: "", hourPart: "0" };
  }

  const [datePart, timePart = "00:00:00"] = String(value).split("T");
  const hourPart = String(Number(timePart.split(":")[0] || 0));

  return { datePart, hourPart };
}


function formatAlertTypeLabel(value) {
  if (!value) {
    return "unknown";
  }

  return String(value)
    .toLowerCase()
    .split("_")
    .join(" ");
}


function getRiskErrorMessage(error) {
  if (
    Array.isArray(error?.detail)
  ) {
    return error.detail
      .map(
        (item) =>
          item?.msg
      )
      .filter(Boolean)
      .join(" ");
  }

  if (
    typeof error?.detail === "string"
  ) {
    return error.detail;
  }

  if (
    error instanceof Error &&
    error.message
  ) {
    return error.message;
  }

  return (
    "We couldn't generate a risk assessment. Please try again."
  );
}