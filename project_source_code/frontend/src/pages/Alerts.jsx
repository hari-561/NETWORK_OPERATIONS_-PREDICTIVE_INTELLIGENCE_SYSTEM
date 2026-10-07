import { useEffect, useMemo, useRef, useState } from "react";

import { Link } from "react-router-dom";

import {
  AlertTriangle,
  ChevronRight,
  Clock3,
  Map as MapIcon,
  RefreshCw,
  ShieldAlert,
} from "lucide-react";

import L from "leaflet";

import {
  MapContainer,
  TileLayer,
  GeoJSON,
} from "react-leaflet";

import {
  getAlerts,
} from "../services/api";


/*
 * The alert API contains a large number of records.
 * We retrieve the complete alert set once, then use
 * virtual scrolling so the browser only renders the
 * rows currently visible on screen.
 */
const ALL_ALERTS_LIMIT = 100000;


/*
 * Approximate row height used by the virtual list.
 * The actual row is kept visually consistent with
 * the existing operations-row styling.
 */
const ROW_HEIGHT = 82;


/*
 * Number of extra rows rendered above/below the
 * visible viewport. This keeps scrolling smooth.
 */
const OVERSCAN = 6;


const ALERT_TYPES = [
  {
    value: "",
    label: "All alert types",
  },
  {
    value: "HIGH_ACTIVITY",
    label: "High activity",
  },
  {
    value: "ACTIVITY_SPIKE",
    label: "Activity spike",
  },
  {
    value: "ACTIVITY_DROP",
    label: "Activity drop",
  },
];


export default function Alerts() {

  const [alerts, setAlerts] =
    useState([]);

  const [asOf, setAsOf] =
    useState(null);

  const [alertType, setAlertType] =
    useState("");

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState(null);


  /*
   * -----------------------------------------
   * LOAD ALL ALERTS
   * -----------------------------------------
   */

  useEffect(() => {
    loadAlerts();
  }, []);


  async function loadAlerts() {

    try {

      setLoading(true);
      setError(null);


      const response =
        await getAlerts(
          ALL_ALERTS_LIMIT
        );


      const data =
        Array.isArray(response?.data)
          ? response.data
          : [];


      setAlerts(data);


      setAsOf(
        response?.as_of || null
      );


    } catch (err) {

      setAlerts([]);

      setError(
        "We couldn't retrieve the network alerts. Please try again."
      );

    } finally {

      setLoading(false);

    }
  }


  /*
   * -----------------------------------------
   * FILTER
   * -----------------------------------------
   */

  const filteredAlerts =
    useMemo(() => {

      if (!alertType) {
        return alerts;
      }


      return alerts.filter(
        (item) =>
          String(
            item.alert_type || ""
          ).toUpperCase() ===
          alertType
      );

    }, [
      alerts,
      alertType,
    ]);


  /*
   * -----------------------------------------
   * FILTER CHANGE
   * -----------------------------------------
   */

  function handleAlertTypeChange(
    event
  ) {

    setAlertType(
      event.target.value
    );

  }


  return (
    <div className="page alerts-page">

      {/* =====================================
          HEADER
      ===================================== */}

      <section className="page-header">

        <div>

          <div className="eyebrow">
            NETWORK OPERATIONS
          </div>

          <h1>
            Network Alerts
          </h1>

          <p>
            Operational network conditions
            requiring attention across Milan.
          </p>

        </div>


        {asOf && (
          <div className="grid-reporting">

            <Clock3 size={15} />

            <div>

              <span>
                REPORTING AS OF
              </span>

              <strong>
                {formatDate(asOf)}
              </strong>

            </div>

          </div>
        )}

      </section>


      {/* =====================================
          ERROR
      ===================================== */}

      {error && (
        <section className="operation-error">

          <AlertTriangle size={17} />

          <span>
            {error}
          </span>

          <button
            className="secondary-action"
            onClick={loadAlerts}
          >
            <RefreshCw size={14} />
            Try again
          </button>

        </section>
      )}


      {/* =====================================
          MAIN
      ===================================== */}

      {!error && !loading && (
        <>

          {/* =====================================
              SUMMARY
          ===================================== */}

          <section className="operations-summary">

            <OperationalMetric
              icon={ShieldAlert}
              label="Total Alerts"
              value={formatCount(
                filteredAlerts.length
              )}
              description={
                alertType
                  ? "Matching alert records"
                  : "All alert records"
              }
            />


            <OperationalMetric
              icon={MapIcon}
              label="Map Coverage"
              value="Milan"
              description="10,000 network grid cells"
            />

          </section>


          {/* =====================================
              ALERT PANEL
          ===================================== */}

          <section className="operations-grid">

            <div className="operations-panel">

              {/* =================================
                  HEADER
              ================================= */}

              <div className="panel-heading operations-panel-heading">

                <div>

                  <div className="eyebrow">
                    CURRENT ALERTS
                  </div>

                  <h2>
                    Operational alerts
                  </h2>

                  <p>
                    Browse the complete alert record
                    set by alert type.
                  </p>

                </div>


                {/* =================================
                    TYPE FILTER
                ================================= */}

                <div className="panel-limit-control">

                  <label htmlFor="alert-type">
                    TYPE
                  </label>

                  <select
                    id="alert-type"
                    value={alertType}
                    onChange={
                      handleAlertTypeChange
                    }
                  >

                    {ALERT_TYPES.map(
                      (type) => (

                        <option
                          key={type.value}
                          value={type.value}
                        >
                          {type.label}
                        </option>

                      )
                    )}

                  </select>

                </div>

              </div>


              {/* =================================
                  RESULT SUMMARY
              ================================= */}

              <div className="alert-result-summary">

                <div>

                  <strong>
                    {formatCount(
                      filteredAlerts.length
                    )}
                  </strong>

                  <span>
                    {alertType
                      ? "matching alerts"
                      : "total alerts"}
                  </span>

                </div>


                <span>

                  {alertType
                    ? `Filtered from ${formatCount(
                        alerts.length
                      )} alerts`
                    : "Complete alert dataset"}

                </span>

              </div>


              {/* =================================
                  VIRTUAL ALERT LIST
              ================================= */}
                <PaginatedAlertList
                  alerts={filteredAlerts}
                />

            </div>

          </section>


          {/* =====================================
              MAP
          ===================================== */}

          <section className="map-panel">

            <div className="panel-heading">

              <div>

                <div className="eyebrow">
                  GEOGRAPHIC VIEW
                </div>

                <h2>
                  Milan network map
                </h2>

                <p>
                  All Milan grid cells are shown in
                  grey. Alert grids are highlighted
                  according to their operational severity.
                </p>

              </div>

            </div>


            <MilanNetworkMap
              alerts={filteredAlerts}
              filterKey={alertType}
            />

          </section>

        </>

      )}

    </div>
  );
}


/* =========================================
   SUMMARY METRIC
========================================= */

function OperationalMetric({
  icon: Icon,
  label,
  value,
  description,
}) {

  return (
    <article className="operations-metric">

      <div className="operations-metric-icon">
        <Icon size={18} />
      </div>

      <div className="operations-metric-label">
        {label}
      </div>

      <div className="operations-metric-value">
        {value}
      </div>

      <div className="operations-metric-description">
        {description}
      </div>

    </article>
  );
}


/* =========================================
   VIRTUAL ALERT LIST
========================================= */
function PaginatedAlertList({
  alerts,
}) {
  const PAGE_SIZE = 100;

  const [page, setPage] =
    useState(1);

  const totalPages =
    Math.max(
      1,
      Math.ceil(
        alerts.length / PAGE_SIZE
      )
    );

  /*
   * Reset to page 1 whenever the
   * filtered dataset changes.
   */
  useEffect(() => {
    setPage(1);
  }, [alerts]);


  const startIndex =
    (page - 1) * PAGE_SIZE;

  const endIndex =
    Math.min(
      startIndex + PAGE_SIZE,
      alerts.length
    );

  const pageAlerts =
    alerts.slice(
      startIndex,
      endIndex
    );


  function goToPage(
    targetPage
  ) {
    setPage(
      Math.min(
        Math.max(targetPage, 1),
        totalPages
      )
    );
  }


  /*
   * Build a compact page-number
   * sequence.
   *
   * Example:
   * 1 2 3 4 5 ... 1000
   */

  function getPageNumbers() {

    if (totalPages <= 7) {

      return Array.from(
        {
          length: totalPages,
        },
        (_, index) =>
          index + 1
      );

    }


    if (page <= 4) {

      return [
        1,
        2,
        3,
        4,
        5,
        "...",
        totalPages,
      ];

    }


    if (page >= totalPages - 3) {

      return [
        1,
        "...",
        totalPages - 4,
        totalPages - 3,
        totalPages - 2,
        totalPages - 1,
        totalPages,
      ];

    }


    return [
      1,
      "...",
      page - 1,
      page,
      page + 1,
      "...",
      totalPages,
    ];

  }


  return (
    <>

      {/* =====================================
          ALERT LIST
      ===================================== */}

      <div className="operations-list">

        {pageAlerts.length > 0 ? (

          pageAlerts.map(
            (item, index) => (

              <AlertRow
                key={`${item.grid_id}-${item.timestamp}-${item.alert_type}-${startIndex + index}`}
                rank={
                  startIndex +
                  index +
                  1
                }
                item={item}
              />

            )
          )

        ) : (

          <div className="operations-empty">
            No alerts were found for the
            selected alert type.
          </div>

        )}

      </div>


      {/* =====================================
          PAGINATION
      ===================================== */}

      {alerts.length > 0 && (
        <div className="alert-pagination">

          <button
            type="button"
            className="pagination-button"
            onClick={() =>
              goToPage(page - 1)
            }
            disabled={page === 1}
          >
            <span>‹</span>
            Previous
          </button>


          <div className="pagination-pages">

            {getPageNumbers().map(
              (pageNumber, index) => {

                if (
                  pageNumber === "..."
                ) {

                  return (
                    <span
                      key={`ellipsis-${index}`}
                      className="pagination-ellipsis"
                    >
                      …
                    </span>
                  );

                }


                return (
                  <button
                    key={pageNumber}
                    type="button"
                    className={
                      `pagination-page-button ${
                        pageNumber === page
                          ? "active"
                          : ""
                      }`
                    }
                    onClick={() =>
                      goToPage(
                        pageNumber
                      )
                    }
                  >
                    {pageNumber}
                  </button>
                );

              }
            )}

          </div>


          <button
            type="button"
            className="pagination-button"
            onClick={() =>
              goToPage(page + 1)
            }
            disabled={
              page === totalPages
            }
          >
            Next
            <span>›</span>
          </button>

        </div>
      )}


      {/* =====================================
          PAGE RANGE
      ===================================== */}

      {alerts.length > 0 && (
        <div className="alert-page-range">

          Showing{" "}
          {formatCount(
            startIndex + 1
          )}
          –
          {formatCount(
            endIndex
          )}
          {" "}of{" "}
          {formatCount(
            alerts.length
          )}
          {" "}alerts

        </div>
      )}

    </>
  );
}

/* =========================================
   ALERT ROW
========================================= */
function AlertRow({
  rank,
  item,
}) {
  const investigationPath =
    `/alerts/${item.grid_id}/${encodeURIComponent(item.timestamp)}`;

  return (
    <Link
      to={investigationPath}
      state={{ alert: item }}
      className="operation-row operation-row-link"
    >

      <div className="operation-rank">
        #{rank}
      </div>


      <div className="operation-main">

        <strong>
          Grid {item.grid_id}
        </strong>

        <span>
          {item.alert_type}
        </span>

      </div>


      <div className="operation-activity">

        <strong>
          {formatValue(
            item.current_activity
          )}
        </strong>

        <span>
          {formatHour(
            item.timestamp
          )}
        </span>

      </div>


      <StatusBadge
        severity={item.severity}
      />

      <ChevronRight
        size={15}
        className="operation-row-chevron"
      />

    </Link>
  );
}

/* =========================================
   STATUS BADGE
========================================= */

function StatusBadge({
  severity,
}) {

  const normalized =
    String(severity || "")
      .toUpperCase();


  const label =
    normalized === "HIGH"
      ? "HIGH"
      : normalized === "MEDIUM"
        ? "ATTENTION"
        : "NORMAL";


  return (
    <span
      className={`operation-status status-${label.toLowerCase()}`}
    >
      {label}
    </span>
  );
}


/* =========================================
   MILAN NETWORK MAP
========================================= */

function MilanNetworkMap({
  alerts,
  filterKey,
}) {

  const [geoJson, setGeoJson] =
    useState(null);

  const [
    selectedGrid,
    setSelectedGrid,
  ] = useState(null);


  /*
   * -----------------------------------------
   * LOAD GEOJSON ONCE
   * -----------------------------------------
   */

  useEffect(() => {

    fetch(
      "/references/milano-grid.geojson"
    )
      .then((response) => {

        if (!response.ok) {

          throw new Error(
            "Unable to load Milan grid map."
          );

        }

        return response.json();

      })
      .then((data) => {

        setGeoJson(data);

      })
      .catch(() => {

        setGeoJson(null);

      });

  }, []);


  /*
   * -----------------------------------------
   * RESET SELECTION WHEN FILTER CHANGES
   * -----------------------------------------
   */

  useEffect(() => {

    setSelectedGrid(null);

  }, [filterKey]);


  /*
   * -----------------------------------------
   * GRID STATUS
   * -----------------------------------------
   *
   * All 10,000 grids remain on the map.
   *
   * Only grids represented in the currently
   * filtered alert dataset receive a status.
   */

  const statusByGrid =
    useMemo(() => {

      const result =
        new Map();


      alerts.forEach((item) => {

        const gridId =
          Number(item.grid_id);


        const status =
          mapSeverity(
            item.severity
          );


        const existing =
          result.get(gridId);


        if (
          !existing ||
          severityRank(status) >
            severityRank(existing)
        ) {

          result.set(
            gridId,
            status
          );

        }

      });


      return result;

    }, [alerts]);


  /*
   * -----------------------------------------
   * FEATURE STYLE
   * -----------------------------------------
   */

  function getFeatureStyle(
    feature
  ) {

    const gridId =
      Number(
        feature.properties?.cellId
      );


    const status =
      statusByGrid.get(gridId) ||
      "NORMAL";


    /*
     * NORMAL = grey grid context.
     * ATTENTION = medium alert.
     * HIGH = high alert.
     */

    if (status === "HIGH") {

      return {
        color: "#ff6b72",
        weight:
          selectedGrid === gridId
            ? 2.5
            : 0.7,
        opacity: 1,
        fillColor: "#ff6b72",
        fillOpacity:
          selectedGrid === gridId
            ? 0.8
            : 0.58,
      };

    }


    if (status === "ATTENTION") {

      return {
        color: "#f0b35b",
        weight:
          selectedGrid === gridId
            ? 2.5
            : 0.7,
        opacity: 1,
        fillColor: "#f0b35b",
        fillOpacity:
          selectedGrid === gridId
            ? 0.78
            : 0.55,
      };

    }


    return {
      color: "#64748b",
      weight:
        selectedGrid === gridId
          ? 2
          : 0.45,
      opacity: 0.9,
      fillColor: "#64748b",
      fillOpacity:
        selectedGrid === gridId
          ? 0.35
          : 0.16,
    };
  }


  /*
   * -----------------------------------------
   * FEATURE INTERACTION
   * -----------------------------------------
   */

  function handleFeature(
    feature,
    layer
  ) {

    const gridId =
      Number(
        feature.properties?.cellId
      );


    const status =
      statusByGrid.get(gridId);


    /*
     * Only alert grids get operational
     * tooltips.
     *
     * Normal grey cells remain geographic
     * context only.
     */

    if (status) {

      const matchingAlerts =
        alerts.filter(
          (item) =>
            Number(
              item.grid_id
            ) === gridId
        );


      const alertTypes =
        [
          ...new Set(
            matchingAlerts.map(
              (item) =>
                formatAlertType(
                  item.alert_type
                )
            )
          ),
        ];


      layer.bindTooltip(
        `
          <div class="map-tooltip">
            <strong>Grid ${gridId}</strong>
            <span>${status}</span>
            <small>
              ${alertTypes.join(", ")}
            </small>
          </div>
        `,
        {
          sticky: true,
        }
      );

    }


    layer.on({

      click: () => {

        if (status) {

          setSelectedGrid(
            gridId
          );

        }

      },


      mouseover: () => {

        if (status) {

          layer.setStyle({

            weight: 2,

            fillOpacity: 0.85,

          });

        }

      },


      mouseout: () => {

        layer.setStyle(
          getFeatureStyle(
            feature
          )
        );

      },

    });

  }


  return (
    <div className="network-map">

      {/* =====================================
          LEGEND
      ===================================== */}

      <div className="map-status-legend">

        <MapLegend
          status="NORMAL"
          label="Normal"
        />

        <MapLegend
          status="ATTENTION"
          label="Attention"
        />

        <MapLegend
          status="HIGH"
          label="High"
        />

      </div>


      {/* =====================================
          MAP
      ===================================== */}

      {geoJson ? (

        <MapContainer
          center={[
            45.4642,
            9.19,
          ]}
          zoom={11}
          scrollWheelZoom={true}
          className="milan-map"
        >

          <TileLayer
            attribution="&copy; OpenStreetMap contributors"
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />


          <GeoJSON
            key={
              `${filterKey}-${alerts.length}`
            }
            data={geoJson}
            style={getFeatureStyle}
            onEachFeature={
              handleFeature
            }

            /*
             * Canvas renderer is used instead of
             * individual SVG paths for the 10,000
             * Milan grid polygons.
             */

            renderer={L.canvas({
              padding: 0.5,
            })}
          />

        </MapContainer>

      ) : (

        <div className="map-loading">
          Loading Milan network map…
        </div>

      )}

    </div>
  );
}


/* =========================================
   MAP SEVERITY
========================================= */

function mapSeverity(
  severity
) {

  const value =
    String(severity || "")
      .toUpperCase();


  if (value === "HIGH") {
    return "HIGH";
  }


  if (value === "MEDIUM") {
    return "ATTENTION";
  }


  return "NORMAL";
}


/* =========================================
   SEVERITY RANK
========================================= */

function severityRank(
  severity
) {

  if (severity === "HIGH") {
    return 3;
  }


  if (severity === "ATTENTION") {
    return 2;
  }


  return 1;
}


/* =========================================
   MAP LEGEND
========================================= */

function MapLegend({
  status,
  label,
}) {

  return (
    <div className="map-legend-item">

      <span
        className={`map-legend-dot map-dot-${status.toLowerCase()}`}
      />

      <span>
        {label}
      </span>

    </div>
  );
}


/* =========================================
   ALERT TYPE FORMAT
========================================= */

function formatAlertType(
  value
) {

  if (!value) {
    return "Unknown alert";
  }


  return String(value)
    .toLowerCase()
    .split("_")
    .map(
      (word) =>
        word.charAt(0).toUpperCase() +
        word.slice(1)
    )
    .join(" ");
}


/* =========================================
   COUNT FORMAT
========================================= */

function formatCount(
  value
) {

  return Number(
    value || 0
  ).toLocaleString(
    "en-US"
  );
}


/* =========================================
   ACTIVITY FORMAT
========================================= */

function formatValue(
  value
) {

  const number =
    Number(value) || 0;


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


/* =========================================
   HOUR FORMAT
========================================= */

function formatHour(
  timestamp
) {

  if (!timestamp) {
    return "—";
  }


  const value =
    String(timestamp)
      .split("T")[1];


  return value
    ? value.slice(0, 5)
    : "—";
}


/* =========================================
   DATE FORMAT
========================================= */

function formatDate(
  value
) {

  if (!value) {
    return "—";
  }


  return new Date(value)
    .toLocaleString(
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