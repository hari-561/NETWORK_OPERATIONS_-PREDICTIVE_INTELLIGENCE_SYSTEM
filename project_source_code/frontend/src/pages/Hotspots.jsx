import { useEffect, useState } from "react";

import {
  AlertTriangle,
  Clock3,
  Map as MapIcon,
  RefreshCw,
  TrendingUp,
} from "lucide-react";

import {
  MapContainer,
  TileLayer,
  GeoJSON,
} from "react-leaflet";

import {
  getHotspots,
} from "../services/api";


export default function Hotspots() {
  const [hotspots, setHotspots] = useState([]);

  const [asOf, setAsOf] = useState(null);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const [hotspotLimit, setHotspotLimit] =
    useState(10);


  /*
   * -----------------------------------------
   * LOAD HOTSPOTS
   * -----------------------------------------
   */

  useEffect(() => {
    loadHotspots();
  }, [hotspotLimit]);


  async function loadHotspots() {
    try {
      setLoading(true);
      setError(null);

      const response =
        await getHotspots(hotspotLimit);

      setHotspots(
        response?.data || []
      );

      setAsOf(
        response?.as_of || null
      );

    } catch (err) {
      setHotspots([]);

      setError(
        "We couldn't retrieve current network hotspots. Please try again."
      );

    } finally {
      setLoading(false);
    }
  }


  return (
    <div className="page hotspots-page">

      {/* =====================================
          HEADER
      ===================================== */}

      <section className="page-header">

        <div>

          <div className="eyebrow">
            NETWORK OPERATIONS
          </div>

          <h1>
            Network Hotspots
          </h1>

          <p>
            Prioritized network activity areas
            across Milan.
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
            onClick={loadHotspots}
          >
            <RefreshCw size={14} />
            Try again
          </button>

        </section>
      )}


      {/* =====================================
          CONTENT
      ===================================== */}

      {!error && !loading && (
        <>

          {/* =====================================
              SUMMARY
          ===================================== */}

          <section className="operations-summary">

            <OperationalMetric
              icon={TrendingUp}
              label="Priority Hotspots"
              value={hotspots.length}
              description="High-activity areas"
            />

            <OperationalMetric
              icon={MapIcon}
              label="Map Coverage"
              value="Milan"
              description="Network grid view"
            />

          </section>


          {/* =====================================
              HOTSPOT LIST
          ===================================== */}

          <section className="operations-grid">

            <div className="operations-panel">

              <div className="panel-heading operations-panel-heading">

                <div>

                  <div className="eyebrow">
                    PRIORITY AREAS
                  </div>

                  <h2>
                    Network hotspots
                  </h2>

                  <p>
                    Grids with unusually high
                    activity.
                  </p>

                </div>


                <div className="panel-limit-control">

                  <label htmlFor="hotspot-limit">
                    SHOW
                  </label>

                  <select
                    id="hotspot-limit"
                    value={hotspotLimit}
                    onChange={(event) =>
                      setHotspotLimit(
                        Number(
                          event.target.value
                        )
                      )
                    }
                  >
                    <option value={5}>
                      5
                    </option>

                    <option value={10}>
                      10
                    </option>

                    <option value={25}>
                      25
                    </option>

                    <option value={50}>
                      50
                    </option>

                    <option value={100}>
                      100
                    </option>

                  </select>

                </div>

              </div>


              <div className="operations-list">

                {hotspots.length > 0 ? (

                  hotspots.map(
                    (item, index) => (
                      <OperationalRow
                        key={`${item.grid_id}-${item.timestamp}`}
                        rank={index + 1}
                        item={item}
                      />
                    )
                  )

                ) : (

                  <div className="operations-empty">
                    No network hotspots were detected
                    for the current reporting period.
                  </div>

                )}

              </div>

            </div>

          </section>


          {/* =====================================
              MILAN MAP
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
                  Select a network grid to inspect
                  its activity in Grid Explorer.
                </p>

              </div>

            </div>


            <MilanNetworkMap
              hotspots={hotspots}
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
   HOTSPOT ROW
========================================= */

function OperationalRow({
  rank,
  item,
}) {
  return (
    <div className="operation-row">

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
          {formatHour(item.timestamp)}
        </span>

      </div>


      <StatusBadge
        severity={item.severity}
      />

    </div>
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
  hotspots,
}) {
  const [geoJson, setGeoJson] =
    useState(null);

  const [selectedGrid, setSelectedGrid] =
    useState(null);


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
   * BUILD GRID STATUS
   * -----------------------------------------
   */

  const statusByGrid = new Map();


  hotspots.forEach((item) => {

    statusByGrid.set(
      Number(item.grid_id),
      "HIGH"
    );

  });


  /*
   * -----------------------------------------
   * FEATURE STYLE
   * -----------------------------------------
   */

  function getFeatureStyle(feature) {

    const gridId =
      Number(
        feature.properties?.cellId
      );


    const status =
      statusByGrid.get(gridId) ||
      "NORMAL";


    return {

      className:
        `milan-grid milan-grid-${status.toLowerCase()}`,

      weight:
        selectedGrid === gridId
          ? 2.5
          : 0.6,

      opacity: 1,

      fillOpacity:
        selectedGrid === gridId
          ? 0.75
          : status === "NORMAL"
            ? 0.25
            : 0.65,
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
      statusByGrid.get(gridId) ||
      "NORMAL";


    layer.bindTooltip(
      `
        <div class="map-tooltip">
          <strong>Grid ${gridId}</strong>
          <span>${status}</span>
        </div>
      `,
      {
        sticky: true,
      }
    );


    layer.on({

      click: () => {
        setSelectedGrid(gridId);
      },


      mouseover: () => {

        layer.setStyle({
          weight: 2,
          fillOpacity: 0.8,
        });

      },


      mouseout: () => {

        layer.setStyle(
          getFeatureStyle(feature)
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
          center={[45.4642, 9.19]}
          zoom={11}
          scrollWheelZoom={true}
          className="milan-map"
        >

          <TileLayer
            attribution="&copy; OpenStreetMap contributors"
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          <GeoJSON
            key={selectedGrid}
            data={geoJson}
            style={getFeatureStyle}
            onEachFeature={handleFeature}
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
   FORMATTERS
========================================= */

function formatValue(value) {

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


function formatHour(timestamp) {

  if (!timestamp) {
    return "—";
  }


  const value =
    String(timestamp).split("T")[1];


  return value
    ? value.slice(0, 5)
    : "—";
}


function formatDate(value) {

  if (!value) {
    return "—";
  }


  return new Date(value).toLocaleString(
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