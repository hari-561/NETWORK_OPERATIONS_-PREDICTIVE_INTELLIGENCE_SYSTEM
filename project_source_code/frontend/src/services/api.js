const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ||
  "http://localhost:8000";


async function request(
  url,
  options = {}
) {
  const response = await fetch(
    url,
    options
  );

  if (!response.ok) {
    const error =
      new Error(
        `Request failed with status ${response.status}`
      );

    error.status = response.status;

    try {
      const body = await response.json();

      error.detail =
        body?.detail ||
        body?.message ||
        null;
    } catch {
      error.detail = null;
    }

    throw error;
  }

  return response.json();
}


export async function getNetworkSummary() {
  return request(
    `${API_BASE_URL}/network/summary`
  );
}


export async function getGridActivity(
  gridId,
  { date, hour, asOf } = {}
) {
  const params = new URLSearchParams();

  if (date) {
    params.set("date", date);
  }

  if (hour !== undefined && hour !== null) {
    params.set("hour", hour);
  }

  if (asOf) {
    params.set("as_of", asOf);
  }

  const queryString = params.toString();

  const url =
    `${API_BASE_URL}/network/grid/${gridId}` +
    (queryString ? `?${queryString}` : "");

  return request(url);
}


export async function getHotspots(limit = 10) {
  return request(
    `${API_BASE_URL}/network/hotspots?limit=${limit}`
  );
}

export async function getAlerts(limit = 99999, { asOf, severity } = {}) {
  const params = new URLSearchParams();

  params.set("limit", limit);

  if (asOf) {
    params.set("as_of", asOf);
  }

  if (severity) {
    params.set("severity", severity);
  }

  return request(
    `${API_BASE_URL}/network/alerts?${params.toString()}`
  );
}
export async function getGridFeatures(gridId, { asOf } = {}) {
  const params = new URLSearchParams();

  if (asOf) {
    params.set("as_of", asOf);
  }

  const query = params.toString();

  return request(
    `${API_BASE_URL}/network/grid/${gridId}/features${
      query ? `?${query}` : ""
    }`
  );
}



export async function predictRisk(payload) {
  return request(
    `${API_BASE_URL}/network/predict-risk`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    }
  );
}


export async function getGridAnomaly(gridId, { asOf } = {}) {
  const params = new URLSearchParams();

  if (asOf) {
    params.set("as_of", asOf);
  }

  const query = params.toString();

  return request(
    `${API_BASE_URL}/network/grid/${gridId}/anomaly${
      query ? `?${query}` : ""
    }`
  );
}


export async function getGridLocation(gridId) {
  return request(
    `${API_BASE_URL}/network/grid/${gridId}/location`
  );
}


export async function getPipelineStatus() {
  return request(
    `${API_BASE_URL}/pipeline/status`
  );
}


export async function getApiHealth() {
  return request(
    `${API_BASE_URL}/health`
  );
}