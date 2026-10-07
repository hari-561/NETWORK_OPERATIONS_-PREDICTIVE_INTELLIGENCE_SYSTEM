import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import AppLayout from "./components/layout/AppLayout";

import Overview from "./pages/Overview";
import Grids from "./pages/Grids";
import Hotspots from "./pages/Hotspots";
import Alerts from "./pages/Alerts";
import AlertDetail from "./pages/AlertDetail";
import Risk from "./pages/Risk";
import Compare from "./pages/Compare";
import PipelineHealth from "./pages/PipelineHealth";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<Navigate to="/overview" replace />} />

          <Route path="/overview" element={<Overview />} />
          <Route path="/grids" element={<Grids />} />
          <Route path="/hotspots" element={<Hotspots />} />
          <Route path="/alerts" element={<Alerts />} />
          <Route
            path="/alerts/:gridId/:timestamp"
            element={<AlertDetail />}
          />
          <Route path="/risk" element={<Risk />} />
          <Route path="/compare" element={<Compare />} />
          <Route path="/pipeline" element={<PipelineHealth />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;