import { Clock3, Radio } from "lucide-react";

export default function Topbar() {
  return (
    <header className="topbar">
      <div className="topbar-left">
        <div className="live-indicator">
          <span className="live-dot" />
          LIVE MONITORING
        </div>
      </div>

      <div className="topbar-right">
        <div className="api-indicator">
          <Radio size={16} />
          <span>API CONNECTED</span>
        </div>

        <div className="time-display">
          <Clock3 size={16} />
          <span>
            {new Date().toLocaleTimeString([], {
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            })}
          </span>
        </div>
      </div>
    </header>
  );
}