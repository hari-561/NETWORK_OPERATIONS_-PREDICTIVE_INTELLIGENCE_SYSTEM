import {
  Activity,
  AlertTriangle,
  BrainCircuit,
  GitCompareArrows,
  Grid3X3,
  HeartPulse,
  MapPinned,
  Network,
} from "lucide-react";

import { NavLink } from "react-router-dom";

const navigationGroups = [
  {
    title: "MONITORING",
    items: [
      {
        label: "Overview",
        path: "/overview",
        icon: Activity,
      },
      {
        label: "Grid Explorer",
        path: "/grids",
        icon: Grid3X3,
      },
      {
        label: "Hotspots",
        path: "/hotspots",
        icon: MapPinned,
      },
    ],
  },
  {
    title: "OPERATIONS",
    items: [
      {
        label: "Alerts",
        path: "/alerts",
        icon: AlertTriangle,
      },
      {
        label: "Compare",
        path: "/compare",
        icon: GitCompareArrows,
      },
    ],
  },
  {
    title: "INTELLIGENCE",
    items: [
      {
        label: "Risk Intelligence",
        path: "/risk",
        icon: BrainCircuit,
      },
      {
        label: "Pipeline Health",
        path: "/pipeline",
        icon: HeartPulse,
      },
    ],
  },
];

export default function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">
          <Network size={20} />
        </div>

        <div>
          <div className="brand-name">NEXUS</div>
          <div className="brand-subtitle">NETWORK OPERATIONS</div>
        </div>
      </div>

      {navigationGroups.map((group) => (
        <div className="nav-section" key={group.title}>
          <span className="nav-section-title">{group.title}</span>

          <nav>
            {group.items.map(({ label, path, icon: Icon }) => (
              <NavLink
                key={path}
                to={path}
                className={({ isActive }) =>
                  `nav-item ${isActive ? "active" : ""}`
                }
              >
                <Icon size={18} strokeWidth={1.8} />
                <span>{label}</span>
              </NavLink>
            ))}
          </nav>
        </div>
      ))}

      <div className="sidebar-footer">
        <div className="system-indicator">
          <span className="status-dot" />
          <div>
            <div className="system-label">SYSTEM STATUS</div>
            <div className="system-value">Operational</div>
          </div>
        </div>
      </div>
    </aside>
  );
}