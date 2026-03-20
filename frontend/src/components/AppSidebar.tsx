import { NavLink, useLocation } from "react-router-dom";
import { Settings, Database } from "lucide-react";
import { useI18n } from "@/lib/i18n";
import logoImg from "../assets/Logo_Olympus.png";
import apolloImg from "../assets/Apollo.png";
import athenaImg from "../assets/Athena.png";
import hermesImg from "../assets/Hermes.png";

const AppSidebar = () => {
  const location = useLocation();
  const { t } = useI18n();

  const navTiles = [
    { to: "/", label: "Apollo", desc: t("sidebar.apollo"), img: apolloImg },
    { to: "/athena", label: "Athena", desc: t("sidebar.athena"), img: athenaImg },
    { to: "/hermes", label: "Hermes", desc: t("sidebar.hermes"), img: hermesImg },
  ];

  return (
    <aside className="w-64 h-screen sticky top-0 border-r border-border bg-card flex flex-col overflow-hidden">
      <div className="p-4 border-b border-border flex items-center gap-3">
        <img src={logoImg} alt="Olympus Connect" className="h-10 w-auto" />
        <div>
          <h1 className="text-2xl font-bold tracking-wide text-foreground leading-tight">Olympus<br />Connect</h1>
        </div>
      </div>

      <nav className="flex-1 flex flex-col gap-4 p-3 overflow-y-auto">
        {navTiles.map((tile) => {
          const isActive =
            location.pathname === tile.to ||
            (tile.to === "/" && location.pathname === "/apollo");
          return (
            <NavLink
              key={tile.to}
              to={tile.to}
              className={`group relative flex flex-col rounded-xl overflow-hidden transition-all duration-200 ${
                isActive
                  ? "ring-2 ring-primary shadow-lg scale-[1.02]"
                  : "ring-1 ring-border hover:ring-primary/50 hover:shadow-md hover:scale-[1.01]"
              }`}
            >
              <div className="relative h-32 overflow-visible bg-white">
                <img
                  src={tile.img}
                  alt={tile.label}
                  className={`w-full h-full p-2 object-contain object-center transition-all duration-300 ${
                    isActive
                      ? "opacity-90 scale-105"
                      : "opacity-60 group-hover:opacity-80 group-hover:scale-105"
                  }`}
                />
                <span className={`absolute -bottom-8 left-3 text-3xl font-black tracking-tight leading-none transition-colors ${
                  isActive ? "text-primary" : "text-foreground/80 group-hover:text-primary"
                }`}>
                  {tile.label}
                </span>
              </div>
              <div className="px-3 pt-9 pb-2">
                <p className="text-[11px] text-muted-foreground">{tile.desc}</p>
              </div>
            </NavLink>
          );
        })}
      </nav>

      <div className="border-t border-border">
        <NavLink
          to="/explorer"
          className={`flex items-center gap-3 px-4 py-2.5 text-sm transition-colors ${
            location.pathname === "/explorer"
              ? "bg-sidebar-accent text-primary font-medium"
              : "text-muted-foreground hover:bg-muted hover:text-foreground"
          }`}
        >
          <Database className="h-4 w-4" />
          <span>{t("sidebar.explorer")}</span>
        </NavLink>
        <NavLink
          to="/settings"
          className={`flex items-center gap-3 px-4 py-2.5 text-sm transition-colors ${
            location.pathname === "/settings"
              ? "bg-sidebar-accent text-primary font-medium"
              : "text-muted-foreground hover:bg-muted hover:text-foreground"
          }`}
        >
          <Settings className="h-4 w-4" />
          <span>{t("sidebar.settings")}</span>
        </NavLink>
      </div>
    </aside>
  );
};

export default AppSidebar;
