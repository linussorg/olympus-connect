import { Outlet } from "react-router-dom";
import AppSidebar from "./AppSidebar";

const DashboardLayout = () => (
  <div className="flex min-h-screen bg-background">
    <AppSidebar />
    <main className="flex-1 overflow-auto">
      <Outlet />
    </main>
  </div>
);

export default DashboardLayout;
