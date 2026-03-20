import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes, Navigate } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { I18nProvider } from "./lib/i18n";
import { ChatProvider } from "./hooks/use-chat";
import DashboardLayout from "./components/DashboardLayout";
import HermesPage from "./pages/hermes/HermesPage";
import Apollo from "./pages/Apollo";
import Athena from "./pages/Athena";
import Settings from "./pages/Settings";
import MappingOverrides from "./pages/MappingOverrides";
import Explorer from "./pages/Explorer";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient();

const App = () => (
  <QueryClientProvider client={queryClient}>
    <I18nProvider>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <ChatProvider>
      <BrowserRouter>
        <Routes>
          <Route element={<DashboardLayout />}>
            <Route path="/" element={<Apollo />} />
            <Route path="/apollo" element={<Apollo />} />
            <Route path="/hermes" element={<HermesPage />} />
            <Route path="/hermes-upload" element={<Navigate to="/hermes" replace />} />
            <Route path="/jobs" element={<Navigate to="/hermes" replace />} />
            <Route path="/athena" element={<Athena />} />
            <Route path="/mapping-rules" element={<MappingOverrides />} />
            <Route path="/explorer" element={<Explorer />} />
            <Route path="/settings" element={<Settings />} />
          </Route>
          <Route path="*" element={<NotFound />} />
        </Routes>
      </BrowserRouter>
      </ChatProvider>
    </TooltipProvider>
    </I18nProvider>
  </QueryClientProvider>
);

export default App;
