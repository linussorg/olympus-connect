import { useSearchParams, useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Link2 } from "lucide-react";
import { useI18n } from "@/lib/i18n";
import HermesListView from "./HermesListView";
import HermesDetailView from "./HermesDetailView";

const HermesPage = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { t } = useI18n();
  const jobId = searchParams.get("jobId");

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-xl font-semibold text-foreground">Hermes</h2>
          <p className="text-sm text-muted-foreground mt-0.5">{t("hermes.subtitle")}</p>
        </div>
        <Button variant="outline" size="sm" onClick={() => navigate("/mapping-rules")}>
          <Link2 className="h-3.5 w-3.5 mr-1.5" />
          {t("hermes.mappingRules")}
        </Button>
      </div>
      {jobId ? <HermesDetailView jobId={jobId} /> : <HermesListView />}
    </div>
  );
};

export default HermesPage;
