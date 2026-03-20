export type AnomalyType = "mapping-source" | "mapping-target" | "mapping" | "data" | "demographic";

export interface Anomaly {
  id: number;
  type: AnomalyType;
  targetTable: string;
  field: string;
  sourceColumn?: string;
  confidence?: number;
  patientId?: string;
  patientName?: string;
  currentValue: string;
  proposedValue: string;
  reason: string;
  status: "pending" | "accepted" | "manual" | "ignored";
  manualValue?: string;
  candidates?: { target: string; confidence: number; reason: string }[];
  category?: string;
  origin?: string;
  affectedCount?: number;
  row?: number;
}
