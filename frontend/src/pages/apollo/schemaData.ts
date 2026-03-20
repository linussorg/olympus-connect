export interface SchemaField {
  name: string;
  type: string;
  description: string;
}

export interface SchemaTable {
  id: string;
  label: string;
  description: string;
  category: string;
  recordCount: number;
  sources: string[];
  fields: SchemaField[];
}

export const schemaTables: SchemaTable[] = [
  {
    id: "tbCaseData",
    label: "Falldaten",
    description: "Stammdaten zu Patientenfällen inkl. Aufnahme, Entlassung und Diagnosen.",
    category: "Stammdaten",
    recordCount: 12_847,
    sources: ["KIS Export (CSV)", "HL7 ADT Feed"],
    fields: [
      { name: "coE2I222", type: "bigint", description: "Fall-ID" },
      { name: "coPatientId", type: "bigint", description: "Patienten-ID" },
      { name: "coE2I223", type: "datetime", description: "Aufnahmedatum" },
      { name: "coE2I228", type: "datetime", description: "Entlassdatum" },
      { name: "coLastname", type: "nvarchar(256)", description: "Nachname" },
      { name: "coFirstname", type: "nvarchar(256)", description: "Vorname" },
      { name: "coGender", type: "nvarchar(256)", description: "Geschlecht" },
      { name: "coDateOfBirth", type: "datetime", description: "Geburtsdatum" },
      { name: "coAgeYears", type: "int", description: "Alter in Jahren" },
      { name: "coTypeOfStay", type: "nvarchar(256)", description: "Aufenthaltsart" },
      { name: "coIcd", type: "nvarchar(256)", description: "ICD-Hauptdiagnose" },
      { name: "coDrgName", type: "nvarchar(256)", description: "DRG-Bezeichnung" },
      { name: "coRecliningType", type: "nvarchar(256)", description: "Lagerungstyp" },
      { name: "coState", type: "nvarchar(256)", description: "Status" },
    ],
  },
  {
    id: "tbImportLabsData",
    label: "Labordaten",
    description: "Laborwerte für 15 Analyte mit Referenzbereichen und Flags.",
    category: "Diagnostik",
    recordCount: 248_320,
    sources: ["LIS Export (HL7)", "Labor-CSV Batch"],
    fields: [
      { name: "coCaseId", type: "bigint", description: "Fall-ID (FK)" },
      { name: "coSpecimen_datetime", type: "datetime", description: "Entnahmezeitpunkt" },
      { name: "coSodium_value", type: "nvarchar(256)", description: "Natrium – Wert" },
      { name: "coSodium_flag", type: "nvarchar(256)", description: "Natrium – Flag (H/L/N)" },
      { name: "coCRP_value", type: "nvarchar(256)", description: "CRP – Wert" },
      { name: "coCRP_flag", type: "nvarchar(256)", description: "CRP – Flag" },
      { name: "coHemoglobin_value", type: "nvarchar(256)", description: "Hämoglobin – Wert" },
      { name: "coCreatinine_value", type: "nvarchar(256)", description: "Kreatinin – Wert" },
      { name: "coLactate_value", type: "nvarchar(256)", description: "Laktat – Wert" },
    ],
  },
  {
    id: "tbImportIcd10Data",
    label: "ICD-10 / OPS",
    description: "Diagnose- und Prozedurencodes mit Verweildauer und Stationszuordnung.",
    category: "Diagnostik",
    recordCount: 45_120,
    sources: ["KIS Diagnose-Export", "PEPP-Grouper"],
    fields: [
      { name: "coCaseId", type: "bigint", description: "Fall-ID (FK)" },
      { name: "coWard", type: "nvarchar(256)", description: "Station" },
      { name: "coAdmission_date", type: "datetime", description: "Aufnahmedatum" },
      { name: "coDischarge_date", type: "datetime", description: "Entlassdatum" },
      { name: "coLOS", type: "int", description: "Verweildauer (Tage)" },
      { name: "coPrimary_icd10_code", type: "nvarchar(256)", description: "Haupt-ICD-10 Code" },
      { name: "coPrimary_icd10_description", type: "nvarchar(256)", description: "Beschreibung" },
      { name: "coSecondary_icd10_code", type: "nvarchar(256)", description: "Nebendiagnose" },
      { name: "coOPS_code", type: "nvarchar(256)", description: "OPS-Code" },
      { name: "coOPS_description", type: "nvarchar(256)", description: "OPS-Beschreibung" },
    ],
  },
  {
    id: "tbImportAcData",
    label: "Assessments (epaAC)",
    description: "Pflegerische Assessments inkl. Dekubitusrisiko und Scores.",
    category: "Pflege",
    recordCount: 18_640,
    sources: ["epaAC Assessment Tool"],
    fields: [
      { name: "coCaseId", type: "bigint", description: "Fall-ID (FK)" },
      { name: "coMaxDekuGrad", type: "smallint", description: "Max. Dekubitusgrad" },
      { name: "coDekubitusWertTotal", type: "smallint", description: "Dekubitus-Gesamtscore" },
      { name: "coLastAssessment", type: "datetime", description: "Letztes Assessment" },
      { name: "coE3I0889", type: "nvarchar(512)", description: "Freitext-Assessment" },
      { name: "coCaseIdAlpha", type: "nvarchar(256)", description: "Alpha-Fall-ID" },
    ],
  },
  {
    id: "tbImportDeviceMotionData",
    label: "Bewegungsdaten (stündlich)",
    description: "Aggregierte Bewegungs- und Sturzsensordaten pro Stunde.",
    category: "Sensorik",
    recordCount: 1_245_000,
    sources: ["IoT Sensor Gateway", "Device Export (JSON)"],
    fields: [
      { name: "coCaseId", type: "bigint", description: "Fall-ID (FK)" },
      { name: "coTimestamp", type: "datetime", description: "Zeitstempel" },
      { name: "coPatient_id", type: "bigint", description: "Patienten-ID" },
      { name: "coMovement_index", type: "float", description: "Bewegungsindex" },
      { name: "coMicro_movements", type: "int", description: "Mikrobewegungen" },
      { name: "coBed_exit", type: "bit", description: "Bettausstieg" },
      { name: "coFall_event", type: "bit", description: "Sturzereignis" },
      { name: "coImpact_magnitude", type: "float", description: "Aufprallstärke" },
      { name: "coPost_fall_immobility", type: "int", description: "Immobilität nach Sturz (s)" },
    ],
  },
  {
    id: "tbImportDevice1HzMotionData",
    label: "Bewegungsdaten (1 Hz)",
    description: "Rohe IMU-Daten mit Druckzonen und Bett-Events bei 1 Hz.",
    category: "Sensorik",
    recordCount: 84_200_000,
    sources: ["IoT Sensor Gateway (Raw Stream)"],
    fields: [
      { name: "coCaseId", type: "bigint", description: "Fall-ID (FK)" },
      { name: "coTimestamp", type: "datetime", description: "Zeitstempel" },
      { name: "coDevice_id", type: "nvarchar(256)", description: "Geräte-ID" },
      { name: "coAccel_x", type: "float", description: "Beschleunigung X" },
      { name: "coAccel_y", type: "float", description: "Beschleunigung Y" },
      { name: "coAccel_z", type: "float", description: "Beschleunigung Z" },
      { name: "coPressure_zone_1", type: "float", description: "Druckzone 1" },
      { name: "coFall_event", type: "bit", description: "Sturzereignis" },
    ],
  },
  {
    id: "tbImportMedicationInpatientData",
    label: "Medikation",
    description: "Medikationsverordnungen, Änderungen und Verabreichungen.",
    category: "Therapie",
    recordCount: 67_890,
    sources: ["KIS Medikationsmodul", "PDMS Export"],
    fields: [
      { name: "coCaseId", type: "bigint", description: "Fall-ID (FK)" },
      { name: "coRecord_type", type: "nvarchar(256)", description: "Typ (ORDER/CHANGE/ADMIN)" },
      { name: "coATC_code", type: "nvarchar(256)", description: "ATC-Code" },
      { name: "coMedication_name", type: "nvarchar(256)", description: "Medikamentenname" },
      { name: "coRoute", type: "nvarchar(256)", description: "Verabreichungsweg" },
      { name: "coDose_value", type: "nvarchar(256)", description: "Dosis" },
      { name: "coDose_unit", type: "nvarchar(256)", description: "Einheit" },
      { name: "coFrequency", type: "nvarchar(256)", description: "Frequenz" },
      { name: "coStart_datetime", type: "datetime", description: "Startzeit" },
      { name: "coStop_datetime", type: "datetime", description: "Stoppzeit" },
    ],
  },
  {
    id: "tbImportNursingDailyReportsData",
    label: "Pflegeberichte",
    description: "Tägliche Freitext-Pflegeberichte nach Schicht.",
    category: "Pflege",
    recordCount: 34_500,
    sources: ["epaAC Pflegedoku", "Papier-Scan (OCR)"],
    fields: [
      { name: "coCaseId", type: "bigint", description: "Fall-ID (FK)" },
      { name: "coPatient_id", type: "bigint", description: "Patienten-ID" },
      { name: "coWard", type: "nvarchar(256)", description: "Station" },
      { name: "coReport_date", type: "datetime", description: "Berichtsdatum" },
      { name: "coShift", type: "nvarchar(256)", description: "Schicht (Früh/Spät/Nacht)" },
      { name: "coNursing_note_free_text", type: "nvarchar(max)", description: "Pflegebericht (Freitext)" },
    ],
  },
];

export const categories = [...new Set(schemaTables.map((t) => t.category))];
