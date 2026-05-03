import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import express from "express";
import axios from "axios";
import XLSX from "xlsx";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "6mb" }));

const PORT = Number(process.env.PORT || 3000);
const BASE_URL = process.env.ADEME_BASE_URL || "https://data.ademe.fr/api/records/1.0/search/";
const DATASET = process.env.ADEME_DATASET || "base-carboner";
const DATA_DIR = path.join(__dirname, "data");

const BASE_CARBONE_CANDIDATES = [
  "Base_Carbone_V23.9.csv",
  "Base_Carbone_V23.6.csv"
];

const AGRIBALYSE_PRODUCTS_CANDIDATES = [
  "AGRIBALYSE3.2_Tableur produits alimentaires_PublieAOUT25.xlsx",
  "AGRIBALYSE3.2_Tableur-produits-alimentaires_PublieAOUT25.xlsx"
];

const AGRIBALYSE_AGRI_CANDIDATES = [
  "AGRIBALYSE3.2_partie agriculture_conv_PublieNOV25.xlsx",
  "AGRIBALYSE3.2_partie-agriculture_conv_PublieNOV25.xlsx"
];

const EXTRA_DATASET_PATTERNS = [
  /^BI_3\.0_/i,
  /^Tableau-de-suivi/i,
  /^donnees-aet-base-empreinte/i
];

const server = new McpServer({
  name: "mcp-carbone-hermes",
  version: "5.1.0"
});

function normalize(text = "") {
  return String(text)
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^a-z0-9\s.,/+\-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;

  const cleaned = String(value)
    .replace(/\u00a0/g, " ")
    .replace(/\s/g, "")
    .replace(",", ".")
    .replace(/[^\d.-]/g, "");

  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function firstDefined(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function makeSearchText(parts) {
  return normalize(parts.filter(Boolean).join(" "));
}

function resolveFirstExistingFile(dir, candidates) {
  for (const name of candidates) {
    const fullPath = path.join(dir, name);
    if (fs.existsSync(fullPath)) return fullPath;
  }
  return null;
}

function listExtraDatasetFiles(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((name) => EXTRA_DATASET_PATTERNS.some((pattern) => pattern.test(name)))
    .map((name) => path.join(dir, name));
}

const MATERIAL_TAXONOMY = [
  { canonical: "verre", aliases: ["verre", "bouteille verre", "flacon verre", "emballage verre"] },
  { canonical: "aluminium", aliases: ["aluminium", "alu", "canette", "canette alu", "boite alu"] },
  { canonical: "acier", aliases: ["acier", "inox", "acier galvanise", "galvanise", "metal acier"] },
  { canonical: "carton", aliases: ["carton", "papier", "papier carton", "carton d emballage", "emballage carton"] },
  { canonical: "pet", aliases: ["pet", "plastique pet", "bouteille pet", "flacon pet", "emballage pet"] },
  { canonical: "pp", aliases: ["pp", "polypropylene", "plastique pp", "barquette pp"] },
  { canonical: "plastique", aliases: ["plastique", "emballage plastique", "film plastique", "barquette plastique"] },
  { canonical: "bois", aliases: ["bois", "palette bois", "support bois", "emballage bois"] },
  { canonical: "cuivre", aliases: ["cuivre"] }
];

const TRANSPORT_TAXONOMY = [
  { canonical: "transport routier", aliases: ["camion", "routier", "poids lourd", "semi", "semi remorque", "transport camion"] },
  { canonical: "transport maritime", aliases: ["maritime", "bateau", "navire", "cargo"] },
  { canonical: "transport aérien", aliases: ["aerien", "avion", "fret aerien"] },
  { canonical: "transport ferroviaire", aliases: ["train", "rail", "ferroviaire"] }
];

const BUSINESS_OBJECTS = [
  {
    canonical: "palette europe",
    aliases: ["palette europe", "euro palette", "palette eur", "palette epal"],
    default_material: "bois",
    default_mass_kg: 25,
    official_search_queries: ["palette bois", "emballage bois", "support bois", "palette"],
    confidence: "medium"
  },
  {
    canonical: "palette",
    aliases: ["palette", "palette bois"],
    default_material: "bois",
    default_mass_kg: 25,
    official_search_queries: ["palette bois", "emballage bois", "support bois"],
    confidence: "medium"
  },
  {
    canonical: "canette",
    aliases: ["canette", "canette alu"],
    default_material: "aluminium",
    default_mass_kg: null,
    official_search_queries: ["canette aluminium", "aluminium emballage", "aluminium"],
    confidence: "medium"
  },
  {
    canonical: "bouteille verre",
    aliases: ["bouteille verre", "bouteille en verre", "flacon verre"],
    default_material: "verre",
    default_mass_kg: null,
    official_search_queries: ["bouteille verre", "verre emballage", "verre"],
    confidence: "medium"
  }
];

function resolveCanonicalTerm(input = "", taxonomy = []) {
  const text = normalize(input);
  let bestMatch = null;

  for (const entry of taxonomy) {
    for (const alias of entry.aliases) {
      const aliasText = normalize(alias);
      if (text.includes(aliasText)) {
        if (!bestMatch || aliasText.length > bestMatch.alias.length) {
          bestMatch = {
            canonical: entry.canonical,
            alias: aliasText,
            aliases: entry.aliases
          };
        }
      }
    }
  }

  if (!bestMatch) {
    return {
      canonical: input,
      matched_aliases: [],
      aliases: []
    };
  }

  return {
    canonical: bestMatch.canonical,
    matched_aliases: [bestMatch.alias],
    aliases: bestMatch.aliases
  };
}

function resolveBusinessObject(input = "") {
  const text = normalize(input);
  let best = null;

  for (const entry of BUSINESS_OBJECTS) {
    for (const alias of entry.aliases) {
      const aliasText = normalize(alias);
      if (text.includes(aliasText)) {
        if (!best || aliasText.length > best.alias.length) {
          best = { ...entry, alias: aliasText };
        }
      }
    }
  }

  return best;
}

function simplifyMaterial(input = "") {
  return resolveCanonicalTerm(input, MATERIAL_TAXONOMY).canonical;
}

function simplifyTransportMode(input = "") {
  const resolved = resolveCanonicalTerm(input, TRANSPORT_TAXONOMY).canonical;
  return normalize(resolved) === normalize(input) ? `transport ${input}` : resolved;
}

function buildMaterialQueries(material = "") {
  const resolved = resolveCanonicalTerm(material, MATERIAL_TAXONOMY);
  const canonical = resolved.canonical;
  const original = normalize(material);

  const queries = [
    canonical,
    material,
    `${canonical} kg`,
    `${canonical} fabrication`,
    `${canonical} matière`,
    `${canonical} emballage`
  ];

  if (original.includes("recycl")) {
    queries.push(`${canonical} recyclé`);
    queries.push(`${canonical} recycle`);
  }

  for (const alias of resolved.aliases || []) {
    queries.push(alias);
    queries.push(`${alias} kg`);
  }

  return unique(queries.map((value) => String(value).trim()));
}

function buildTransportQueries(mode = "") {
  const resolved = resolveCanonicalTerm(mode, TRANSPORT_TAXONOMY);
  const canonical =
    normalize(resolved.canonical) === normalize(mode)
      ? simplifyTransportMode(mode)
      : resolved.canonical;

  return unique([
    canonical,
    mode,
    `${canonical} marchandise`,
    `${canonical} marchandises`,
    `${canonical} t.km`,
    "transport marchandises",
    "transport routier marchandises"
  ]);
}

function buildEnergyQueries(energyType = "") {
  const value = String(energyType || "").trim();
  return unique([
    value,
    `${value} kWh`,
    "électricité France",
    "electricite France",
    "mix électrique France"
  ]);
}

function buildObjectQueries(objectName = "") {
  const object = resolveBusinessObject(objectName);
  if (!object) return [objectName];

  const materialQueries = object.default_material ? buildMaterialQueries(object.default_material) : [];
  return unique([
    object.canonical,
    ...object.official_search_queries,
    ...materialQueries
  ]);
}

function isLikelyUnitMatch(unit = "", expectedUnit = "") {
  const u = normalize(unit);
  const expected = normalize(expectedUnit);

  if (!expected) return true;

  if (expected === "kg") {
    return u.includes("kg") || u.includes("kilogramme");
  }

  if (expected === "t.km") {
    return (
      u.includes("t.km") ||
      u.includes("tkm") ||
      u.includes("tonne.km") ||
      u.includes("tonne km") ||
      u.includes("tonnes.km")
    );
  }

  if (expected === "kwh") {
    return u.includes("kwh");
  }

  return u.includes(expected);
}

function scoreTextMatch(searchText = "", query = "") {
  const q = normalize(query);
  if (!q) return 0;

  let score = 0;
  const words = q.split(" ").filter((word) => word.length > 2);

  if (searchText.includes(q)) score += 50;
  for (const word of words) {
    if (searchText.includes(word)) score += 12;
  }

  return score;
}

function detectRowIndex(rows, requiredLabels = []) {
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i].map((cell) => normalize(cell));
    const ok = requiredLabels.every((label) => row.some((cell) => cell.includes(normalize(label))));
    if (ok) return i;
  }
  return -1;
}

function rowsToObjects(rows, headerIndex) {
  const headers = rows[headerIndex].map((value) => String(value || "").trim());
  const objects = [];

  for (let i = headerIndex + 1; i < rows.length; i += 1) {
    const row = rows[i];
    if (!row || row.every((cell) => cell === null || cell === undefined || String(cell).trim() === "")) {
      continue;
    }

    const obj = {};
    headers.forEach((header, index) => {
      if (header) obj[header] = row[index];
    });
    objects.push(obj);
  }

  return objects;
}

function safeReadWorkbook(filePath, options = {}) {
  if (!filePath || !fs.existsSync(filePath)) return null;

  const buffer = fs.readFileSync(filePath);

  return XLSX.read(buffer, {
    type: "buffer",
    raw: false,
    cellDates: false,
    dense: true,
    ...options
  });
}

function loadBaseCarboneRecords(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return [];

  const workbook = XLSX.readFile(filePath, {
    raw: false,
    dense: true,
    FS: ";",
    codepage: 1252
  });

  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rawRows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  if (!rawRows.length) return [];

  const headers = rawRows[0].map((value) => String(value || "").trim());
  const rows = rawRows.slice(1);

  return rows
    .map((row) => {
      const item = {};
      headers.forEach((header, index) => {
        item[header] = row[index];
      });
      return item;
    })
    .map((row) => {
      const factor = firstDefined(
        toNumber(row["CO2f"]),
        toNumber(row["Total poste non décomposé"]),
        toNumber(row["Total poste decompose"]),
        toNumber(row["Valeur"])
      );

      const name = firstDefined(row["Nom base français"], row["Nom attribut français"]);
      const category = firstDefined(row["Code de la catégorie"], row["Type poste"], row["Nom poste français"]);
      const unit = row["Unité français"];
      const tags = row["Tags français"];
      const source = firstDefined(row["Programme"], row["Source"], "Base Carbone");
      const status = row["Statut de l'élément"];

      if (!name || factor === null || !unit) return null;

      return {
        id: row["Identifiant de l'élément"] || `bc-${normalize(name)}-${factor}`,
        dataset: "base_carbone",
        origin: "local_official_dataset",
        source_file: path.basename(filePath),
        name,
        factor,
        unit,
        category: category || "catégorie non précisée",
        source,
        status: status || "",
        search_text: makeSearchText([name, category, tags, unit, source]),
        raw: row
      };
    })
    .filter(Boolean);
}

function loadAgribalyseProductsRecords(filePath) {
  const workbook = safeReadWorkbook(filePath);
  if (!workbook) return [];

  const sheet = workbook.Sheets["Synthese"];
  if (!sheet) return [];

  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  const headerIndex = detectRowIndex(rows, [
    "Nom du Produit en Français",
    "kg CO2 eq/kg de produit"
  ]);

  if (headerIndex < 0) return [];

  return rowsToObjects(rows, headerIndex)
    .map((row) => {
      const factor = toNumber(row["kg CO2 eq/kg de produit"]);
      const name = row["Nom du Produit en Français"];
      const category = firstDefined(row["Groupe d'aliment"], row["Sous-groupe d'aliment"]);
      const lciName = row["LCI Name"];
      const code = firstDefined(row["Code\nAGB"], row["Code\nCIQUAL"]);
      const source = "AGRIBALYSE 3.2 produits alimentaires";

      if (!name || factor === null) return null;

      return {
        id: code || `agb-prod-${normalize(name)}`,
        dataset: "agribalyse_products",
        origin: "local_official_dataset",
        source_file: path.basename(filePath),
        name,
        factor,
        unit: "kgCO2e/kg de produit",
        category: category || "produit alimentaire",
        source,
        status: row["DQR - Note de qualité de la donnée (1 excellente ; 5 très faible)"] || "",
        search_text: makeSearchText([name, category, lciName, row["Livraison"], row["Approche emballage "], source]),
        raw: row
      };
    })
    .filter(Boolean);
}

function loadAgribalyseAgricultureRecords(filePath) {
  const workbook = safeReadWorkbook(filePath);
  if (!workbook) return [];

  const sheet = workbook.Sheets["AGB 3.2 agricole conventionnel"];
  if (!sheet) return [];

  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  const headerIndex = detectRowIndex(rows, [
    "Nom du Produit en Français",
    "kg CO2 eq/kg"
  ]);

  if (headerIndex < 0) return [];

  return rowsToObjects(rows, headerIndex)
    .map((row) => {
      const factor = toNumber(row["kg CO2 eq/kg"]);
      const name = row["Nom du Produit en Français"];
      const category = firstDefined(row["Catégorie"], row["Type de production"]);
      const lciName = row["LCI Name"];
      const source = "AGRIBALYSE 3.2 agriculture conventionnelle";

      if (!name || factor === null) return null;

      return {
        id: `agb-agri-${normalize(name)}`,
        dataset: "agribalyse_agriculture",
        origin: "local_official_dataset",
        source_file: path.basename(filePath),
        name,
        factor,
        unit: "kgCO2e/kg",
        category: category || "production agricole",
        source,
        status: row["Type de production"] || "",
        search_text: makeSearchText([name, category, lciName, source]),
        raw: row
      };
    })
    .filter(Boolean);
}

function chooseLikelyName(row) {
  const entries = Object.entries(row);
  const preferred = [
    "nom",
    "name",
    "libelle",
    "label",
    "produit",
    "poste",
    "categorie",
    "category",
    "designation"
  ];

  for (const key of preferred) {
    const found = entries.find(([k, v]) => normalize(k).includes(key) && v);
    if (found) return String(found[1]);
  }

  const firstText = entries.find(([_, v]) => typeof v === "string" && normalize(v).length > 3);
  return firstText ? String(firstText[1]) : null;
}

function chooseLikelyUnit(row) {
  const entries = Object.entries(row);
  const unitKey = entries.find(([k]) => normalize(k).includes("unite") || normalize(k).includes("unit"));
  if (unitKey && unitKey[1]) return String(unitKey[1]);

  const values = entries.map(([, v]) => normalize(v));
  const candidate = values.find((v) => v.includes("kg co2") || v.includes("t.km") || v.includes("kwh"));
  return candidate || null;
}

function chooseLikelyFactor(row) {
  const entries = Object.entries(row);

  const preferredKeys = [
    "co2",
    "changement climatique",
    "total",
    "impact",
    "valeur"
  ];

  for (const hint of preferredKeys) {
    const found = entries.find(([k, v]) => normalize(k).includes(hint) && toNumber(v) !== null);
    if (found) return toNumber(found[1]);
  }

  for (const [, value] of entries) {
    const n = toNumber(value);
    if (n !== null && Math.abs(n) < 1000000) return n;
  }

  return null;
}

function loadGenericExtraRecords(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const records = [];

  if (ext === ".csv") {
    const workbook = XLSX.readFile(filePath, {
      raw: false,
      dense: true,
      FS: ";",
      codepage: 1252
    });

    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });

    rows.forEach((row, index) => {
      const name = chooseLikelyName(row);
      const factor = chooseLikelyFactor(row);
      const unit = chooseLikelyUnit(row);

      if (!name || factor === null) return;

      records.push({
        id: `extra-${path.basename(filePath)}-${index}`,
        dataset: "extra_local_dataset",
        origin: "local_secondary_dataset",
        source_file: path.basename(filePath),
        name,
        factor,
        unit: unit || "unité non précisée",
        category: "dataset secondaire local",
        source: path.basename(filePath),
        status: "",
        search_text: makeSearchText([name, unit, JSON.stringify(row)]),
        raw: row
      });
    });

    return records;
  }

  if (ext === ".xlsx" || ext === ".xls") {
    const workbook = safeReadWorkbook(filePath);
    if (!workbook) return [];

    workbook.SheetNames.forEach((sheetName) => {
      if (["notice", "readme", "infos", "environnement de calcul"].includes(normalize(sheetName))) return;

      const sheet = workbook.Sheets[sheetName];
      const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });

      rows.forEach((row, index) => {
        const name = chooseLikelyName(row);
        const factor = chooseLikelyFactor(row);
        const unit = chooseLikelyUnit(row);

        if (!name || factor === null) return;

        records.push({
          id: `extra-${path.basename(filePath)}-${sheetName}-${index}`,
          dataset: "extra_local_dataset",
          origin: "local_secondary_dataset",
          source_file: path.basename(filePath),
          name,
          factor,
          unit: unit || "unité non précisée",
          category: sheetName,
          source: path.basename(filePath),
          status: "",
          search_text: makeSearchText([name, sheetName, unit, JSON.stringify(row)]),
          raw: row
        });
      });
    });

    return records;
  }

  return [];
}

function loadLocalDatasets() {
  const baseCarboneFile = resolveFirstExistingFile(DATA_DIR, BASE_CARBONE_CANDIDATES);
  const agribalyseProductsFile = resolveFirstExistingFile(DATA_DIR, AGRIBALYSE_PRODUCTS_CANDIDATES);
  const agribalyseAgriFile = resolveFirstExistingFile(DATA_DIR, AGRIBALYSE_AGRI_CANDIDATES);
  const extraFiles = listExtraDatasetFiles(DATA_DIR);

  const baseCarbone = loadBaseCarboneRecords(baseCarboneFile);
  const agribalyseProducts = loadAgribalyseProductsRecords(agribalyseProductsFile);
  const agribalyseAgriculture = loadAgribalyseAgricultureRecords(agribalyseAgriFile);
  const extraLocal = extraFiles.flatMap((filePath) => loadGenericExtraRecords(filePath));

  return {
    files: {
      baseCarboneFile,
      agribalyseProductsFile,
      agribalyseAgriFile,
      extraFiles
    },
    baseCarbone,
    agribalyseProducts,
    agribalyseAgriculture,
    extraLocal,
    all: [...baseCarbone, ...agribalyseProducts, ...agribalyseAgriculture, ...extraLocal]
  };
}

let LOCAL_DATASETS = loadLocalDatasets();

function searchLocalDatasets(query = "", datasetFilter = null, rows = 100) {
  const pool = datasetFilter
    ? LOCAL_DATASETS.all.filter((item) => item.dataset === datasetFilter)
    : LOCAL_DATASETS.all;

  return pool
    .map((record) => ({
      record,
      score: scoreTextMatch(record.search_text, query)
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, rows)
    .map((item) => item.record);
}

async function fetchAdemeLiveRecords({ query = "", rows = 100, start = 0 }) {
  try {
    const params = {
      dataset: DATASET,
      rows,
      start
    };

    if (query) params.q = query;

    const response = await axios.get(BASE_URL, {
      params,
      timeout: 15000
    });

    const records = response.data?.records || [];
    console.log(`[ADEME_LIVE] query="${query || "(none)"}" start=${start} records=${records.length}`);
    return records;
  } catch (error) {
    console.error("[ADEME_LIVE_ERROR]", {
      query,
      message: error.message,
      status: error.response?.status
    });
    return [];
  }
}

function extractLiveFactor(record) {
  const fields = record.fields || {};

  const factor = firstDefined(
    toNumber(fields.co2f),
    toNumber(fields.CO2f),
    toNumber(fields.emission_de_co2),
    toNumber(fields.emission_ges),
    toNumber(fields.facteur_d_emission),
    toNumber(fields.facteur_emission),
    toNumber(fields.valeur_facteur),
    toNumber(fields.valeur),
    toNumber(fields.total_poste_non_decompose),
    toNumber(fields.total_poste_decompose),
    toNumber(fields.total),
    toNumber(fields.impact)
  );

  const name = firstDefined(
    fields.nom_base_carbone,
    fields.nom,
    fields.libelle,
    fields.poste,
    fields.nom_attribut_fr,
    fields.nom_francais,
    fields.name,
    fields.titre
  );

  const unit = firstDefined(
    fields.unite_fr,
    fields.unite,
    fields.unite_facteur,
    fields.unite_declaree,
    fields.unit
  );

  const category = firstDefined(fields.categorie, fields.type_ligne, fields.type_de_facteur);

  if (!name || factor === null || !unit) return null;

  return {
    id: record.recordid || `live-${normalize(name)}-${factor}`,
    dataset: "ademe_live",
    origin: "ademe_live",
    source_file: "ADEME live",
    name,
    factor,
    unit,
    category: category || "catégorie non précisée",
    source: "ADEME Base Carbone live",
    status: "",
    search_text: makeSearchText([name, category, unit]),
    raw: fields
  };
}

function datasetPreferenceFromContext(context = {}) {
  const domain = normalize(context.domain || "");
  const canonical = normalize(context.canonical || "");
  const original = normalize(context.original || "");

  if (domain === "energy") return ["base_carbone", "ademe_live", "extra_local_dataset"];
  if (domain === "transport") return ["base_carbone", "ademe_live", "extra_local_dataset"];
  if (domain === "material") return ["base_carbone", "ademe_live", "extra_local_dataset"];
  if (domain === "object") return ["base_carbone", "ademe_live", "extra_local_dataset"];

  const foodHints = ["aliment", "boisson", "yaourt", "lait", "viande", "fromage", "pomme", "banane", "salade"];
  const agriHints = ["ble", "mais", "vigne", "lait cru", "porc", "boeuf", "oeuf", "poulet", "quinoa"];

  if (foodHints.some((hint) => original.includes(hint) || canonical.includes(hint))) {
    return ["agribalyse_products", "base_carbone", "ademe_live", "extra_local_dataset"];
  }

  if (agriHints.some((hint) => original.includes(hint) || canonical.includes(hint))) {
    return ["agribalyse_agriculture", "agribalyse_products", "base_carbone", "ademe_live", "extra_local_dataset"];
  }

  return ["base_carbone", "agribalyse_products", "agribalyse_agriculture", "ademe_live", "extra_local_dataset"];
}

function scoreFactor(record, expectedUnit = "", context = {}) {
  let score = 0;
  const comments = [];
  const name = normalize(record.name || "");
  const category = normalize(record.category || "");
  const unit = normalize(record.unit || "");
  const canonical = normalize(context.canonical || "");
  const query = normalize(context.query || "");
  const domain = normalize(context.domain || "");

  if (record.factor > 0) score += 25;
  if (record.unit) score += 15;
  if (expectedUnit && isLikelyUnitMatch(unit, expectedUnit)) {
    score += 25;
  } else if (expectedUnit) {
    score -= 10;
    comments.push("Unité peu alignée.");
  }

  if (canonical && name.includes(canonical)) score += 22;
  if (canonical && category.includes(canonical)) score += 10;
  if (query && name.includes(query)) score += 8;

  if (record.dataset === "base_carbone") score += 12;
  if (record.dataset === "agribalyse_products") score += 10;
  if (record.dataset === "agribalyse_agriculture") score += 10;
  if (record.dataset === "ademe_live") score += 6;
  if (record.dataset === "extra_local_dataset") score += 4;

  if (domain === "transport" && (name.includes("marchandise") || category.includes("transport"))) score += 10;
  if (domain === "energy" && (name.includes("electric") || name.includes("électric"))) score += 10;
  if (domain === "object" && (name.includes("palette") || category.includes("emballage") || category.includes("bois"))) score += 10;
  if (domain === "material" && (name.includes("emballage") || category.includes("emballage"))) score += 6;

  if (name.includes("moyenne") || name.includes("generique")) {
    score -= 5;
    comments.push("Libellé générique.");
  }

  let matchQuality = "close";
  if (canonical && name.includes(canonical) && (!expectedUnit || isLikelyUnitMatch(unit, expectedUnit))) {
    matchQuality = "exact";
  }

  if (record.dataset === "extra_local_dataset" && matchQuality === "exact") {
    matchQuality = "close";
  }

  let grade = "E";
  if (score >= 90) grade = "A";
  else if (score >= 75) grade = "B";
  else if (score >= 60) grade = "C";
  else if (score >= 45) grade = "D";

  return { score, grade, comments, match_quality: matchQuality };
}

function buildSelectionReason(selected, expectedUnit = "") {
  const reasons = [];
  if (selected?.quality?.grade) reasons.push(`grade ${selected.quality.grade}`);
  if (selected?.dataset) reasons.push(`dataset ${selected.dataset}`);
  if (selected?.quality?.match_quality) reasons.push(`match ${selected.quality.match_quality}`);
  if (selected?.source_file) reasons.push(`fichier ${selected.source_file}`);
  if (expectedUnit && isLikelyUnitMatch(selected.unit || "", expectedUnit)) reasons.push(`unité alignée ${expectedUnit}`);
  if (selected?.name) reasons.push(`libellé retenu: ${selected.name}`);
  return reasons.join(" | ");
}

async function findBestFactor(queries, expectedUnit = "", context = {}) {
  const datasetOrder = datasetPreferenceFromContext(context);
  const queriesTested = [];
  const allCandidates = [];

  for (const query of queries) {
    queriesTested.push(query);

    for (const dataset of datasetOrder) {
      if (dataset === "ademe_live") {
        const liveRecords = await fetchAdemeLiveRecords({ query, rows: 50 });
        const parsed = liveRecords
          .map(extractLiveFactor)
          .filter(Boolean)
          .map((item) => ({
            ...item,
            matched_query: query,
            quality: scoreFactor(item, expectedUnit, { ...context, query })
          }));
        allCandidates.push(...parsed);
      } else {
        const localRecords = searchLocalDatasets(query, dataset, 50).map((item) => ({
          ...item,
          matched_query: query,
          quality: scoreFactor(item, expectedUnit, { ...context, query })
        }));
        allCandidates.push(...localRecords);
      }
    }
  }

  const dedupedMap = new Map();

  for (const candidate of allCandidates) {
    const key = candidate.id || `${candidate.name}|${candidate.unit}|${candidate.factor}`;
    const existing = dedupedMap.get(key);

    if (!existing) {
      dedupedMap.set(key, {
        ...candidate,
        matched_queries: [candidate.matched_query],
        query_hits: 1
      });
      continue;
    }

    const merged = {
      ...existing,
      matched_queries: unique([...existing.matched_queries, candidate.matched_query]),
      query_hits: existing.query_hits + 1
    };

    if (candidate.quality.score > existing.quality.score) {
      Object.assign(merged, candidate);
    }

    dedupedMap.set(key, merged);
  }

  const ranked = [...dedupedMap.values()]
    .map((candidate) => ({
      ...candidate,
      quality: {
        ...candidate.quality,
        score: candidate.quality.score + Math.min(candidate.query_hits * 4, 12)
      }
    }))
    .sort((a, b) => b.quality.score - a.quality.score);

  const selected = ranked[0] || null;
  const second = ranked[1] || null;
  const ambiguous = Boolean(selected && second && Math.abs(selected.quality.score - second.quality.score) < 8);

  return {
    status: selected
      ? ambiguous
        ? "OFFICIAL_FACTOR_AMBIGUOUS"
        : "OFFICIAL_FACTOR_FOUND"
      : "OFFICIAL_FACTOR_NOT_FOUND",
    query_used: selected?.matched_query || null,
    queries_tested: queriesTested,
    normalized_input: {
      original: context.original || "",
      canonical: context.canonical || "",
      matched_aliases: context.matched_aliases || []
    },
    selected,
    candidates: ranked.slice(0, 5),
    selection_reason: selected ? buildSelectionReason(selected, expectedUnit) : null,
    ambiguity_level: selected ? (ambiguous ? "medium" : "low") : "high",
    recommended_followup_question: ambiguous ? context.followupQuestion || null : null
  };
}

function provisionalMaterialFactor(material) {
  const m = normalize(material);

  if (m.includes("verre")) {
    return {
      dataset: "provisional",
      origin: "provisional",
      source_file: "none",
      name: "Estimation provisoire verre",
      factor: 0.85,
      unit: "kgCO2e/kg",
      category: "approximation",
      source: "Estimation provisoire non confirmée",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."], match_quality: "provisional" }
    };
  }

  if (m.includes("aluminium") || m.includes("alu")) {
    return {
      dataset: "provisional",
      origin: "provisional",
      source_file: "none",
      name: "Estimation provisoire aluminium",
      factor: 8.6,
      unit: "kgCO2e/kg",
      category: "approximation",
      source: "Estimation provisoire non confirmée",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."], match_quality: "provisional" }
    };
  }

  if (m.includes("bois") || m.includes("palette")) {
    return {
      dataset: "provisional",
      origin: "provisional",
      source_file: "none",
      name: "Estimation provisoire bois",
      factor: 0.12,
      unit: "kgCO2e/kg",
      category: "approximation",
      source: "Estimation provisoire non confirmée",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."], match_quality: "provisional" }
    };
  }

  return null;
}

function provisionalTransportFactor(mode) {
  const m = normalize(mode);

  if (m.includes("camion") || m.includes("routier")) {
    return {
      dataset: "provisional",
      origin: "provisional",
      source_file: "none",
      name: "Estimation provisoire transport routier",
      factor: 0.12,
      unit: "kgCO2e/t.km",
      category: "approximation",
      source: "Estimation provisoire non confirmée",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."], match_quality: "provisional" }
    };
  }

  return null;
}

function confidenceFromFactor(selected) {
  if (!selected?.quality?.grade) return "Faible";
  if (selected.quality.match_quality === "provisional") return "Faible";
  if (selected.quality.grade === "A" || selected.quality.grade === "B") return "Élevé";
  if (selected.quality.grade === "C") return "Moyen";
  return "Faible";
}

function buildSourceQualification(selected) {
  if (!selected) return "source non trouvée";
  if (selected.quality?.match_quality === "exact") return "facteur officiel exact retenu";
  if (selected.quality?.match_quality === "close") return "meilleure correspondance officielle retenue";
  return "estimation provisoire non confirmée";
}

function mcpText(payload) {
  return {
    content: [{ type: "text", text: JSON.stringify(payload, null, 2) }]
  };
}

server.tool(
  "estimate_material_emission",
  "Estime l’empreinte carbone d’une matière",
  {
    material: z.string(),
    quantity: z.number(),
    unit: z.string().default("kg"),
    official_only: z.boolean().default(true)
  },
  async ({ material, quantity, unit = "kg", official_only = true }) => {
    const resolved = resolveCanonicalTerm(material, MATERIAL_TAXONOMY);

    const found = await findBestFactor(buildMaterialQueries(material), "kg", {
      domain: "material",
      original: material,
      canonical: resolved.canonical,
      matched_aliases: resolved.matched_aliases,
      followupQuestion: "Peux-tu préciser la matière exacte pour renforcer le match officiel ?"
    });

    let selected = found.selected;
    let provisionalUsed = false;

    if (!selected && !official_only) {
      selected = provisionalMaterialFactor(material);
      provisionalUsed = Boolean(selected);
    }

    if (!selected) {
      return mcpText({
        status: "OFFICIAL_FACTOR_NOT_FOUND",
        material,
        quantity,
        unit,
        normalized_input: found.normalized_input,
        queries_tested: found.queries_tested,
        candidates_reviewed: found.candidates,
        recommended_followup_question: found.recommended_followup_question
      });
    }

    return mcpText({
      status: "OK",
      factor_status: provisionalUsed ? "PROVISIONAL_ESTIMATE" : found.status,
      match_quality: selected.quality?.match_quality || "close",
      source_qualification: buildSourceQualification(selected),
      official_only,
      provisional_used: provisionalUsed,
      post: "matière",
      material,
      simplified_material: simplifyMaterial(material),
      normalized_input: found.normalized_input,
      quantity,
      quantity_unit: unit,
      selected_factor: selected,
      calculation: {
        formula: "quantity × factor",
        result: quantity * selected.factor,
        unit: "kgCO2e"
      },
      confidence: confidenceFromFactor(selected),
      queries_tested: found.queries_tested,
      selection_reason: found.selection_reason,
      ambiguity_level: found.ambiguity_level,
      recommended_followup_question: found.recommended_followup_question,
      candidates_reviewed: found.candidates
    });
  }
);

server.tool(
  "estimate_transport_emission",
  "Estime l’empreinte carbone d’un transport",
  {
    mode: z.string(),
    mass_kg: z.number(),
    distance_km: z.number(),
    official_only: z.boolean().default(true)
  },
  async ({ mode, mass_kg, distance_km, official_only = true }) => {
    const resolved = resolveCanonicalTerm(mode, TRANSPORT_TAXONOMY);
    const tonne_km = (mass_kg / 1000) * distance_km;

    const found = await findBestFactor(buildTransportQueries(mode), "t.km", {
      domain: "transport",
      original: mode,
      canonical: resolved.canonical,
      matched_aliases: resolved.matched_aliases,
      followupQuestion: "Peux-tu confirmer le mode de transport exact ?"
    });

    let selected = found.selected;
    let provisionalUsed = false;

    if (!selected && !official_only) {
      selected = provisionalTransportFactor(mode);
      provisionalUsed = Boolean(selected);
    }

    if (!selected) {
      return mcpText({
        status: "OFFICIAL_FACTOR_NOT_FOUND",
        mode,
        mass_kg,
        distance_km,
        tonne_km,
        normalized_input: found.normalized_input,
        queries_tested: found.queries_tested,
        candidates_reviewed: found.candidates,
        recommended_followup_question: found.recommended_followup_question
      });
    }

    return mcpText({
      status: "OK",
      factor_status: provisionalUsed ? "PROVISIONAL_ESTIMATE" : found.status,
      match_quality: selected.quality?.match_quality || "close",
      source_qualification: buildSourceQualification(selected),
      official_only,
      provisional_used: provisionalUsed,
      post: "transport",
      mode,
      clean_mode: simplifyTransportMode(mode),
      normalized_input: found.normalized_input,
      mass_kg,
      distance_km,
      tonne_km,
      selected_factor: selected,
      calculation: {
        formula: "tonne.km × factor",
        result: tonne_km * selected.factor,
        unit: "kgCO2e"
      },
      confidence: confidenceFromFactor(selected),
      queries_tested: found.queries_tested,
      selection_reason: found.selection_reason,
      ambiguity_level: found.ambiguity_level,
      recommended_followup_question: found.recommended_followup_question,
      candidates_reviewed: found.candidates
    });
  }
);

server.tool(
  "estimate_standard_object_emission",
  "Estime l’empreinte d’un objet standard comme palette Europe",
  {
    object_name: z.string(),
    quantity: z.number().default(1),
    mass_kg_override: z.number().optional(),
    official_only: z.boolean().default(true)
  },
  async ({ object_name, quantity = 1, mass_kg_override, official_only = true }) => {
    const object = resolveBusinessObject(object_name);

    if (!object) {
      return mcpText({
        status: "OBJECT_NOT_RECOGNIZED",
        object_name,
        message: "Objet métier non reconnu."
      });
    }

    const unitMassKg = mass_kg_override ?? object.default_mass_kg;

    if (!unitMassKg) {
      return mcpText({
        status: "MASS_REQUIRED",
        object_name,
        recognized_object: object.canonical,
        default_material: object.default_material,
        message: "Objet reconnu mais masse manquante. Fournis mass_kg_override."
      });
    }

    const found = await findBestFactor(buildObjectQueries(object_name), "kg", {
      domain: "object",
      original: object_name,
      canonical: object.canonical,
      matched_aliases: [object.alias],
      followupQuestion: "Peux-tu confirmer l’objet exact si tu veux un match officiel plus fin ?"
    });

    let selected = found.selected;
    let provisionalUsed = false;

    if (!selected && !official_only) {
      selected = provisionalMaterialFactor(object.default_material);
      provisionalUsed = Boolean(selected);
    }

    if (!selected) {
      return mcpText({
        status: "OFFICIAL_FACTOR_NOT_FOUND",
        object_name,
        recognized_object: object.canonical,
        default_material: object.default_material,
        default_mass_kg: object.default_mass_kg,
        normalized_input: found.normalized_input,
        queries_tested: found.queries_tested,
        candidates_reviewed: found.candidates,
        recommended_followup_question: found.recommended_followup_question
      });
    }

    const totalMassKg = unitMassKg * quantity;

    return mcpText({
      status: "OK",
      factor_status: provisionalUsed ? "PROVISIONAL_ESTIMATE" : found.status,
      match_quality: selected.quality?.match_quality || "close",
      source_qualification: buildSourceQualification(selected),
      official_only,
      provisional_used: provisionalUsed,
      object_name,
      recognized_object: object.canonical,
      default_material: object.default_material,
      quantity,
      unit_mass_kg_used: unitMassKg,
      total_mass_kg: totalMassKg,
      selected_factor: selected,
      calculation: {
        formula: "(quantity × unit_mass_kg) × factor",
        result: totalMassKg * selected.factor,
        unit: "kgCO2e"
      },
      assumptions: [
        `objet interprété comme ${object.canonical}`,
        `matière probable ${object.default_material}`,
        `masse unitaire utilisée ${unitMassKg} kg`
      ],
      confidence: confidenceFromFactor(selected),
      queries_tested: found.queries_tested,
      selection_reason: found.selection_reason,
      ambiguity_level: found.ambiguity_level,
      recommended_followup_question: found.recommended_followup_question,
      candidates_reviewed: found.candidates
    });
  }
);

server.tool(
  "estimate_energy_emission",
  "Estime l’empreinte d’une consommation énergétique",
  {
    energy_type: z.string(),
    consumption_kwh: z.number(),
    official_only: z.boolean().default(true)
  },
  async ({ energy_type, consumption_kwh, official_only = true }) => {
    const found = await findBestFactor(buildEnergyQueries(energy_type), "kwh", {
      domain: "energy",
      original: energy_type,
      canonical: energy_type,
      matched_aliases: [],
      followupQuestion: "Peux-tu préciser le type d’énergie ou le pays ?"
    });

    if (!found.selected) {
      return mcpText({
        status: "OFFICIAL_FACTOR_NOT_FOUND",
        energy_type,
        consumption_kwh,
        queries_tested: found.queries_tested,
        candidates_reviewed: found.candidates
      });
    }

    return mcpText({
      status: "OK",
      factor_status: found.status,
      match_quality: found.selected.quality?.match_quality || "close",
      source_qualification: buildSourceQualification(found.selected),
      official_only,
      post: "énergie",
      energy_type,
      selected_factor: found.selected,
      calculation: {
        formula: "kWh × factor",
        result: consumption_kwh * found.selected.factor,
        unit: "kgCO2e"
      },
      confidence: confidenceFromFactor(found.selected),
      queries_tested: found.queries_tested,
      selection_reason: found.selection_reason,
      ambiguity_level: found.ambiguity_level,
      candidates_reviewed: found.candidates
    });
  }
);

server.tool(
  "estimate_food_emission",
  "Estime l’empreinte d’un produit alimentaire via Agribalyse",
  {
    product_name: z.string(),
    quantity_kg: z.number()
  },
  async ({ product_name, quantity_kg }) => {
    const found = await findBestFactor([product_name], "kg", {
      domain: "food",
      original: product_name,
      canonical: product_name,
      matched_aliases: [],
      followupQuestion: "Peux-tu préciser le produit alimentaire exact ?"
    });

    const selected = found.selected?.dataset === "agribalyse_products" || found.selected?.dataset === "agribalyse_agriculture"
      ? found.selected
      : found.candidates.find((item) => item.dataset === "agribalyse_products" || item.dataset === "agribalyse_agriculture") || null;

    if (!selected) {
      return mcpText({
        status: "OFFICIAL_FACTOR_NOT_FOUND",
        product_name,
        quantity_kg,
        queries_tested: found.queries_tested,
        candidates_reviewed: found.candidates
      });
    }

    return mcpText({
      status: "OK",
      factor_status: "OFFICIAL_FACTOR_FOUND",
      match_quality: selected.quality?.match_quality || "close",
      source_qualification: buildSourceQualification(selected),
      post: "alimentaire",
      product_name,
      quantity_kg,
      selected_factor: selected,
      calculation: {
        formula: "quantity_kg × factor",
        result: quantity_kg * selected.factor,
        unit: "kgCO2e"
      },
      confidence: confidenceFromFactor(selected),
      queries_tested: found.queries_tested,
      selection_reason: buildSelectionReason(selected, "kg"),
      candidates_reviewed: found.candidates
    });
  }
);

server.tool(
  "search_factor",
  "Recherche brute dans les bases locales et ADEME live",
  {
    query: z.string(),
    rows: z.number().optional()
  },
  async ({ query, rows = 10 }) => {
    const localResults = searchLocalDatasets(query, null, rows);
    const liveResults = (await fetchAdemeLiveRecords({ query, rows }))
      .map(extractLiveFactor)
      .filter(Boolean);

    return mcpText({
      query,
      local_count: localResults.length,
      live_count: liveResults.length,
      local_results: localResults.slice(0, rows),
      live_results: liveResults.slice(0, rows)
    });
  }
);

server.tool(
  "get_factor",
  "Récupère le meilleur facteur selon une requête",
  {
    query: z.string(),
    expected_unit_keyword: z.string().optional(),
    official_only: z.boolean().default(true)
  },
  async ({ query, expected_unit_keyword = "", official_only = true }) => {
    const found = await findBestFactor([query], expected_unit_keyword, {
      domain: "generic",
      original: query,
      canonical: query,
      matched_aliases: [],
      followupQuestion: "Peux-tu préciser la matière, le transport, l’énergie, l’objet ou l’aliment visé ?"
    });

    let selected = found.selected;
    let provisionalUsed = false;

    if (!selected && !official_only) {
      selected = provisionalMaterialFactor(query) || provisionalTransportFactor(query);
      provisionalUsed = Boolean(selected);
    }

    return mcpText({
      status: selected ? "OK" : "OFFICIAL_FACTOR_NOT_FOUND",
      factor_status: selected ? (provisionalUsed ? "PROVISIONAL_ESTIMATE" : found.status) : found.status,
      match_quality: selected?.quality?.match_quality || (selected ? "close" : null),
      source_qualification: buildSourceQualification(selected),
      official_only,
      provisional_used: provisionalUsed,
      query,
      normalized_input: found.normalized_input,
      selected_factor: selected,
      selection_reason: found.selection_reason,
      ambiguity_level: found.ambiguity_level,
      recommended_followup_question: found.recommended_followup_question,
      candidates: found.candidates,
      queries_tested: found.queries_tested
    });
  }
);

server.tool(
  "calculate",
  "Calcule une émission carbone",
  {
    activity: z.number(),
    factor: z.number()
  },
  async ({ activity, factor }) => {
    return mcpText({
      formula: "activity × factor",
      activity,
      factor,
      result: activity * factor,
      unit: "kgCO2e"
    });
  }
);

app.get("/", (req, res) => {
  res.send("MCP Carbone Hermes OK — v5.1.0");
});

app.get("/health", async (req, res) => {
  const liveTest = await fetchAdemeLiveRecords({ query: "verre", rows: 3 });

  res.json({
    status: "OK",
    service: "mcp-carbone-hermes",
    version: "5.1.0",
    dataset: DATASET,
    files: {
      base_carbone_file: LOCAL_DATASETS.files.baseCarboneFile ? path.basename(LOCAL_DATASETS.files.baseCarboneFile) : null,
      agribalyse_products_file: LOCAL_DATASETS.files.agribalyseProductsFile ? path.basename(LOCAL_DATASETS.files.agribalyseProductsFile) : null,
      agribalyse_agri_file: LOCAL_DATASETS.files.agribalyseAgriFile ? path.basename(LOCAL_DATASETS.files.agribalyseAgriFile) : null,
      extra_files: LOCAL_DATASETS.files.extraFiles.map((f) => path.basename(f))
    },
    local_datasets: {
      base_carbone: LOCAL_DATASETS.baseCarbone.length,
      agribalyse_products: LOCAL_DATASETS.agribalyseProducts.length,
      agribalyse_agriculture: LOCAL_DATASETS.agribalyseAgriculture.length,
      extra_local: LOCAL_DATASETS.extraLocal.length,
      total: LOCAL_DATASETS.all.length
    },
    ademe_live: {
      status: liveTest.length > 0 ? "OK" : "CHECK_NEEDED",
      test_query: "verre",
      result_count: liveTest.length
    }
  });
});

app.get("/reload-data", (req, res) => {
  LOCAL_DATASETS = loadLocalDatasets();
  res.json({
    status: "OK",
    files: LOCAL_DATASETS.files,
    local_datasets: {
      base_carbone: LOCAL_DATASETS.baseCarbone.length,
      agribalyse_products: LOCAL_DATASETS.agribalyseProducts.length,
      agribalyse_agriculture: LOCAL_DATASETS.agribalyseAgriculture.length,
      extra_local: LOCAL_DATASETS.extraLocal.length,
      total: LOCAL_DATASETS.all.length
    }
  });
});

app.get("/debug/ademe/:query", async (req, res) => {
  const query = req.params.query;
  const localResults = searchLocalDatasets(query, null, 10);
  const liveResults = (await fetchAdemeLiveRecords({ query, rows: 10 }))
    .map(extractLiveFactor)
    .filter(Boolean);

  res.json({
    query,
    local_count: localResults.length,
    live_count: liveResults.length,
    local_results: localResults,
    live_results: liveResults
  });
});

app.post("/mcp", async (req, res) => {
  try {
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });

    res.on("close", () => {
      transport.close();
    });

    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error("[MCP_ERROR]", error);
    if (!res.headersSent) {
      res.status(500).json({ error: "MCP_ERROR", message: error.message });
    }
  }
});

app.listen(PORT, () => {
  console.log(`MCP Carbone Hermes v5.1.0 running on port ${PORT}`);
  console.log("Loaded files:", {
    baseCarboneFile: LOCAL_DATASETS.files.baseCarboneFile,
    agribalyseProductsFile: LOCAL_DATASETS.files.agribalyseProductsFile,
    agribalyseAgriFile: LOCAL_DATASETS.files.agribalyseAgriFile,
    extraFiles: LOCAL_DATASETS.files.extraFiles
  });
  console.log("Dataset counts:", {
    base_carbone: LOCAL_DATASETS.baseCarbone.length,
    agribalyse_products: LOCAL_DATASETS.agribalyseProducts.length,
    agribalyse_agriculture: LOCAL_DATASETS.agribalyseAgriculture.length,
    extra_local: LOCAL_DATASETS.extraLocal.length,
    total: LOCAL_DATASETS.all.length
  });
});
