import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import express from "express";
import axios from "axios";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = Number(process.env.PORT || 3000);
const DATASET = process.env.ADEME_DATASET || "base-carboner";
const BASE_URL = process.env.ADEME_BASE_URL || "https://data.ademe.fr/api/records/1.0/search/";
const LOCAL_SNAPSHOT_PATH = path.join(__dirname, "data", "ademe_snapshot.json");

const server = new McpServer({
  name: "mcp-carbone-hermes",
  version: "4.0.0"
});

function normalize(text = "") {
  return String(text)
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^a-z0-9\s.,/+-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;

  const cleaned = String(value)
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

function stringifyFields(fields = {}) {
  return normalize(Object.values(fields).join(" "));
}

function loadLocalSnapshot() {
  try {
    if (!fs.existsSync(LOCAL_SNAPSHOT_PATH)) return [];
    const raw = fs.readFileSync(LOCAL_SNAPSHOT_PATH, "utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    console.error("[LOCAL_SNAPSHOT_ERROR]", error.message);
    return [];
  }
}

let LOCAL_SNAPSHOT = loadLocalSnapshot();

const MATERIAL_TAXONOMY = [
  { canonical: "verre", aliases: ["verre", "bouteille verre", "flacon verre", "emballage verre"] },
  { canonical: "aluminium", aliases: ["aluminium", "alu", "canette", "canette alu", "boite alu"] },
  { canonical: "acier", aliases: ["acier", "inox", "galvanise", "acier galvanise", "metal acier"] },
  { canonical: "carton", aliases: ["carton", "papier", "papier carton", "carton d emballage", "emballage carton"] },
  { canonical: "pet", aliases: ["pet", "plastique pet", "bouteille pet", "flacon pet", "emballage pet"] },
  { canonical: "pp", aliases: ["pp", "polypropylene", "plastique pp", "barquette pp"] },
  { canonical: "plastique", aliases: ["plastique", "emballage plastique", "film plastique", "barquette plastique"] },
  { canonical: "bois", aliases: ["bois", "palette bois", "support bois"] },
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
    aliases: ["palette europe", "euro palette", "palette eur", "palette epal", "palette europe epal"],
    default_material: "bois",
    default_mass_kg: 25,
    official_search_queries: ["palette bois", "bois palette", "emballage bois", "palette", "support bois"],
    confidence: "medium"
  },
  {
    canonical: "palette",
    aliases: ["palette", "palette bois"],
    default_material: "bois",
    default_mass_kg: 25,
    official_search_queries: ["palette bois", "emballage bois", "palette", "support bois"],
    confidence: "medium"
  },
  {
    canonical: "canette",
    aliases: ["canette", "canette alu", "boisson canette"],
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
  },
  {
    canonical: "carton d emballage",
    aliases: ["carton d emballage", "carton emballage", "caisse carton"],
    default_material: "carton",
    default_mass_kg: null,
    official_search_queries: ["carton emballage", "papier carton", "carton"],
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
  const original = normalize(material);
  const canonical = resolved.canonical;
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

  return [...new Set(queries.map((q) => String(q).trim()).filter(Boolean))];
}

function buildTransportQueries(mode = "") {
  const resolved = resolveCanonicalTerm(mode, TRANSPORT_TAXONOMY);
  const canonical =
    normalize(resolved.canonical) === normalize(mode)
      ? simplifyTransportMode(mode)
      : resolved.canonical;

  return [
    canonical,
    mode,
    `${canonical} marchandise`,
    `${canonical} marchandises`,
    `${canonical} t.km`,
    "transport marchandises",
    "transport routier marchandises"
  ].filter((value, index, arr) => value && arr.indexOf(value) === index);
}

function buildEnergyQueries(energyType = "") {
  const value = String(energyType || "").trim();

  return [
    value,
    `${value} kWh`,
    "électricité France",
    "electricite France",
    "mix électrique France"
  ].filter((item, index, arr) => item && arr.indexOf(item) === index);
}

function buildObjectQueries(objectName = "") {
  const object = resolveBusinessObject(objectName);
  if (!object) return [objectName];

  const materialQueries = object.default_material ? buildMaterialQueries(object.default_material) : [];
  return [
    object.canonical,
    ...object.official_search_queries,
    ...materialQueries
  ].filter((value, index, arr) => value && arr.indexOf(value) === index);
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

function toFieldBag(record = {}) {
  if (record.fields && typeof record.fields === "object") {
    return record.fields;
  }

  return {
    nom_base_carbone: record.name || record.nom || record.label || "",
    unite_fr: record.unit || record.unite || "",
    categorie: record.category || record.categorie || "",
    co2f: record.factor ?? record.valeur ?? null,
    keywords: Array.isArray(record.keywords) ? record.keywords.join(" ") : record.keywords || "",
    description: record.description || "",
    source_name: record.source || ""
  };
}

function extractFactor(record, origin = "unknown") {
  const fields = toFieldBag(record);

  const factorCandidates = [
    fields.co2f,
    fields.CO2f,
    fields.emission_de_co2,
    fields.emission_ges,
    fields.facteur_d_emission,
    fields.facteur_emission,
    fields.valeur_facteur,
    fields.valeur,
    fields.total_poste_non_decompose,
    fields.total_poste_decompose,
    fields.total,
    fields.impact,
    record.factor
  ];

  let factor = factorCandidates.map(toNumber).find((v) => Number.isFinite(v));

  if (!Number.isFinite(factor)) {
    for (const [key, value] of Object.entries(fields)) {
      const n = toNumber(value);
      const k = normalize(key);
      if (
        n !== null &&
        Number.isFinite(n) &&
        (k.includes("co2") || k.includes("ges") || k.includes("emission") || k.includes("facteur") || k.includes("total"))
      ) {
        factor = n;
        break;
      }
    }
  }

  const name = firstDefined(
    fields.nom_base_carbone,
    fields.nom,
    fields.libelle,
    fields.poste,
    fields.nom_attribut_fr,
    fields.nom_francais,
    fields.name,
    fields.titre,
    record.name
  );

  const unit = firstDefined(
    fields.unite_fr,
    fields.unite,
    fields.unite_facteur,
    fields.unite_declaree,
    fields.unit,
    record.unit
  );

  return {
    id: record.recordid || record.id || `${name || "unknown"}|${unit || ""}|${factor || ""}`,
    name: name || "Facteur ADEME non nommé",
    factor,
    unit: unit || "unité non précisée",
    category: firstDefined(fields.categorie, fields.type_ligne, fields.type_de_facteur, record.category) || "catégorie non précisée",
    source: record.source || "ADEME Base Carbone",
    origin,
    raw_keys: Object.keys(fields),
    raw: fields
  };
}

function scoreLocalSnapshotMatch(record, query = "") {
  const q = normalize(query);
  const fields = toFieldBag(record);
  const haystack = stringifyFields(fields);

  if (!q) return 0;

  let score = 0;
  const words = q.split(" ").filter((word) => word.length > 2);

  if (haystack.includes(q)) score += 50;

  for (const word of words) {
    if (haystack.includes(word)) score += 12;
  }

  return score;
}

function searchLocalSnapshot(query = "", rows = 100) {
  if (!LOCAL_SNAPSHOT.length) return [];

  return LOCAL_SNAPSHOT
    .map((record) => ({
      record,
      score: scoreLocalSnapshotMatch(record, query)
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, rows)
    .map((item) => item.record);
}

function scoreFactor(item, expectedUnit = "", context = {}) {
  let score = 0;
  const comments = [];
  const unit = normalize(item.unit || "");
  const name = normalize(item.name || "");
  const category = normalize(item.category || "");
  const canonical = normalize(context.canonical || "");
  const domain = normalize(context.domain || "");
  const query = normalize(context.query || "");

  if (item.factor > 0) score += 25;
  else comments.push("Facteur nul ou négatif exclu.");

  if (item.unit && item.unit !== "unité non précisée") score += 15;
  else comments.push("Unité non précisée.");

  if (expectedUnit && isLikelyUnitMatch(unit, expectedUnit)) {
    score += 25;
  } else if (expectedUnit) {
    score -= 10;
    comments.push("Unité potentiellement non alignée.");
  }

  if (item.name && item.name !== "Facteur ADEME non nommé") score += 10;
  if (item.origin === "local_snapshot") score += 8;
  if (item.origin === "ademe_live") score += 6;

  if (canonical && name.includes(canonical)) score += 20;
  if (canonical && category.includes(canonical)) score += 10;
  if (query && name.includes(query)) score += 10;

  if (domain === "transport" && (name.includes("marchandise") || category.includes("transport"))) score += 10;
  if (domain === "material" && (name.includes("emballage") || category.includes("mati"))) score += 5;
  if (domain === "energy" && (name.includes("electric") || name.includes("électric"))) score += 8;
  if (domain === "object" && (name.includes("palette") || category.includes("emballage"))) score += 10;

  if (name.includes("moyenne") || name.includes("generique")) {
    score -= 5;
    comments.push("Libellé potentiellement générique.");
  }

  let grade = "E";
  if (score >= 90) grade = "A";
  else if (score >= 75) grade = "B";
  else if (score >= 60) grade = "C";
  else if (score >= 45) grade = "D";

  return { score, grade, comments };
}

function buildSelectionReason(selected, expectedUnit = "") {
  const reasons = [];
  if (selected?.quality?.grade) reasons.push(`grade ${selected.quality.grade}`);
  if (selected?.origin) reasons.push(`source ${selected.origin}`);
  if (expectedUnit && isLikelyUnitMatch(selected.unit || "", expectedUnit)) reasons.push(`unité alignée ${expectedUnit}`);
  if (selected?.name) reasons.push(`libellé retenu: ${selected.name}`);
  return reasons.join(" | ");
}

async function findBestFactor(queries, expectedUnit = "", context = {}) {
  const queriesTested = [];
  const allCandidates = [];

  for (const query of queries) {
    queriesTested.push(query);

    const localRecords = searchLocalSnapshot(query, 100);
    const liveRecords = await fetchAdemeLiveRecords({ query, rows: 100 });

    const localCandidates = localRecords
      .map((record) => extractFactor(record, "local_snapshot"))
      .filter((item) => Number.isFinite(item.factor) && item.factor > 0)
      .map((item) => ({
        ...item,
        matched_query: query,
        quality: scoreFactor(item, expectedUnit, { ...context, query })
      }));

    const liveCandidates = liveRecords
      .map((record) => extractFactor(record, "ademe_live"))
      .filter((item) => Number.isFinite(item.factor) && item.factor > 0)
      .map((item) => ({
        ...item,
        matched_query: query,
        quality: scoreFactor(item, expectedUnit, { ...context, query })
      }));

    allCandidates.push(...localCandidates, ...liveCandidates);
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
      matched_queries: [...new Set([...existing.matched_queries, candidate.matched_query])],
      query_hits: existing.query_hits + 1
    };

    if (candidate.quality.score > existing.quality.score) {
      merged.name = candidate.name;
      merged.factor = candidate.factor;
      merged.unit = candidate.unit;
      merged.category = candidate.category;
      merged.source = candidate.source;
      merged.origin = candidate.origin;
      merged.raw_keys = candidate.raw_keys;
      merged.raw = candidate.raw;
      merged.quality = candidate.quality;
      merged.matched_query = candidate.matched_query;
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
    selection_reason: selected ? buildSelectionReason(selected, expectedUnit) : null,
    ambiguity_level: selected ? (ambiguous ? "medium" : "low") : "high",
    recommended_followup_question: ambiguous ? context.followupQuestion || null : null,
    selected,
    candidates: ranked.slice(0, 5)
  };
}

function provisionalMaterialFactor(material) {
  const m = normalize(material);

  if (m.includes("verre")) {
    return {
      name: "Estimation provisoire verre",
      factor: 0.85,
      unit: "kgCO2e/kg",
      source: "Estimation provisoire non confirmée",
      origin: "provisional",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."] }
    };
  }

  if (m.includes("aluminium") || m.includes("alu")) {
    return {
      name: "Estimation provisoire aluminium",
      factor: 8.6,
      unit: "kgCO2e/kg",
      source: "Estimation provisoire non confirmée",
      origin: "provisional",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."] }
    };
  }

  if (m.includes("acier") || m.includes("inox")) {
    return {
      name: "Estimation provisoire acier",
      factor: 2.2,
      unit: "kgCO2e/kg",
      source: "Estimation provisoire non confirmée",
      origin: "provisional",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."] }
    };
  }

  if (m.includes("bois") || m.includes("palette")) {
    return {
      name: "Estimation provisoire bois",
      factor: 0.12,
      unit: "kgCO2e/kg",
      source: "Estimation provisoire non confirmée",
      origin: "provisional",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."] }
    };
  }

  return null;
}

function provisionalTransportFactor(mode) {
  const m = normalize(mode);

  if (m.includes("routier") || m.includes("camion")) {
    return {
      name: "Estimation provisoire transport routier",
      factor: 0.12,
      unit: "kgCO2e/t.km",
      source: "Estimation provisoire non confirmée",
      origin: "provisional",
      quality: { score: 30, grade: "D", comments: ["Aucun facteur officiel exact retrouvé."] }
    };
  }

  return null;
}

function confidenceFromFactor(selected, fallbackUsed = false) {
  if (fallbackUsed) return "Faible";
  if (!selected?.quality?.grade) return "Faible";
  if (selected.quality.grade === "A" || selected.quality.grade === "B") return "Élevé";
  if (selected.quality.grade === "C") return "Moyen";
  return "Faible";
}

function mcpText(payload) {
  return {
    content: [{ type: "text", text: JSON.stringify(payload, null, 2) }]
  };
}

server.tool(
  "estimate_material_emission",
  "Estime l’empreinte carbone d’une matière avec recherche officielle ADEME live + snapshot local",
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
      followupQuestion: "Peux-tu préciser la matière exacte si tu veux un facteur officiel plus robuste ?"
    });

    let selected = found.selected;
    let provisional_used = false;

    if (!selected && !official_only) {
      selected = provisionalMaterialFactor(material);
      provisional_used = Boolean(selected);
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
      factor_status: provisional_used ? "PROVISIONAL_ESTIMATE" : found.status,
      post: "matière",
      official_only,
      provisional_used,
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
      confidence: confidenceFromFactor(selected, provisional_used),
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
  "Estime l’empreinte carbone d’un transport avec recherche officielle ADEME live + snapshot local",
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
      followupQuestion: "Peux-tu confirmer le mode de transport exact pour fiabiliser le facteur officiel ?"
    });

    let selected = found.selected;
    let provisional_used = false;

    if (!selected && !official_only) {
      selected = provisionalTransportFactor(mode);
      provisional_used = Boolean(selected);
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
      factor_status: provisional_used ? "PROVISIONAL_ESTIMATE" : found.status,
      post: "transport",
      official_only,
      provisional_used,
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
      confidence: confidenceFromFactor(selected, provisional_used),
      queries_tested: found.queries_tested,
      selection_reason: found.selection_reason,
      ambiguity_level: found.ambiguity_level,
      recommended_followup_question: found.recommended_followup_question,
      candidates_reviewed: found.candidates
    });
  }
);

server.tool(
  "estimate_full_product_emission",
  "Estime une empreinte produit complète : matière + transport",
  {
    material: z.string(),
    material_quantity_kg: z.number(),
    transport_mode: z.string().optional(),
    transport_distance_km: z.number().optional(),
    official_only: z.boolean().default(true)
  },
  async ({ material, material_quantity_kg, transport_mode, transport_distance_km, official_only = true }) => {
    const decomposition = [];
    let total = 0;

    const materialResolved = resolveCanonicalTerm(material, MATERIAL_TAXONOMY);
    const materialFound = await findBestFactor(buildMaterialQueries(material), "kg", {
      domain: "material",
      original: material,
      canonical: materialResolved.canonical,
      matched_aliases: materialResolved.matched_aliases,
      followupQuestion: "Peux-tu préciser la matière exacte du produit ?"
    });

    let materialFactor = materialFound.selected;
    let materialProvisional = false;

    if (!materialFactor && !official_only) {
      materialFactor = provisionalMaterialFactor(material);
      materialProvisional = Boolean(materialFactor);
    }

    if (materialFactor) {
      const emission = material_quantity_kg * materialFactor.factor;
      total += emission;
      decomposition.push({
        post: "matière",
        activity: material_quantity_kg,
        activity_unit: "kg",
        factor: materialFactor,
        provisional_used: materialProvisional,
        factor_status: materialProvisional ? "PROVISIONAL_ESTIMATE" : materialFound.status,
        selection_reason: materialFound.selection_reason,
        normalized_input: materialFound.normalized_input,
        emission_kgco2e: emission
      });
    }

    let transportFound = null;

    if (transport_mode && transport_distance_km) {
      const tonne_km = (material_quantity_kg / 1000) * transport_distance_km;
      const transportResolved = resolveCanonicalTerm(transport_mode, TRANSPORT_TAXONOMY);

      transportFound = await findBestFactor(buildTransportQueries(transport_mode), "t.km", {
        domain: "transport",
        original: transport_mode,
        canonical: transportResolved.canonical,
        matched_aliases: transportResolved.matched_aliases,
        followupQuestion: "Peux-tu confirmer le mode de transport exact ?"
      });

      let transportFactor = transportFound.selected;
      let transportProvisional = false;

      if (!transportFactor && !official_only) {
        transportFactor = provisionalTransportFactor(transport_mode);
        transportProvisional = Boolean(transportFactor);
      }

      if (transportFactor) {
        const emission = tonne_km * transportFactor.factor;
        total += emission;
        decomposition.push({
          post: "transport",
          activity: tonne_km,
          activity_unit: "t.km",
          factor: transportFactor,
          provisional_used: transportProvisional,
          factor_status: transportProvisional ? "PROVISIONAL_ESTIMATE" : transportFound.status,
          selection_reason: transportFound.selection_reason,
          normalized_input: transportFound.normalized_input,
          emission_kgco2e: emission
        });
      }
    }

    return mcpText({
      status: decomposition.length > 0 ? "OK" : "OFFICIAL_FACTOR_NOT_FOUND",
      scope: "Scope 3 achats / cradle-to-gate partiel",
      official_only,
      material,
      material_quantity_kg,
      transport_mode: transport_mode || "NON FOURNI / A CONFIRMER",
      transport_distance_km: transport_distance_km || "NON FOURNI / A CONFIRMER",
      material_normalized_input: materialFound.normalized_input,
      material_selection_reason: materialFound.selection_reason,
      transport_selection_reason: transport_mode ? transportFound?.selection_reason || null : null,
      decomposition,
      total_kgco2e: total,
      confidence: decomposition.some((d) => d.provisional_used) ? "Faible" : "Moyen"
    });
  }
);

server.tool(
  "estimate_standard_object_emission",
  "Estime l’empreinte carbone d’un objet métier standard comme palette Europe, canette ou bouteille",
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
        message: "Objet métier non reconnu. Utilise plutôt estimate_material_emission ou précise la matière."
      });
    }

    const unitMass = mass_kg_override ?? object.default_mass_kg;

    if (!unitMass) {
      return mcpText({
        status: "MASS_REQUIRED",
        object_name,
        recognized_object: object.canonical,
        default_material: object.default_material,
        message: "Objet reconnu mais masse non disponible. Fournis mass_kg_override pour obtenir un calcul."
      });
    }

    const found = await findBestFactor(buildObjectQueries(object_name), "kg", {
      domain: "object",
      original: object_name,
      canonical: object.canonical,
      matched_aliases: [object.alias],
      followupQuestion: "Peux-tu confirmer l’objet exact si tu veux fiabiliser encore le facteur officiel ?"
    });

    let selected = found.selected;
    let provisional_used = false;

    if (!selected && !official_only) {
      selected = provisionalMaterialFactor(object.default_material);
      provisional_used = Boolean(selected);
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

    const totalMassKg = unitMass * quantity;
    const totalKgco2e = totalMassKg * selected.factor;

    return mcpText({
      status: "OK",
      factor_status: provisional_used ? "PROVISIONAL_ESTIMATE" : found.status,
      official_only,
      provisional_used,
      object_name,
      recognized_object: object.canonical,
      default_material: object.default_material,
      unit_mass_kg_used: unitMass,
      quantity,
      total_mass_kg: totalMassKg,
      selected_factor: selected,
      calculation: {
        formula: "(quantity × unit_mass_kg) × factor",
        result: totalKgco2e,
        unit: "kgCO2e"
      },
      assumptions: [
        `objet interprété comme: ${object.canonical}`,
        `matière probable: ${object.default_material}`,
        `masse unitaire utilisée: ${unitMass} kg`
      ],
      confidence: confidenceFromFactor(selected, provisional_used),
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
  "Estime l’empreinte carbone d’une consommation énergétique",
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
        normalized_input: found.normalized_input,
        queries_tested: found.queries_tested,
        candidates_reviewed: found.candidates,
        official_only
      });
    }

    return mcpText({
      status: "OK",
      factor_status: found.status,
      official_only,
      post: "énergie",
      energy_type,
      normalized_input: found.normalized_input,
      consumption_kwh,
      selected_factor: found.selected,
      calculation: {
        formula: "kWh × factor",
        result: consumption_kwh * found.selected.factor,
        unit: "kgCO2e"
      },
      confidence: confidenceFromFactor(found.selected, false),
      queries_tested: found.queries_tested,
      selection_reason: found.selection_reason,
      ambiguity_level: found.ambiguity_level,
      recommended_followup_question: found.recommended_followup_question,
      candidates_reviewed: found.candidates
    });
  }
);

server.tool(
  "search_factor",
  "Recherche brute de facteurs ADEME live + snapshot local",
  {
    query: z.string(),
    rows: z.number().optional()
  },
  async ({ query, rows = 10 }) => {
    const localResults = searchLocalSnapshot(query, rows).map((record) => extractFactor(record, "local_snapshot"));
    const liveResults = (await fetchAdemeLiveRecords({ query, rows })).map((record) => extractFactor(record, "ademe_live"));

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
  "Récupère le meilleur facteur selon une requête, en priorité sur sources officielles",
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
      followupQuestion: "Peux-tu préciser la matière, l’énergie, le transport ou l’objet visé ?"
    });

    let selected = found.selected;
    let provisional_used = false;

    if (!selected && !official_only) {
      selected = provisionalMaterialFactor(query) || provisionalTransportFactor(query);
      provisional_used = Boolean(selected);
    }

    return mcpText({
      status: selected ? "OK" : "OFFICIAL_FACTOR_NOT_FOUND",
      factor_status: selected ? (provisional_used ? "PROVISIONAL_ESTIMATE" : found.status) : found.status,
      query,
      official_only,
      provisional_used,
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
  res.send("MCP Carbone Hermes OK — v4.0.0");
});

app.get("/health", async (req, res) => {
  const localCount = LOCAL_SNAPSHOT.length;
  const liveTest = await fetchAdemeLiveRecords({ query: "verre", rows: 3 });

  res.json({
    status: "OK",
    service: "mcp-carbone-hermes",
    version: "4.0.0",
    dataset: DATASET,
    local_snapshot: {
      path: LOCAL_SNAPSHOT_PATH,
      loaded_records: localCount
    },
    ademe_live: {
      status: liveTest.length > 0 ? "OK" : "CHECK_NEEDED",
      test_query: "verre",
      result_count: liveTest.length
    }
  });
});

app.get("/reload-snapshot", (req, res) => {
  LOCAL_SNAPSHOT = loadLocalSnapshot();
  res.json({
    status: "OK",
    loaded_records: LOCAL_SNAPSHOT.length
  });
});

app.get("/debug/ademe/:query", async (req, res) => {
  const query = req.params.query;
  const localResults = searchLocalSnapshot(query, 10).map((record) => extractFactor(record, "local_snapshot"));
  const liveResults = (await fetchAdemeLiveRecords({ query, rows: 10 })).map((record) => extractFactor(record, "ademe_live"));

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
  console.log(`MCP Carbone Hermes v4.0.0 running on port ${PORT}`);
  console.log(`Local snapshot records: ${LOCAL_SNAPSHOT.length}`);
});
