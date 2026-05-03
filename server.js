import express from "express";
import axios from "axios";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 3000;
const DATASET = "base-carboner";
const BASE_URL = "https://data.ademe.fr/api/records/1.0/search/";

const server = new McpServer({
  name: "mcp-carbone-hermes",
  version: "3.3.0"
});

function normalize(text = "") {
  return String(text)
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^a-z0-9\s.,/-]/g, " ")
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

const MATERIAL_TAXONOMY = [
  {
    canonical: "verre",
    aliases: ["verre", "bouteille verre", "flacon verre", "emballage verre"]
  },
  {
    canonical: "aluminium",
    aliases: ["aluminium", "alu", "canette", "boite alu"]
  },
  {
    canonical: "acier",
    aliases: ["acier", "inox", "galvanise", "acier galvanise", "metal acier"]
  },
  {
    canonical: "carton",
    aliases: ["carton", "papier", "papier carton", "emballage carton"]
  },
  {
    canonical: "pet",
    aliases: ["pet", "plastique pet", "bouteille pet", "flacon pet", "emballage pet"]
  },
  {
    canonical: "pp",
    aliases: ["pp", "polypropylene", "plastique pp", "barquette pp"]
  },
  {
    canonical: "plastique",
    aliases: ["plastique", "emballage plastique", "film plastique", "barquette plastique"]
  },
  {
    canonical: "bois",
    aliases: ["bois", "palette", "palette bois"]
  },
  {
    canonical: "cuivre",
    aliases: ["cuivre"]
  }
];

const TRANSPORT_TAXONOMY = [
  {
    canonical: "transport routier",
    aliases: ["camion", "routier", "poids lourd", "semi", "semi remorque", "transport camion"]
  },
  {
    canonical: "transport maritime",
    aliases: ["maritime", "bateau", "navire", "cargo"]
  },
  {
    canonical: "transport aérien",
    aliases: ["aerien", "avion", "fret aerien"]
  },
  {
    canonical: "transport ferroviaire",
    aliases: ["train", "rail", "ferroviaire"]
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

async function fetchAdemeRecords({ query = "", rows = 100, start = 0 }) {
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
    console.log(`[ADEME] query="${query || "(none)"}" start=${start} records=${records.length}`);
    return records;
  } catch (error) {
    console.error(`[ADEME_ERROR] query="${query}"`, {
      message: error.message,
      status: error.response?.status,
      data: error.response?.data
    });
    return [];
  }
}

function scoreLocalRecordMatch(record, query = "") {
  const q = normalize(query);
  const fields = record.fields || {};
  const haystack = stringifyFields(fields);

  if (!q) return 0;

  const words = q.split(" ").filter((word) => word.length > 2);
  let score = 0;

  if (haystack.includes(q)) score += 40;

  for (const word of words) {
    if (haystack.includes(word)) score += 10;
  }

  return score;
}

async function searchAdeme(query, rows = 100) {
  const direct = await fetchAdemeRecords({ query, rows });

  if (direct.length > 0) return direct;

  const fallbackRecords = [];
  const pages = [0, 100, 200, 300, 400, 500];

  for (const start of pages) {
    const page = await fetchAdemeRecords({ query: "", rows: 100, start });
    fallbackRecords.push(...page);
  }

  const scored = fallbackRecords
    .map((record) => ({
      record,
      score: scoreLocalRecordMatch(record, query)
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .map((item) => item.record);

  console.log(`[ADEME_FALLBACK_FILTER] query="${query}" filtered=${scored.length}`);
  return scored.slice(0, rows);
}

function detectNumericFields(fields = {}) {
  const numeric = [];

  for (const [key, value] of Object.entries(fields)) {
    const n = toNumber(value);
    if (n !== null && Number.isFinite(n)) {
      numeric.push({ key, value: n });
    }
  }

  return numeric;
}

function extractFactor(record) {
  const fields = record.fields || {};

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
    fields.impact
  ];

  let factor = factorCandidates.map(toNumber).find((v) => Number.isFinite(v));

  if (!Number.isFinite(factor)) {
    const numericFields = detectNumericFields(fields);

    const preferred = numericFields.find((item) => {
      const k = normalize(item.key);
      return (
        k.includes("co2") ||
        k.includes("ges") ||
        k.includes("emission") ||
        k.includes("facteur") ||
        k.includes("total")
      );
    });

    factor = preferred?.value ?? null;
  }

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

  return {
    id: record.recordid,
    name: name || "Facteur ADEME non nommé",
    factor,
    unit: unit || "unité non précisée",
    category: firstDefined(fields.categorie, fields.type_ligne, fields.type_de_facteur) || "catégorie non précisée",
    source: "ADEME Base Carbone",
    raw_keys: Object.keys(fields),
    raw: fields
  };
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

  if (canonical && name.includes(canonical)) score += 20;
  if (canonical && category.includes(canonical)) score += 10;

  if (query && name.includes(query)) score += 10;

  if (domain === "transport" && (name.includes("marchandise") || category.includes("transport"))) score += 10;
  if (domain === "material" && (name.includes("emballage") || category.includes("mati"))) score += 5;
  if (domain === "energy" && (name.includes("electric") || name.includes("électric"))) score += 8;

  if (name.includes("moyenne") || name.includes("generique")) {
    score -= 5;
    comments.push("Libellé potentiellement générique.");
  }

  let grade = "E";
  if (score >= 85) grade = "A";
  else if (score >= 70) grade = "B";
  else if (score >= 55) grade = "C";
  else if (score >= 40) grade = "D";

  return { score, grade, comments };
}

function buildSelectionReason(selected, expectedUnit = "") {
  const reasons = [];

  if (selected?.quality?.grade) reasons.push(`grade ${selected.quality.grade}`);
  if (expectedUnit && isLikelyUnitMatch(selected.unit || "", expectedUnit)) {
    reasons.push(`unité alignée ${expectedUnit}`);
  }
  if (selected?.name) reasons.push(`libellé retenu: ${selected.name}`);

  return reasons.join(" | ");
}

async function findBestFactor(queries, expectedUnit = "", context = {}) {
  const tested = [];
  const allCandidates = [];

  for (const query of queries) {
    const records = await searchAdeme(query, 100);
    tested.push(query);

    const parsed = records.map(extractFactor);

    console.log(
      `[PARSED] query="${query}"`,
      parsed.slice(0, 3).map((item) => ({
        name: item.name,
        factor: item.factor,
        unit: item.unit,
        keys: item.raw_keys
      }))
    );

    const candidates = parsed
      .filter((item) => Number.isFinite(item.factor))
      .filter((item) => item.factor > 0)
      .map((item) => ({
        ...item,
        matched_query: query,
        quality: scoreFactor(item, expectedUnit, { ...context, query })
      }));

    allCandidates.push(...candidates);
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
    status: selected ? (ambiguous ? "AMBIGUOUS_RESULT" : "CONFIRMED_RESULT") : "NO_FACTOR_FOUND",
    query_used: selected?.matched_query || null,
    queries_tested: tested,
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

function fallbackMaterialFactor(material) {
  const m = normalize(material);

  if (m.includes("verre")) {
    return {
      name: "Estimation de secours verre",
      factor: 0.85,
      unit: "kgCO2e/kg",
      source: "Estimation de secours non confirmée",
      quality: {
        score: 30,
        grade: "D",
        comments: ["Estimation de secours utilisée car aucun facteur ADEME exploitable n’a été récupéré."]
      }
    };
  }

  if (m.includes("aluminium") || m.includes("alu")) {
    return {
      name: "Estimation de secours aluminium",
      factor: 8.6,
      unit: "kgCO2e/kg",
      source: "Estimation de secours non confirmée",
      quality: {
        score: 30,
        grade: "D",
        comments: ["Estimation de secours utilisée car aucun facteur ADEME exploitable n’a été récupéré."]
      }
    };
  }

  if (m.includes("acier") || m.includes("inox")) {
    return {
      name: "Estimation de secours acier",
      factor: 2.2,
      unit: "kgCO2e/kg",
      source: "Estimation de secours non confirmée",
      quality: {
        score: 30,
        grade: "D",
        comments: ["Estimation de secours utilisée car aucun facteur ADEME exploitable n’a été récupéré."]
      }
    };
  }

  return null;
}

function fallbackTransportFactor(mode) {
  const m = normalize(mode);

  if (m.includes("routier") || m.includes("camion")) {
    return {
      name: "Estimation de secours transport routier marchandise",
      factor: 0.12,
      unit: "kgCO2e/t.km",
      source: "Estimation de secours non confirmée",
      quality: {
        score: 30,
        grade: "D",
        comments: ["Estimation de secours utilisée car aucun facteur ADEME exploitable n’a été récupéré."]
      }
    };
  }

  return null;
}

function mcpText(payload) {
  return {
    content: [{ type: "text", text: JSON.stringify(payload, null, 2) }]
  };
}

server.tool(
  "estimate_material_emission",
  "Estime l’empreinte carbone d’une matière avec recherche ADEME et fallback contrôlé",
  {
    material: z.string(),
    quantity: z.number(),
    unit: z.string().default("kg")
  },
  async ({ material, quantity, unit = "kg" }) => {
    const resolved = resolveCanonicalTerm(material, MATERIAL_TAXONOMY);
    const found = await findBestFactor(buildMaterialQueries(material), "kg", {
      domain: "material",
      original: material,
      canonical: resolved.canonical,
      matched_aliases: resolved.matched_aliases,
      followupQuestion:
        "Peux-tu préciser la matière exacte si tu veux un facteur plus robuste, par exemple PET, PP, verre, aluminium ou carton ?"
    });

    let selected = found.selected;
    let fallback_used = false;

    if (!selected) {
      selected = fallbackMaterialFactor(material);
      fallback_used = Boolean(selected);
    }

    if (!selected) {
      return mcpText({
        status: "NO_FACTOR_FOUND",
        material,
        quantity,
        unit,
        normalized_input: found.normalized_input,
        queries_tested: found.queries_tested
      });
    }

    return mcpText({
      status: "OK",
      factor_status: fallback_used ? "FALLBACK_RESULT" : found.status,
      post: "matière",
      fallback_used,
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
      confidence:
        fallback_used
          ? "Faible"
          : selected.quality.grade === "A" || selected.quality.grade === "B"
            ? "Élevé"
            : "Moyen",
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
  "Estime l’empreinte carbone d’un transport en t.km avec recherche ADEME et fallback contrôlé",
  {
    mode: z.string(),
    mass_kg: z.number(),
    distance_km: z.number()
  },
  async ({ mode, mass_kg, distance_km }) => {
    const resolved = resolveCanonicalTerm(mode, TRANSPORT_TAXONOMY);
    const tonne_km = (mass_kg / 1000) * distance_km;

    const found = await findBestFactor(buildTransportQueries(mode), "t.km", {
      domain: "transport",
      original: mode,
      canonical: resolved.canonical,
      matched_aliases: resolved.matched_aliases,
      followupQuestion:
        "Peux-tu confirmer le mode de transport si tu veux un résultat plus robuste : routier, maritime, aérien ou ferroviaire ?"
    });

    let selected = found.selected;
    let fallback_used = false;

    if (!selected) {
      selected = fallbackTransportFactor(mode);
      fallback_used = Boolean(selected);
    }

    if (!selected) {
      return mcpText({
        status: "NO_FACTOR_FOUND",
        mode,
        mass_kg,
        distance_km,
        tonne_km,
        normalized_input: found.normalized_input,
        queries_tested: found.queries_tested
      });
    }

    return mcpText({
      status: "OK",
      factor_status: fallback_used ? "FALLBACK_RESULT" : found.status,
      post: "transport",
      fallback_used,
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
      confidence:
        fallback_used
          ? "Faible"
          : selected.quality.grade === "A" || selected.quality.grade === "B"
            ? "Élevé"
            : "Moyen",
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
    transport_distance_km: z.number().optional()
  },
  async ({ material, material_quantity_kg, transport_mode, transport_distance_km }) => {
    const decomposition = [];
    let total = 0;

    const materialResolved = resolveCanonicalTerm(material, MATERIAL_TAXONOMY);
    const materialFound = await findBestFactor(buildMaterialQueries(material), "kg", {
      domain: "material",
      original: material,
      canonical: materialResolved.canonical,
      matched_aliases: materialResolved.matched_aliases,
      followupQuestion: "Peux-tu préciser la matière exacte du produit pour affiner le facteur ?"
    });

    let materialFactor = materialFound.selected;
    let materialFallback = false;

    if (!materialFactor) {
      materialFactor = fallbackMaterialFactor(material);
      materialFallback = Boolean(materialFactor);
    }

    if (materialFactor) {
      const emission = material_quantity_kg * materialFactor.factor;
      total += emission;
      decomposition.push({
        post: "matière",
        activity: material_quantity_kg,
        activity_unit: "kg",
        factor: materialFactor,
        fallback_used: materialFallback,
        factor_status: materialFallback ? "FALLBACK_RESULT" : materialFound.status,
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
        followupQuestion: "Peux-tu confirmer le mode de transport exact pour affiner la partie logistique ?"
      });

      let transportFactor = transportFound.selected;
      let transportFallback = false;

      if (!transportFactor) {
        transportFactor = fallbackTransportFactor(transport_mode);
        transportFallback = Boolean(transportFactor);
      }

      if (transportFactor) {
        const emission = tonne_km * transportFactor.factor;
        total += emission;
        decomposition.push({
          post: "transport",
          activity: tonne_km,
          activity_unit: "t.km",
          factor: transportFactor,
          fallback_used: transportFallback,
          factor_status: transportFallback ? "FALLBACK_RESULT" : transportFound.status,
          selection_reason: transportFound.selection_reason,
          normalized_input: transportFound.normalized_input,
          emission_kgco2e: emission
        });
      }
    }

    return mcpText({
      status: decomposition.length > 0 ? "OK" : "NO_FACTOR_FOUND",
      scope: "Scope 3 achats / cradle-to-gate partiel",
      material,
      material_quantity_kg,
      transport_mode: transport_mode || "NON FOURNI / A CONFIRMER",
      transport_distance_km: transport_distance_km || "NON FOURNI / A CONFIRMER",
      material_normalized_input: materialFound.normalized_input,
      material_selection_reason: materialFound.selection_reason,
      transport_selection_reason: transport_mode ? transportFound?.selection_reason || null : null,
      decomposition,
      total_kgco2e: total,
      confidence: decomposition.some((d) => d.fallback_used) ? "Faible" : "Moyen"
    });
  }
);

server.tool(
  "estimate_energy_emission",
  "Estime l’empreinte carbone d’une consommation énergétique",
  {
    energy_type: z.string(),
    consumption_kwh: z.number()
  },
  async ({ energy_type, consumption_kwh }) => {
    const found = await findBestFactor(buildEnergyQueries(energy_type), "kwh", {
      domain: "energy",
      original: energy_type,
      canonical: energy_type,
      matched_aliases: [],
      followupQuestion:
        "Peux-tu préciser le type d’énergie ou le pays si tu veux une estimation énergétique plus robuste ?"
    });

    if (!found.selected) {
      return mcpText({
        status: "NO_FACTOR_FOUND",
        energy_type,
        consumption_kwh,
        normalized_input: found.normalized_input,
        queries_tested: found.queries_tested
      });
    }

    return mcpText({
      status: "OK",
      factor_status: found.status,
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
      confidence:
        found.selected.quality.grade === "A" || found.selected.quality.grade === "B"
          ? "Élevé"
          : "Moyen",
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
  "Recherche brute de facteurs ADEME",
  {
    query: z.string(),
    rows: z.number().optional()
  },
  async ({ query, rows = 10 }) => {
    const records = await searchAdeme(query, rows);
    return mcpText({ query, count: records.length, results: records.map(extractFactor) });
  }
);

server.tool(
  "get_factor",
  "Récupère le meilleur facteur selon une requête",
  {
    query: z.string(),
    expected_unit_keyword: z.string().optional()
  },
  async ({ query, expected_unit_keyword = "" }) => {
    const found = await findBestFactor([query], expected_unit_keyword, {
      domain: "generic",
      original: query,
      canonical: query,
      matched_aliases: [],
      followupQuestion: "Peux-tu préciser la matière, l’énergie ou le mode de transport visé ?"
    });

    return mcpText({
      status: found.selected ? "OK" : "NO_FACTOR_FOUND",
      factor_status: found.status,
      query,
      normalized_input: found.normalized_input,
      selected_factor: found.selected,
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
  res.send("MCP Carbone Hermes OK — v3.3.0");
});

app.get("/health", (req, res) => {
  res.json({ status: "OK", service: "mcp-carbone-hermes", version: "3.3.0", dataset: DATASET });
});

app.get("/debug/ademe/:query", async (req, res) => {
  const query = req.params.query;
  const records = await searchAdeme(query, 10);
  res.json({
    query,
    count: records.length,
    parsed: records.map(extractFactor),
    raw_first_record: records[0] || null
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
  console.log(`MCP Carbone Hermes v3.3 running on port ${PORT}`);
});
