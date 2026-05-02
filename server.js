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
  version: "3.2.0"
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

function simplifyMaterial(input = "") {
  const text = normalize(input);

  if (text.includes("verre")) return "verre";
  if (text.includes("aluminium") || text.includes("alu") || text.includes("canette")) return "aluminium";
  if (text.includes("acier") || text.includes("inox") || text.includes("galvanise")) return "acier";
  if (text.includes("carton") || text.includes("papier")) return "carton";
  if (text.includes("pet")) return "pet";
  if (text.includes("plastique") || text.includes("polypropylene") || text.includes("pp")) return "plastique";
  if (text.includes("bois") || text.includes("palette")) return "bois";
  if (text.includes("cuivre")) return "cuivre";

  return input;
}

function simplifyTransportMode(input = "") {
  const text = normalize(input);

  if (text.includes("camion") || text.includes("routier") || text.includes("semi")) return "transport routier";
  if (text.includes("maritime") || text.includes("bateau") || text.includes("navire")) return "transport maritime";
  if (text.includes("aerien") || text.includes("avion")) return "transport aérien";
  if (text.includes("train") || text.includes("rail") || text.includes("ferroviaire")) return "transport ferroviaire";

  return `transport ${input}`;
}

function buildMaterialQueries(material = "") {
  const simple = simplifyMaterial(material);
  const queries = [];

  if (normalize(material).includes("recycl")) {
    queries.push(`${simple} recyclé`);
    queries.push(`${simple} recycle`);
  }

  queries.push(simple);
  queries.push(`${simple} kg`);
  queries.push(`${simple} fabrication`);
  queries.push(`${simple} matière`);
  queries.push(`${simple} emballage`);

  return [...new Set(queries.filter(Boolean))];
}

function buildTransportQueries(mode = "") {
  const simple = simplifyTransportMode(mode);

  return [
    simple,
    `${simple} marchandise`,
    `${simple} marchandises`,
    `${simple} t.km`,
    "transport routier marchandises",
    "transport routier"
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

async function searchAdeme(query, rows = 100) {
  const direct = await fetchAdemeRecords({ query, rows });

  if (direct.length > 0) return direct;

  const fallbackRecords = [];
  const pages = [0, 100, 200, 300, 400];

  for (const start of pages) {
    const page = await fetchAdemeRecords({ query: "", rows: 100, start });
    fallbackRecords.push(...page);
  }

  const q = normalize(query);
  const filtered = fallbackRecords.filter((record) => {
    const fields = record.fields || {};
    const haystack = stringifyFields(fields);
    return q
      .split(" ")
      .filter(Boolean)
      .some((word) => word.length > 2 && haystack.includes(word));
  });

  console.log(`[ADEME_FALLBACK_FILTER] query="${query}" filtered=${filtered.length}`);
  return filtered.slice(0, rows);
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

function scoreFactor(item, expectedUnit = "", query = "") {
  let score = 0;
  const comments = [];
  const unit = normalize(item.unit || "");
  const name = normalize(item.name || "");
  const q = normalize(query || "");

  if (item.factor > 0) score += 30;
  else comments.push("Facteur nul ou négatif exclu.");

  if (item.unit && item.unit !== "unité non précisée") score += 20;
  else comments.push("Unité non précisée.");

  if (expectedUnit && isLikelyUnitMatch(unit, expectedUnit)) score += 25;
  else if (expectedUnit) comments.push("Unité potentiellement non alignée.");

  if (item.name && item.name !== "Facteur ADEME non nommé") score += 15;

  const words = q.split(" ").filter((w) => w.length > 2);
  if (words.some((w) => name.includes(w))) score += 15;

  let grade = "E";
  if (score >= 85) grade = "A";
  else if (score >= 70) grade = "B";
  else if (score >= 50) grade = "C";
  else if (score >= 30) grade = "D";

  return { score, grade, comments };
}

async function findBestFactor(queries, expectedUnit = "") {
  const tested = [];

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
        quality: scoreFactor(item, expectedUnit, query)
      }))
      .sort((a, b) => b.quality.score - a.quality.score);

    if (candidates.length > 0) {
      return {
        query_used: query,
        queries_tested: tested,
        selected: candidates[0],
        candidates: candidates.slice(0, 5)
      };
    }
  }

  return {
    query_used: null,
    queries_tested: tested,
    selected: null,
    candidates: []
  };
}

function fallbackMaterialFactor(material) {
  const m = normalize(material);

  if (m.includes("verre")) {
    return {
      name: "Proxy verre — facteur indicatif de secours",
      factor: 0.85,
      unit: "kgCO2e/kg",
      source: "Proxy interne de secours — à remplacer par facteur ADEME",
      quality: { score: 30, grade: "D", comments: ["Proxy utilisé car aucun facteur ADEME exploitable n’a été récupéré."] }
    };
  }

  if (m.includes("aluminium")) {
    return {
      name: "Proxy aluminium — facteur indicatif de secours",
      factor: 8.6,
      unit: "kgCO2e/kg",
      source: "Proxy interne de secours — à remplacer par facteur ADEME",
      quality: { score: 30, grade: "D", comments: ["Proxy utilisé car aucun facteur ADEME exploitable n’a été récupéré."] }
    };
  }

  if (m.includes("acier")) {
    return {
      name: "Proxy acier — facteur indicatif de secours",
      factor: 2.2,
      unit: "kgCO2e/kg",
      source: "Proxy interne de secours — à remplacer par facteur ADEME",
      quality: { score: 30, grade: "D", comments: ["Proxy utilisé car aucun facteur ADEME exploitable n’a été récupéré."] }
    };
  }

  return null;
}

function fallbackTransportFactor(mode) {
  const m = normalize(mode);

  if (m.includes("routier") || m.includes("camion")) {
    return {
      name: "Proxy transport routier marchandise",
      factor: 0.12,
      unit: "kgCO2e/t.km",
      source: "Proxy interne de secours — à remplacer par facteur ADEME",
      quality: { score: 30, grade: "D", comments: ["Proxy utilisé car aucun facteur ADEME exploitable n’a été récupéré."] }
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
    const found = await findBestFactor(buildMaterialQueries(material), "kg");

    let selected = found.selected;
    let fallback_used = false;

    if (!selected) {
      selected = fallbackMaterialFactor(material);
      fallback_used = Boolean(selected);
    }

    if (!selected) {
      return mcpText({ status: "NO_FACTOR_FOUND", material, quantity, unit, queries_tested: found.queries_tested });
    }

    return mcpText({
      status: "OK",
      post: "matière",
      fallback_used,
      material,
      simplified_material: simplifyMaterial(material),
      quantity,
      quantity_unit: unit,
      selected_factor: selected,
      calculation: {
        formula: "quantity × factor",
        result: quantity * selected.factor,
        unit: "kgCO2e"
      },
      confidence: fallback_used ? "Faible" : selected.quality.grade === "A" || selected.quality.grade === "B" ? "Élevé" : "Moyen",
      queries_tested: found.queries_tested,
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
    const tonne_km = (mass_kg / 1000) * distance_km;
    const found = await findBestFactor(buildTransportQueries(mode), "t.km");

    let selected = found.selected;
    let fallback_used = false;

    if (!selected) {
      selected = fallbackTransportFactor(mode);
      fallback_used = Boolean(selected);
    }

    if (!selected) {
      return mcpText({ status: "NO_FACTOR_FOUND", mode, mass_kg, distance_km, tonne_km, queries_tested: found.queries_tested });
    }

    return mcpText({
      status: "OK",
      post: "transport",
      fallback_used,
      mode,
      clean_mode: simplifyTransportMode(mode),
      mass_kg,
      distance_km,
      tonne_km,
      selected_factor: selected,
      calculation: {
        formula: "tonne.km × factor",
        result: tonne_km * selected.factor,
        unit: "kgCO2e"
      },
      confidence: fallback_used ? "Faible" : selected.quality.grade === "A" || selected.quality.grade === "B" ? "Élevé" : "Moyen",
      queries_tested: found.queries_tested,
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

    const materialFound = await findBestFactor(buildMaterialQueries(material), "kg");
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
        emission_kgco2e: emission
      });
    }

    if (transport_mode && transport_distance_km) {
      const tonne_km = (material_quantity_kg / 1000) * transport_distance_km;
      const transportFound = await findBestFactor(buildTransportQueries(transport_mode), "t.km");
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
    const found = await findBestFactor([energy_type, `${energy_type} kWh`, "électricité France"], "kwh");

    if (!found.selected) {
      return mcpText({ status: "NO_FACTOR_FOUND", energy_type, consumption_kwh, queries_tested: found.queries_tested });
    }

    return mcpText({
      status: "OK",
      post: "énergie",
      energy_type,
      consumption_kwh,
      selected_factor: found.selected,
      calculation: {
        formula: "kWh × factor",
        result: consumption_kwh * found.selected.factor,
        unit: "kgCO2e"
      }
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
    const found = await findBestFactor([query], expected_unit_keyword);
    return mcpText({
      status: found.selected ? "OK" : "NO_FACTOR_FOUND",
      query,
      selected_factor: found.selected,
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
  res.send("MCP Carbone Hermes OK — v3.2.0");
});

app.get("/health", (req, res) => {
  res.json({ status: "OK", service: "mcp-carbone-hermes", version: "3.2.0", dataset: DATASET });
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
    if (!res.headersSent) res.status(500).json({ error: "MCP_ERROR", message: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`MCP Carbone Hermes v3.2 running on port ${PORT}`);
});
