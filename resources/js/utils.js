import { getAccessToken } from "./auth";

let loginHandler = null;
const deploymentBaseUrl = new URL("../", import.meta.url);

export function apiUrl(path = "") {
    const relativePath = String(path).replace(/^\/+/, "");
    return new URL(relativePath, deploymentBaseUrl).toString();
}

export function normalizeConnectorName(value, options = {}) {
    const { stripPath = false, stripJsonExtension = false } = options;
    if (typeof value !== "string") {
        return "";
    }

    let normalized = value.trim();
    if (!normalized) {
        return "";
    }

    if (stripPath) {
        const pathNormalized = normalized.replace(/\\/g, "/");
        normalized = pathNormalized.split("/").pop() || pathNormalized;
    }

    if (stripJsonExtension && normalized.toLowerCase().endsWith(".json")) {
        normalized = normalized.slice(0, -5);
    }

    return normalized.trim();
}

export function parseConnectorDimensions(connector) {
    if (!connector || typeof connector !== "object" || !Array.isArray(connector.dimensions)) {
        return [];
    }

    return connector.dimensions.map((dimension) => {
        if (!dimension || typeof dimension !== "object") {
            return {
                composite: "",
                bindings: {}
            };
        }

        const composite = typeof dimension.composite === "string"
            ? dimension.composite.trim()
            : "";
        const bindings = (dimension.bindings && typeof dimension.bindings === "object" && !Array.isArray(dimension.bindings))
            ? dimension.bindings
            : {};

        return { composite, bindings };
    });
}

export function getSortedBindingEntries(bindings) {
    if (!bindings || typeof bindings !== "object" || Array.isArray(bindings)) {
        return [];
    }

    const entries = [];
    for (const [slotRaw, targetRaw] of Object.entries(bindings)) {
        const slotStr = String(slotRaw).trim();
        if (!/^\d+$/.test(slotStr)) {
            continue;
        }

        const slotId = Number.parseInt(slotStr, 10);
        if (!Number.isInteger(slotId) || slotId < 0) {
            continue;
        }

        const targetName = typeof targetRaw === "string" ? targetRaw.trim() : "";
        if (!targetName) {
            continue;
        }

        entries.push({ slotId, targetName });
    }

    entries.sort((lhs, rhs) => {
        if (lhs.slotId !== rhs.slotId) {
            return lhs.slotId - rhs.slotId;
        }
        return lhs.targetName.localeCompare(rhs.targetName);
    });

    return entries;
}

export function configureLoginHandler(handler) {
    loginHandler = handler;
}

export function copyTextFromElement(elementId) {
    const el = document.getElementById(elementId);
    if (!el) return;

    const text = el.innerText || el.textContent;
    navigator.clipboard.writeText(text)
        .then(() => {
        })
        .catch(err => {
            alert("Failed to copy: " + err);
        });
}

// Function to format the JSON response nicely
export function formatJSON(text) {
    try {
        const json = JSON.parse(text);
        return JSON.stringify(json, null, 2); // Pretty-print with 2 spaces
    } catch (e) {
        return text; // If not valid JSON, return as it is
    }
}

export async function requestWithLogin(url, options = {}) {
    // clone options to avoid mutating caller object
    const opts = { ...options };
    const headers = new Headers(opts.headers || {});

    // attach Authorization header if we have an access token
    let token = getAccessToken();
    if (token) {
        headers.set("Authorization", `Bearer ${token}`);
    }

    opts.headers = headers;

    // DO NOT include cookies on normal API calls unless needed
    // (keeps CSRF surface minimal)
    opts.credentials ??= "omit";

    // first attempt
    let res = await fetch(url, opts);
    if (res.status !== 401 || typeof loginHandler !== "function") {
        return res;
    }

    // 401 → try login once more
    if (!await loginHandler()) {
        return res; // refresh failed → propagate 401
    }

    // retry original request ONCE with new token
    token = getAccessToken();
    if (token) {
        headers.set("Authorization", `Bearer ${token}`);
    }

    opts.headers = headers;

    return fetch(url, opts);
}
