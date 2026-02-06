import { setAccessToken } from "./auth";
import { requestWithLogin, formatJSON, configureLoginHandler } from "./utils";

export { copyTextFromElement } from "./utils";

// --------------------------------------------------------------------------
// Feature
// --------------------------------------------------------------------------
export async function updateFeatureRequestPreview() {
    const json = constructStructuredFeature();
    document.getElementById('POST_featureRequestBody').textContent = json;
}

export function clearDimensions() {
    const container = document.getElementById('dimensionsContainer');
    container.innerHTML = '';
    updateFeatureRequestPreview();
}

export function addDimension() {
    const container = document.getElementById('dimensionsContainer');
    const index = container.children.length;

    const dim = document.createElement('div');
    dim.className = 'dimension';

    const fieldset = document.createElement('fieldset');
    fieldset.style.border = '1px solid #555';
    fieldset.style.padding = '1rem';
    fieldset.style.marginBottom = '0.5rem';

    const legend = document.createElement('legend');
    legend.textContent = `Dimension ${index + 1}`;

    const transformationsDiv = document.createElement('div');
    transformationsDiv.className = 'transformations';

    const addBtn = document.createElement('button');
    addBtn.type = 'button';
    addBtn.textContent = '➕ Add transformation';
    addBtn.addEventListener('click', () => addTransformation(addBtn));

    fieldset.appendChild(legend);
    fieldset.appendChild(transformationsDiv);
    fieldset.appendChild(addBtn);

    dim.appendChild(fieldset);
    container.appendChild(dim);

    updateFeatureRequestPreview();
}

export function addTransformation(button) {
    const container = button.previousElementSibling;

    const t = document.createElement('div');
    t.style.marginBottom = '1rem';

    const hr = document.createElement('hr');
    t.appendChild(hr);

    // Create row container
    const row = document.createElement('div');
    row.style.display = 'flex';
    row.style.gap = '1rem';
    row.style.alignItems = 'flex-start';

    // Column 1: name
    const nameCol = document.createElement('div');
    nameCol.style.flex = '1';
    nameCol.style.marginLeft = '1rem';
    nameCol.style.marginRight = '1rem';

    const nameLabel = document.createElement('label');
    nameLabel.textContent = 'Transformation name';

    const nameInput = document.createElement('input');
    nameInput.type = 'text';
    nameInput.className = 'transformation-name';
    nameInput.placeholder = 'add';
    nameInput.addEventListener('input', updateFeatureRequestPreview);

    nameCol.appendChild(nameLabel);
    nameCol.appendChild(nameInput);

    // Column 2: args
    const argsCol = document.createElement('div');
    argsCol.style.flex = '1';
    argsCol.style.marginLeft = '1rem';
    argsCol.style.marginRight = '1rem';

    const argsLabel = document.createElement('label');
    argsLabel.textContent = 'args (comma-separated)';

    const argsInput = document.createElement('input');
    argsInput.type = 'text';
    argsInput.className = 'transformation-args';
    argsInput.placeholder = '...';
    argsInput.addEventListener('input', updateFeatureRequestPreview);

    argsCol.appendChild(argsLabel);
    argsCol.appendChild(argsInput);

    // Assemble row
    row.appendChild(nameCol);
    row.appendChild(argsCol);
    t.appendChild(row);

    container.appendChild(t);
    updateFeatureRequestPreview();
}

function constructStructuredFeature() {
    const name = document.getElementById('in_featureName').value.trim();
    const dimensions = [];

    document.querySelectorAll('.dimension').forEach(dimEl => {
        const transformations = [];
        dimEl.querySelectorAll('.transformations > div').forEach(tEl => {
            const tname = tEl.querySelector('.transformation-name').value.trim();
            const targs = tEl.querySelector('.transformation-args').value.trim().split(',').map(x => parseInt(x.trim())).filter(x => !isNaN(x));
            if (tname) {
                transformations.push({ name: tname, args: targs });
            }
        });
        if (transformations.length > 0) {
            dimensions.push({ transformations });
        }
    });

    return JSON.stringify({ name, dimensions }, null, 2);
}

function populateStructuredFeature(jsonOrObject) {
    const data = typeof jsonOrObject === 'string' ? JSON.parse(jsonOrObject) : jsonOrObject;

    // Set name field
    document.getElementById('in_featureName').value = data.name || '';

    // Clear any existing dimensions
    clearDimensions();

    const container = document.getElementById('dimensionsContainer');
    data.dimensions?.forEach((dim, dimIndex) => {
        addDimension(); // creates and appends a new .dimension block

        const dimEl = container.children[dimIndex];

        const transformationsDiv = dimEl.querySelector('.transformations');

        dim.transformations?.forEach(t => {
            // Add a new transformation
            const addBtn = dimEl.querySelector('button'); // the addTransformation button
            addTransformation(addBtn);

            const tEl = transformationsDiv.lastElementChild;

            // Set transformation name
            tEl.querySelector('.transformation-name').value = t.name || '';

            // Set transformation args
            tEl.querySelector('.transformation-args').value = Array.isArray(t.args)
                ? t.args.join(', ')
                : '';
        });
    });

    updateFeatureRequestPreview(); // trigger preview update
}

export async function sendStructuredFeature() {
    const requestBody = constructStructuredFeature();

    const responseCodeDiv = document.getElementById('POST_featureResponseCode');
    const responseBodyDiv = document.getElementById('POST_featureResponseBody');

    try {
        const apiBase = window.location.origin;
        const res = await requestWithLogin(`${apiBase}/feature`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: requestBody,
        });
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

export async function getFeature() {
    const name = document.getElementById('in_featureName').value.trim();
    const address = document.getElementById('GET_featureAddress').value.trim();

    const responseCodeDiv = document.getElementById('GET_featureResponseCode');
    const responseBodyDiv = document.getElementById('GET_featureResponseBody');
    if (!name) {
        alert("Feature name is required.");
        return;
    }
    const apiBase = window.location.origin;
    const url = `${apiBase}/feature/${name}${address ? `/${address}` : ''}`;
    try {
        const res = await fetch(url);
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
        populateStructuredFeature(JSON.parse(text));
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

// --------------------------------------------------------------------------
// Transformation
// --------------------------------------------------------------------------
export async function getTransformation() {
    const name = document.getElementById('in_transformationName').value.trim();
    const address = document.getElementById('GET_transformationAddress').value.trim();

    const responseCodeDiv = document.getElementById('GET_transformationResponseCode');
    const responseBodyDiv = document.getElementById('GET_transformationResponseBody');

    if (!name) {
        alert("Transformation name is required.");
        return;
    }

    const apiBase = window.location.origin;
    const url = `${apiBase}/transformation/${name}${address ? `/${address}` : ''}`;
    try {
        const res = await fetch(url);
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
        populateStructuredTransformation(JSON.parse(text));
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

export function updateTransformationPreview() {
    const name = document.getElementById('in_transformationName').value.trim();
    const sol_src = document.getElementById('POST_transformationCode').value.trim();
    const obj = { name, sol_src };
    document.getElementById('POST_transformationRequestBody').textContent = JSON.stringify(obj, null, 2);
}

export function populateStructuredTransformation(jsonOrObject) {
    const data = typeof jsonOrObject === 'string' ? JSON.parse(jsonOrObject) : jsonOrObject;

    const nameInput = document.getElementById('in_transformationName');
    const codeInput = document.getElementById('POST_transformationCode');

    if (!data || typeof data !== 'object') {
        console.warn("Invalid transformation data:", data);
        return;
    }

    nameInput.value = data.name || '';
    codeInput.value = data.sol_src || '';

    updateTransformationPreview(); // trigger live preview rendering
}

export async function sendStructuredTransformation() {
    const name = document.getElementById('in_transformationName').value.trim();
    const sol_src = document.getElementById('POST_transformationCode').value.trim();

    const responseCodeDiv = document.getElementById('POST_transformationResponseCode');
    const responseBodyDiv = document.getElementById('POST_transformationResponseBody');

    if (!name) {
        alert("Transformation name is required.");
        return;
    }

    try {
        const apiBase = window.location.origin;
        const res = await requestWithLogin(`${apiBase}/transformation`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ name, sol_src }),
        });
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

// --------------------------------------------------------------------------
// Conditions
// --------------------------------------------------------------------------
export async function getCondition() {
    const name = document.getElementById('in_conditionName').value.trim();
    const address = document.getElementById('GET_conditionAddress').value.trim();

    const responseCodeDiv = document.getElementById('GET_conditionResponseCode');
    const responseBodyDiv = document.getElementById('GET_conditionResponseBody');

    if (!name) {
        alert("Condition name is required.");
        return;
    }

    const apiBase = window.location.origin;
    const url = `${apiBase}/condition/${name}${address ? `/${address}` : ''}`;
    try {
        const res = await fetch(url);
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
        populateStructuredCondition(JSON.parse(text));
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

export function updateConditionPreview() {
    const name = document.getElementById('in_conditionName').value.trim();
    const sol_src = document.getElementById('POST_conditionCode').value.trim();
    const obj = { name, sol_src };
    document.getElementById('POST_conditionRequestBody').textContent = JSON.stringify(obj, null, 2);
}

export function populateStructuredCondition(jsonOrObject) {
    const data = typeof jsonOrObject === 'string' ? JSON.parse(jsonOrObject) : jsonOrObject;

    const nameInput = document.getElementById('in_conditionName');
    const codeInput = document.getElementById('POST_conditionCode');

    if (!data || typeof data !== 'object') {
        console.warn("Invalid condition data:", data);
        return;
    }

    nameInput.value = data.name || '';
    codeInput.value = data.sol_src || '';

    updateConditionPreview(); // trigger live preview rendering
}

export async function sendStructuredCondition() {
    const name = document.getElementById('in_conditionName').value.trim();
    const sol_src = document.getElementById('POST_conditionCode').value.trim();

    const responseCodeDiv = document.getElementById('POST_conditionResponseCode');
    const responseBodyDiv = document.getElementById('POST_conditionResponseBody');

    if (!name) {
        alert("Condition name is required.");
        return;
    }

    try {
        const apiBase = window.location.origin;
        const res = await requestWithLogin(`${apiBase}/condition`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ name, sol_src }),
        });
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

// --------------------------------------------------------------------------
// Particle
// --------------------------------------------------------------------------
function parseCsvList(value) {
    if (!value) return [];
    return value.split(',')
        .map(item => item.trim())
        .filter(item => item.length > 0);
}

function parseIntList(value) {
    if (!value) return [];
    return value.split(',')
        .map(item => item.trim())
        .filter(item => item.length > 0)
        .map(item => Number.parseInt(item, 10))
        .filter(item => !Number.isNaN(item));
}

function setParticleFeatureInfo(message, isError = false) {
    const info = document.getElementById('particleFeatureInfo');
    if (!info) return;
    info.textContent = message;
    info.style.color = isError ? '#ff8a8a' : '#888';
}

function getParticleCompositeValues() {
    const inputs = document.querySelectorAll('.particle-composite-input');
    return Array.from(inputs).map(input => input.value.trim());
}

function renderParticleCompositeInputs(count, existingValues = []) {
    const container = document.getElementById('particleCompositesContainer');
    if (!container) return;

    container.innerHTML = '';

    const safeCount = Number.isFinite(count) && count > 0 ? count : 0;
    if (safeCount === 0) {
        const empty = document.createElement('div');
        empty.style.color = '#888';
        empty.textContent = 'No dimensions to configure.';
        container.appendChild(empty);
        updateParticlePreview();
        return;
    }

    for (let i = 0; i < safeCount; i += 1) {
        const wrapper = document.createElement('div');
        wrapper.style.marginBottom = '0.5rem';

        const label = document.createElement('label');
        label.textContent = `Dimension ${i + 1} composite`;

        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'particle-composite-input';
        input.placeholder = 'leave empty for scalar';
        if (existingValues[i] !== undefined) {
            input.value = existingValues[i];
        }

        wrapper.appendChild(label);
        wrapper.appendChild(input);
        container.appendChild(wrapper);
    }

    updateParticlePreview();
}

export async function fetchParticleFeatureDimensions() {
    const feature_name = document.getElementById('in_particleFeatureName').value.trim();
    if (!feature_name) {
        alert("Feature name is required.");
        setParticleFeatureInfo("Feature name is required.", true);
        return;
    }

    setParticleFeatureInfo("Fetching feature...", false);

    const apiBase = window.location.origin;
    try {
        const res = await fetch(`${apiBase}/feature/${feature_name}`);
        if (!res.ok) {
            throw new Error(`Failed to fetch feature (${res.status})`);
        }
        const data = await res.json();
        const dims = Array.isArray(data.dimensions) ? data.dimensions.length : 0;

        const existingValues = getParticleCompositeValues();
        renderParticleCompositeInputs(dims, existingValues);

        const label = dims === 1 ? 'dimension' : 'dimensions';
        setParticleFeatureInfo(`Loaded ${dims} ${label} from feature "${data.name || feature_name}".`, false);
    } catch (error) {
        console.error(error);
        setParticleFeatureInfo(`Failed to fetch feature: ${error.message}`, true);
    }
}

function constructStructuredParticle() {
    const name = document.getElementById('in_particleName').value.trim();
    const feature_name = document.getElementById('in_particleFeatureName').value.trim();
    const composite_names = getParticleCompositeValues();
    const condition_name = document.getElementById('in_particleConditionName').value.trim();
    const condition_args = parseIntList(document.getElementById('in_particleConditionArgs').value);

    return JSON.stringify({ name, feature_name, composite_names, condition_name, condition_args }, null, 2);
}

export function updateParticlePreview() {
    const json = constructStructuredParticle();
    document.getElementById('POST_particleRequestBody').textContent = json;
}

export function populateStructuredParticle(jsonOrObject) {
    const data = typeof jsonOrObject === 'string' ? JSON.parse(jsonOrObject) : jsonOrObject;

    if (!data || typeof data !== 'object') {
        console.warn("Invalid particle data:", data);
        return;
    }

    document.getElementById('in_particleName').value = data.name || '';
    document.getElementById('in_particleFeatureName').value = data.feature_name || '';
    const compositeNames = Array.isArray(data.composite_names) ? data.composite_names : [];
    renderParticleCompositeInputs(compositeNames.length, compositeNames);
    document.getElementById('in_particleConditionName').value = data.condition_name || '';
    document.getElementById('in_particleConditionArgs').value = Array.isArray(data.condition_args)
        ? data.condition_args.join(', ')
        : '';

    updateParticlePreview();
}

export async function sendStructuredParticle() {
    const requestBody = constructStructuredParticle();

    const responseCodeDiv = document.getElementById('POST_particleResponseCode');
    const responseBodyDiv = document.getElementById('POST_particleResponseBody');

    try {
        const apiBase = window.location.origin;
        const res = await requestWithLogin(`${apiBase}/particle`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: requestBody,
        });
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

export async function getParticle() {
    const name = document.getElementById('in_particleName').value.trim();
    const address = document.getElementById('GET_particleAddress').value.trim();

    const responseCodeDiv = document.getElementById('GET_particleResponseCode');
    const responseBodyDiv = document.getElementById('GET_particleResponseBody');

    if (!name) {
        alert("Particle name is required.");
        return;
    }

    const apiBase = window.location.origin;
    const url = `${apiBase}/particle/${name}${address ? `/${address}` : ''}`;
    try {
        const res = await fetch(url);
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
        populateStructuredParticle(JSON.parse(text));
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

// --------------------------------------------------------------------------
// Version
// --------------------------------------------------------------------------
export async function fetchVersionInfo() {
    const versionDiv = document.getElementById('versionInfo');
    try {
        const res = await fetch(`${window.location.origin}/version`);
        const data = await res.json();
        versionDiv.textContent = `Version ${data.version} (Built: ${data.build_timestamp})`;
    } catch (err) {
        versionDiv.textContent = "⚠️ Failed to fetch version info";
        console.error("Version fetch failed:", err);
    }
}

// --------------------------------------------------------------------------
// Account
// --------------------------------------------------------------------------
let currentAccountPage = 0;
const accountPageSize = 10;
let totalAccountFeaturePages = 0;
let totalAccountTransformationPages = 0;
let totalAccountConditionPages = 0;
let totalAccountParticlePages = 0;

export function nextPage()
{
    const maxPages = Math.max(
        totalAccountFeaturePages,
        totalAccountTransformationPages,
        totalAccountConditionPages,
        totalAccountParticlePages
    );

    if(currentAccountPage < maxPages)
    {
        currentAccountPage += 1;
        fetchAccountResources();
    }
}

export function prevPage()
{
    if (currentAccountPage > 0) {
        currentAccountPage--;
        fetchAccountResources();
    }
}

export async function fetchAccountResources() {
    const address = document.getElementById('accountAddressInput').value.trim();
    const featuresDiv = document.getElementById('accountFeaturesList');
    const transformationsDiv = document.getElementById('accountTransformationsList');
    const conditionsDiv = document.getElementById('accountConditionsList');
    const particlesDiv = document.getElementById('accountParticlesList');

    featuresDiv.textContent = 'Loading...';
    transformationsDiv.textContent = 'Loading...';
    conditionsDiv.textContent = 'Loading...';
    particlesDiv.textContent = 'Loading...';

    if (!address) {
        alert("Address is required.");
        featuresDiv.textContent = '❌ Invalid address';
        transformationsDiv.textContent = '';
        conditionsDiv.textContent = '';
        particlesDiv.textContent = '';
        return;
    }

    try {
        const apiBase = window.location.origin;
        const res = await requestWithLogin(`${apiBase}/account/${address}?limit=${accountPageSize}&page=${currentAccountPage}`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        const data = await res.json();
        
        featuresDiv.innerHTML = data.owned_features?.length
            ? data.owned_features.map((name, i) => {
                const bg = i % 2 === 0 ? '#1e1e1e' : '#2a2a2a';
                return `<div style="padding: 0.5rem; border: 1px solid #3333; border-radius: 4px; margin-bottom: 0.3rem; background: ${bg};"><code>${name}</code></div>`;
            }).join('')
            : '(none)';

        transformationsDiv.innerHTML = data.owned_transformations?.length
            ? data.owned_transformations.map((name, i) => {
                const bg = i % 2 === 0 ? '#1e1e1e' : '#2a2a2a';
                return `<div style="padding: 0.5rem; border: 1px solid #3333; border-radius: 4px; margin-bottom: 0.3rem; background: ${bg};"><code>${name}</code></div>`;
            }).join('')
            : '(none)';

        conditionsDiv.innerHTML = data.owned_conditions?.length
            ? data.owned_conditions.map((name, i) => {
                const bg = i % 2 === 0 ? '#1e1e1e' : '#2a2a2a';
                return `<div style="padding: 0.5rem; border: 1px solid #3333; border-radius: 4px; margin-bottom: 0.3rem; background: ${bg};"><code>${name}</code></div>`;
            }).join('')
            : '(none)';

        particlesDiv.innerHTML = data.owned_particles?.length
            ? data.owned_particles.map((name, i) => {
                const bg = i % 2 === 0 ? '#1e1e1e' : '#2a2a2a';
                return `<div style="padding: 0.5rem; border: 1px solid #3333; border-radius: 4px; margin-bottom: 0.3rem; background: ${bg};"><code>${name}</code></div>`;
            }).join('')
            : '(none)';

        // Compute total pages based on backend totals
        totalAccountFeaturePages = Math.ceil((data.total_features ?? 0) / accountPageSize);
        totalAccountTransformationPages = Math.ceil((data.total_transformations ?? 0) / accountPageSize);
        totalAccountConditionPages = Math.ceil((data.total_conditions ?? 0) / accountPageSize);
        totalAccountParticlePages = Math.ceil((data.total_particles ?? 0) / accountPageSize);

        // Show page number as: Page X of Y
        const maxPages = Math.max(
            totalAccountFeaturePages,
            totalAccountTransformationPages,
            totalAccountConditionPages,
            totalAccountParticlePages
        );

        document.getElementById('accountPageLabel').textContent =
            `Page ${currentAccountPage + 1} of ${maxPages}`;

        // Disable next if on last page
        const isLastPage = currentAccountPage >= maxPages - 1;
        document.getElementById('btn_prevAccountPage').disabled = currentAccountPage === 0;
        document.getElementById('btn_nextAccountPage').disabled = isLastPage;

    } catch (err) {
        featuresDiv.textContent = '❌ Failed to fetch account data';
        transformationsDiv.textContent = err.message;
        conditionsDiv.textContent = '';
        particlesDiv.textContent = '';
    }
}

// --------------------------------------------------------------------------
// Login
// --------------------------------------------------------------------------

// MetaMask login functionality
export async function loginWithMetaMask() 
{
    const loginStatusDiv = document.getElementById('loginStatus');

    if (typeof window.ethereum === 'undefined') {
        alert("MetaMask is not installed!");
        return false;
    }

    try {
        const [address] = await window.ethereum.request({ method: 'eth_requestAccounts' });
        const apiBase = window.location.origin;
        const nonceRes = await fetch(`${apiBase}/nonce/` + address);
        const { nonce } = await nonceRes.json();

        const message = `Login nonce: ${nonce}`;
        const signature = await window.ethereum.request({
            method: 'personal_sign',
            params: [message, address],
        });

        const authRes = await fetch(`${apiBase}/auth`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ address, signature, message }),
        });

        const result = await authRes.json();

        if (result.access_token) {
            loginStatusDiv.innerHTML = `<p style="color: green;">✅ Authenticated as ${address}</p>`;
            setAccessToken(result.access_token);
            return true;
        }
        else {
            loginStatusDiv.innerHTML = `<p style="color: red;">❌ Authentication failed</p>`;
        }
    } catch (err) {
        console.error(err);
        loginStatusDiv.innerHTML = `<p style="color: red;">⚠️ Error: ${err.message}</p>`;
    }

    return false;
}
configureLoginHandler(loginWithMetaMask);
