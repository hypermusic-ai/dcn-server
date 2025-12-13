import { setAccessToken, getAccessToken } from "./auth";

let rootFeatureName;

// --------------------------------------------------------------------------
// Running Instances
// --------------------------------------------------------------------------
let runningInstanceData = [];
let currentNodeEditing = null;

function openRIPopup(nodeId, nodeData) {
    currentNodeEditing = nodeId;
    document.getElementById("popupFeatureLabel").textContent = nodeData.name;
    const existing = runningInstanceData[nodeId] || [0, 0];
    document.getElementById("popupStartPoint").value = existing[0];
    document.getElementById("popupTransformShift").value = existing[1];
    document.getElementById("popupInstanceEditor").style.display = 'block';
}

export function closePopup() {
    document.getElementById("popupInstanceEditor").style.display = 'none';
    currentNodeEditing = null;
}

export function saveInstanceEdit() {
    const a = parseInt(document.getElementById("popupStartPoint").value);
    const b = parseInt(document.getElementById("popupTransformShift").value);
    if (!isNaN(a) && !isNaN(b)) {
        runningInstanceData[currentNodeEditing] = [a, b];
        updateRunningInstanceList();
    }
    closePopup();
}

function updateRunningInstanceList() {
    const jsonString = JSON.stringify({
        running_instances: runningInstanceData.map(([s, t]) => ({ start_point: s, transformation_shift: t }))
    }, null, 2);

    document.getElementById('executeRunningInstances').textContent = jsonString;
}

function clearRunningInstances() {
    runningInstanceData = [];
    updateRunningInstanceList();
}

// --------------------------------------------------------------------------
// Execute
// --------------------------------------------------------------------------
function drawFeatureTree(treeData) {
    const nodes = [];
    const edges = [];

    treeData.forEach((item, index) => {
        nodes.push({
            id: item.id,
            label: item.name.split('/').pop(),
            title: `${item.name}\n${item.scalar ? 'Scalar' : 'Composite'}`,
            color: item.scalar ? '#1e90ff' : '#ffffff',
            font: { color: item.scalar ? '#fff' : '#000' },
            shape: item.scalar ? 'ellipse' : 'box'
        });

        if (item.parent !== -1) {
            edges.push({ from: item.parent, to: item.id });
        }
    });

    const container = document.getElementById('treeContainer');
    const data = { nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) };
    const options = {
        layout: { hierarchical: { direction: 'UD', sortMethod: 'directed' } },
        physics: false,
        edges: { color: '#aaa' },
        interaction: { hover: true }
    };

    const network = new vis.Network(container, data, options);

    network.on("click", function (params) {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            const nodeData = treeData.find(n => n.id === nodeId);
            if (nodeData) {
                openRIPopup(nodeId, nodeData);
            }
        }
    });

    container.style.display = 'block';
    network.fit();
}

async function fetchFeatureDepthFirst(rootFeatureName) {
    clearRunningInstances();

    let nodeIdCounter = 0;
    const compositeList = [];
    const apiBase = window.location.origin;
    const stack = [{
        parent: -1,
        path: '',
        name: rootFeatureName,
        assignId: () => nodeIdCounter++
    }];

    while (stack.length > 0) {
        const { parent, path, name, assignId } = stack.pop();
        const id = assignId();

        try {
            const res = await fetch(`${apiBase}/feature/${name}`);
            if (!res.ok) throw new Error(`Failed to fetch ${name}`);
            const feature = await res.json();

            const fullPath = `${path}/${feature.name}`;
            const scalar = feature.dimensions.length === 0;

            compositeList.push({ parent, id, name: fullPath, scalar });
            runningInstanceData[id] = [0, 0];

            // Push children in reverse so they’re visited left-to-right
            for (let i = feature.dimensions.length - 1; i >= 0; i--) {
                const dim = feature.dimensions[i];
                stack.push({
                    parent: id,
                    path: fullPath,
                    name: dim.feature_name,
                    assignId: () => nodeIdCounter++
                });
            }
        } catch (e) {
            console.error(`Error fetching ${name}, ${e.message}`);
            throw e;
        }
    }

    updateRunningInstanceList();

    return compositeList;
}

export async function fetchBeforeExecute() {
    rootFeatureName = document.getElementById('executeName').value.trim();
    if (!rootFeatureName) {
        alert("Please provide a feature name.");
        return;
    }

    try{
        const features = await fetchFeatureDepthFirst(rootFeatureName);
        drawFeatureTree(features);
    }catch(e){
        console.error(e);

        const container = document.getElementById('treeContainer');
        container.style.display = 'block';
        container.innerHTML = `<p>Failed to fetch features. ${e.message} </p>`;
    }
}

export async function execute() {
    const feature_name = document.getElementById('executeName').value.trim();
    const n = document.getElementById('executeN').value.trim();
    const running_instances = runningInstanceData.map(([start_point, transformation_shift]) => ({ start_point, transformation_shift }));

    const codeDiv = document.getElementById('executeCode');
    const bodyDiv = document.getElementById('executeBody');
    if (!feature_name) {
        alert("Contract name is required.");
        return;
    }
    const apiBase = window.location.origin;
    try {
        const res = await requestWithLogin(`${apiBase}/execute`,
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ feature_name, n, running_instances }),
            }
        );
        const text = await res.text();
        codeDiv.textContent = res.status;
        bodyDiv.textContent = formatJSON(text);
    } catch (error) {
        codeDiv.textContent = 'Error';
        bodyDiv.textContent = error.message;
    }
}

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

    // Create row container
    const row = document.createElement('div');
    row.style.display = 'flex';
    row.style.gap = '1rem';
    row.style.alignItems = 'flex-start';

    // Column feature name
    const nameCol = document.createElement('div');
    nameCol.style.flex = '1';
    nameCol.style.marginLeft = '1rem';
    nameCol.style.marginRight = '1rem';

    const label = document.createElement('label');
    label.textContent = 'Feature Name';

    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'dimension-feature-name';
    input.placeholder = 'pitch';
    input.addEventListener('input', updateFeatureRequestPreview);

    nameCol.appendChild(label);
    nameCol.appendChild(input);

    const transformationsDiv = document.createElement('div');
    transformationsDiv.className = 'transformations';

    const addBtn = document.createElement('button');
    addBtn.type = 'button';
    addBtn.textContent = '➕ Add transformation';
    addBtn.addEventListener('click', () => addTransformation(addBtn));

    fieldset.appendChild(legend);

    // Assemble row
    row.appendChild(nameCol);
    fieldset.appendChild(row);

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
        const feature_name = dimEl.querySelector('.dimension-feature-name').value.trim();
        const transformations = [];
        dimEl.querySelectorAll('.transformations > div').forEach(tEl => {
            const tname = tEl.querySelector('.transformation-name').value.trim();
            const targs = tEl.querySelector('.transformation-args').value.trim().split(',').map(x => parseInt(x.trim())).filter(x => !isNaN(x));
            if (tname) {
                transformations.push({ name: tname, args: targs });
            }
        });
        if (feature_name) {
            dimensions.push({ feature_name, transformations });
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

        // Set feature name
        dimEl.querySelector('.dimension-feature-name').value = dim.feature_name || '';

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
    const requestBodyDiv = document.getElementById('POST_transformationRequestBody');

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

export function nextPage()
{
    if(currentAccountPage < Math.max(totalAccountFeaturePages, totalAccountTransformationPages))
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

    featuresDiv.textContent = 'Loading...';
    transformationsDiv.textContent = 'Loading...';

    if (!address) {
        alert("Address is required.");
        featuresDiv.textContent = '❌ Invalid address';
        transformationsDiv.textContent = '';
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

        // Compute total pages based on backend totals
        totalAccountFeaturePages = Math.ceil((data.total_features ?? 0) / accountPageSize);
        totalAccountTransformationPages = Math.ceil((data.total_transformations ?? 0) / accountPageSize);

        // Show page number as: Page X of Y
        document.getElementById('accountPageLabel').textContent =
            `Page ${currentAccountPage + 1} of ${Math.max(totalAccountFeaturePages, totalAccountTransformationPages)}`;

        // Disable next if on last page
        const isLastPage = currentAccountPage >= Math.max(totalAccountFeaturePages, totalAccountTransformationPages) - 1;
        document.getElementById('btn_prevAccountPage').disabled = currentAccountPage === 0;
        document.getElementById('btn_nextAccountPage').disabled = isLastPage;

    } catch (err) {
        featuresDiv.textContent = '❌ Failed to fetch account data';
        transformationsDiv.textContent = err.message;
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

// --------------------------------------------------------------------------
// Utils
// --------------------------------------------------------------------------

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
function formatJSON(text) {
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
  if (res.status !== 401) {
    return res;
  }

  // 401 → try login once more
  if (!await loginWithMetaMask()) {
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