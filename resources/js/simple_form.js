import { setAccessToken } from "./auth";
import { requestWithLogin, formatJSON, configureLoginHandler, apiUrl } from "./utils";

export { copyTextFromElement } from "./utils";

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

    const url = apiUrl(`/transformation/${name}${address ? `/${address}` : ''}`);
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
        const res = await requestWithLogin(apiUrl('/transformation'), {
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

    const url = apiUrl(`/condition/${name}${address ? `/${address}` : ''}`);
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
        const res = await requestWithLogin(apiUrl('/condition'), {
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
// Connector
// --------------------------------------------------------------------------
function parseIntList(value) {
    if (!value) return [];
    return value.split(',')
        .map(item => item.trim())
        .filter(item => item.length > 0)
        .map(item => Number.parseInt(item, 10))
        .filter(item => !Number.isNaN(item));
}

const connectorDefinitionCache = new Map();
const connectorBindingPointCache = new Map();
const connectorBindingRefreshTimers = new WeakMap();
const connectorBindingRefreshTokens = new WeakMap();

function normalizeConnectorName(value) {
    if (typeof value !== 'string') {
        return '';
    }

    return value.trim();
}

function parseApiConnectorDimensions(connector) {
    if (!connector || typeof connector !== 'object' || !Array.isArray(connector.dimensions)) {
        return [];
    }

    return connector.dimensions.map((dimension) => {
        if (!dimension || typeof dimension !== 'object') {
            return {
                composite: '',
                bindings: {}
            };
        }

        const composite = typeof dimension.composite === 'string'
            ? dimension.composite.trim()
            : '';
        const bindings = (dimension.bindings && typeof dimension.bindings === 'object' && !Array.isArray(dimension.bindings))
            ? dimension.bindings
            : {};

        return { composite, bindings };
    });
}

function getSortedBindingEntries(bindings) {
    if (!bindings || typeof bindings !== 'object' || Array.isArray(bindings)) {
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

        const targetName = typeof targetRaw === 'string' ? targetRaw.trim() : '';
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

async function fetchConnectorDefinition(connectorName) {
    const normalizedName = normalizeConnectorName(connectorName);
    if (!normalizedName) {
        throw new Error('Connector name is empty');
    }

    if (connectorDefinitionCache.has(normalizedName)) {
        return connectorDefinitionCache.get(normalizedName);
    }

    const request = (async () => {
        const res = await fetch(apiUrl(`/connector/${encodeURIComponent(normalizedName)}`));
        if (!res.ok) {
            throw new Error(`Failed to fetch connector '${normalizedName}' (${res.status})`);
        }
        return await res.json();
    })();

    connectorDefinitionCache.set(normalizedName, request);
    try {
        return await request;
    } catch (error) {
        connectorDefinitionCache.delete(normalizedName);
        throw error;
    }
}

async function computeOpenBindingPointLabels(connectorName, visiting = new Set(), memo = new Map()) {
    const normalizedName = normalizeConnectorName(connectorName);
    if (!normalizedName) {
        return [];
    }

    if (memo.has(normalizedName)) {
        return memo.get(normalizedName);
    }

    if (visiting.has(normalizedName)) {
        throw new Error(`Connector cycle detected at '${normalizedName}'`);
    }

    visiting.add(normalizedName);
    try {
        const connector = await fetchConnectorDefinition(normalizedName);
        const resolvedName = normalizeConnectorName(connector?.name) || normalizedName;
        const dimensions = parseApiConnectorDimensions(connector);

        const labels = [];
        for (let dimId = 0; dimId < dimensions.length; dimId++) {
            const dimension = dimensions[dimId];
            const dimLabel = `${resolvedName}:${dimId}`;

            if (!dimension.composite) {
                labels.push(dimLabel);
                continue;
            }

            const childLabels = await computeOpenBindingPointLabels(dimension.composite, visiting, memo);
            const staticBindings = new Set();
            getSortedBindingEntries(dimension.bindings).forEach(({ slotId }) => {
                if (slotId < childLabels.length && !staticBindings.has(slotId)) {
                    staticBindings.add(slotId);
                }
            });

            for (let childSlotId = 0; childSlotId < childLabels.length; childSlotId++) {
                if (staticBindings.has(childSlotId)) {
                    continue;
                }

                labels.push(`${dimLabel}/${childLabels[childSlotId]}`);
            }
        }

        memo.set(normalizedName, labels);
        return labels;
    } finally {
        visiting.delete(normalizedName);
    }
}

async function fetchBindingPointOptions(connectorName) {
    const normalizedName = normalizeConnectorName(connectorName);
    if (!normalizedName) {
        return [];
    }

    if (connectorBindingPointCache.has(normalizedName)) {
        return connectorBindingPointCache.get(normalizedName);
    }

    const request = (async () => {
        const labels = await computeOpenBindingPointLabels(normalizedName, new Set(), new Map());
        return labels.map((label, slotId) => ({ slotId, label }));
    })();

    connectorBindingPointCache.set(normalizedName, request);
    try {
        return await request;
    } catch (error) {
        connectorBindingPointCache.delete(normalizedName);
        throw error;
    }
}

function setBindingPointStatus(dimensionElement, message) {
    const statusElement = dimensionElement.querySelector('.connector-binding-status');
    if (!statusElement) {
        return;
    }

    statusElement.textContent = message;
}

function setBindingSlotSelectOptions(slotSelect, options, selectedSlot = '', emptyLabel = 'Select binding point') {
    const normalizedSelectedSlot = typeof selectedSlot === 'string' ? selectedSlot.trim() : '';

    slotSelect.innerHTML = '';

    const placeholderOption = document.createElement('option');
    placeholderOption.value = '';
    placeholderOption.textContent = emptyLabel;
    slotSelect.appendChild(placeholderOption);

    let hasSelectedOption = false;
    options.forEach(({ slotId, label }) => {
        const option = document.createElement('option');
        option.value = String(slotId);
        option.textContent = `${slotId} - ${label}`;
        slotSelect.appendChild(option);

        if (option.value === normalizedSelectedSlot) {
            hasSelectedOption = true;
        }
    });

    if (normalizedSelectedSlot && !hasSelectedOption) {
        const unresolvedOption = document.createElement('option');
        unresolvedOption.value = normalizedSelectedSlot;
        unresolvedOption.textContent = `${normalizedSelectedSlot} - unresolved`;
        slotSelect.appendChild(unresolvedOption);
        hasSelectedOption = true;
    }

    slotSelect.value = hasSelectedOption ? normalizedSelectedSlot : '';
    slotSelect.disabled = options.length === 0 && !hasSelectedOption;
}

function scheduleBindingPointRefresh(dimensionElement, immediate = false) {
    const previousTimeout = connectorBindingRefreshTimers.get(dimensionElement);
    if (previousTimeout) {
        clearTimeout(previousTimeout);
    }

    const runRefresh = () => {
        connectorBindingRefreshTimers.delete(dimensionElement);
        void refreshDimensionBindingPoints(dimensionElement);
    };

    if (immediate) {
        runRefresh();
        return;
    }

    const timeoutHandle = setTimeout(runRefresh, 250);
    connectorBindingRefreshTimers.set(dimensionElement, timeoutHandle);
}

async function refreshDimensionBindingPoints(dimensionElement) {
    const token = (connectorBindingRefreshTokens.get(dimensionElement) || 0) + 1;
    connectorBindingRefreshTokens.set(dimensionElement, token);

    const slotSelects = Array.from(dimensionElement.querySelectorAll('.connector-binding-slot'));
    if (slotSelects.length === 0) {
        setBindingPointStatus(dimensionElement, 'Binding points are loaded from selected composite.');
        return;
    }

    const compositeInput = dimensionElement.querySelector('.connector-dimension-composite');
    const compositeName = compositeInput ? compositeInput.value.trim() : '';

    if (!compositeName) {
        slotSelects.forEach((slotSelect) => {
            const preferredSlot = slotSelect.dataset.preferredSlot || slotSelect.value;
            setBindingSlotSelectOptions(slotSelect, [], preferredSlot, 'Enter composite name first');
        });
        setBindingPointStatus(dimensionElement, 'Set composite name to load binding points.');
        updateConnectorPreview();
        return;
    }

    slotSelects.forEach((slotSelect) => {
        const preferredSlot = slotSelect.dataset.preferredSlot || slotSelect.value;
        setBindingSlotSelectOptions(slotSelect, [], preferredSlot, 'Loading binding points...');
    });
    setBindingPointStatus(dimensionElement, `Loading binding points from '${compositeName}'...`);

    try {
        const options = await fetchBindingPointOptions(compositeName);
        if (connectorBindingRefreshTokens.get(dimensionElement) !== token) {
            return;
        }

        slotSelects.forEach((slotSelect) => {
            const preferredSlot = slotSelect.dataset.preferredSlot || slotSelect.value;
            setBindingSlotSelectOptions(
                slotSelect,
                options,
                preferredSlot,
                options.length === 0 ? 'No open binding points' : 'Select binding point'
            );
            delete slotSelect.dataset.preferredSlot;
        });

        if (options.length === 0) {
            setBindingPointStatus(dimensionElement, `No open binding points exported by '${compositeName}'.`);
        } else {
            setBindingPointStatus(dimensionElement, `${options.length} binding point(s) loaded from '${compositeName}'.`);
        }
    } catch (error) {
        if (connectorBindingRefreshTokens.get(dimensionElement) !== token) {
            return;
        }

        slotSelects.forEach((slotSelect) => {
            const preferredSlot = slotSelect.dataset.preferredSlot || slotSelect.value;
            setBindingSlotSelectOptions(slotSelect, [], preferredSlot, 'Failed to load binding points');
        });
        setBindingPointStatus(dimensionElement, error?.message || 'Failed to load binding points.');
    }

    updateConnectorPreview();
}

function parseConnectorDimensionsFromForm() {
    const dimensions = [];
    document.querySelectorAll('.connector-dimension').forEach((dimensionElement) => {
        const compositeInput = dimensionElement.querySelector('.connector-dimension-composite');
        const bindings = {};
        dimensionElement.querySelectorAll('.connector-bindings > .connector-binding').forEach((bindingElement) => {
            const slotInput = bindingElement.querySelector('.connector-binding-slot');
            const targetInput = bindingElement.querySelector('.connector-binding-target');

            const slot = slotInput ? slotInput.value.trim() : '';
            if (!slot) {
                return;
            }

            const target = targetInput ? targetInput.value.trim() : '';
            if (!target) {
                return;
            }
            bindings[slot] = target;
        });

        const transformations = [];
        dimensionElement.querySelectorAll('.connector-transformations > .connector-transformation').forEach((transformationElement) => {
            const transformationNameInput = transformationElement.querySelector('.connector-transformation-name');
            const transformationArgsInput = transformationElement.querySelector('.connector-transformation-args');
            const transformationName = transformationNameInput ? transformationNameInput.value.trim() : '';
            if (!transformationName) {
                return;
            }

            const transformationArgs = parseIntList(transformationArgsInput ? transformationArgsInput.value : '');
            transformations.push({ name: transformationName, args: transformationArgs });
        });
        dimensions.push({
            composite: compositeInput ? compositeInput.value.trim() : '',
            bindings,
            transformations
        });
    });
    return dimensions;
}

function renumberConnectorDimensions() {
    const legends = document.querySelectorAll('.connector-dimension .dimension-fieldset > legend');
    legends.forEach((legend, index) => {
        legend.textContent = `Dimension ${index + 1}`;
    });
}

function addConnectorTransformation(addButton, transformation = {}) {
    const fieldset = addButton.closest('.dimension-fieldset');
    const container = fieldset ? fieldset.querySelector('.connector-transformations') : null;
    if (!container) {
        return;
    }

    const transformationElement = document.createElement('div');
    transformationElement.className = 'connector-transformation';

    const separator = document.createElement('hr');
    transformationElement.appendChild(separator);

    const row = document.createElement('div');
    row.className = 'transformation-row';

    const nameColumn = document.createElement('div');
    nameColumn.className = 'transformation-col';
    const nameLabel = document.createElement('label');
    nameLabel.textContent = 'Transformation name';
    const nameInput = document.createElement('input');
    nameInput.type = 'text';
    nameInput.className = 'connector-transformation-name';
    nameInput.placeholder = 'add';
    nameInput.value = typeof transformation.name === 'string' ? transformation.name : '';
    nameInput.addEventListener('input', updateConnectorPreview);
    nameColumn.appendChild(nameLabel);
    nameColumn.appendChild(nameInput);

    const argsColumn = document.createElement('div');
    argsColumn.className = 'transformation-col';
    const argsLabel = document.createElement('label');
    argsLabel.textContent = 'Args (comma-separated)';
    const argsInput = document.createElement('input');
    argsInput.type = 'text';
    argsInput.className = 'connector-transformation-args';
    argsInput.placeholder = '...';
    argsInput.value = Array.isArray(transformation.args) ? transformation.args.join(', ') : '';
    argsInput.addEventListener('input', updateConnectorPreview);
    argsColumn.appendChild(argsLabel);
    argsColumn.appendChild(argsInput);

    row.appendChild(nameColumn);
    row.appendChild(argsColumn);
    transformationElement.appendChild(row);
    container.appendChild(transformationElement);
    updateConnectorPreview();
}

function addConnectorBinding(addButton, binding = {}) {
    const fieldset = addButton.closest('.dimension-fieldset');
    const container = fieldset ? fieldset.querySelector('.connector-bindings') : null;
    if (!container) {
        return;
    }
    const dimensionElement = addButton.closest('.connector-dimension');
    if (!dimensionElement) {
        return;
    }

    const bindingElement = document.createElement('div');
    bindingElement.className = 'connector-binding';

    const row = document.createElement('div');
    row.className = 'binding-row';

    const slotColumn = document.createElement('div');
    slotColumn.className = 'binding-col';
    const slotLabel = document.createElement('label');
    slotLabel.textContent = 'Binding point id';
    const slotInput = document.createElement('select');
    slotInput.className = 'connector-binding-slot';
    const initialSlot = typeof binding.slot === 'string' ? binding.slot.trim() : '';
    if (initialSlot) {
        slotInput.dataset.preferredSlot = initialSlot;
    }
    setBindingSlotSelectOptions(slotInput, [], initialSlot, 'Enter composite name first');
    slotInput.addEventListener('change', updateConnectorPreview);
    slotColumn.appendChild(slotLabel);
    slotColumn.appendChild(slotInput);

    const targetColumn = document.createElement('div');
    targetColumn.className = 'binding-col';
    const targetLabel = document.createElement('label');
    targetLabel.textContent = 'Composite connector';
    const targetInput = document.createElement('input');
    targetInput.type = 'text';
    targetInput.className = 'connector-binding-target';
    targetInput.placeholder = 'TIME';
    targetInput.value = typeof binding.target === 'string' ? binding.target : '';
    targetInput.addEventListener('input', updateConnectorPreview);
    targetColumn.appendChild(targetLabel);
    targetColumn.appendChild(targetInput);

    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.className = 'connector-binding-remove';
    removeButton.textContent = 'Remove';
    removeButton.addEventListener('click', () => {
        bindingElement.remove();
        updateConnectorPreview();
    });

    row.appendChild(slotColumn);
    row.appendChild(targetColumn);
    row.appendChild(removeButton);
    bindingElement.appendChild(row);

    container.appendChild(bindingElement);
    scheduleBindingPointRefresh(dimensionElement, true);
    updateConnectorPreview();
}

function appendConnectorDimension(dimension = {}) {
    const container = document.getElementById('connectorDimensionsContainer');
    if (!container) {
        return;
    }

    const dimensionElement = document.createElement('div');
    dimensionElement.className = 'connector-dimension';

    const fieldset = document.createElement('fieldset');
    fieldset.className = 'dimension-fieldset';
    const legend = document.createElement('legend');
    fieldset.appendChild(legend);

    const compositeLabel = document.createElement('label');
    compositeLabel.textContent = 'Composite connector name';
    const compositeInput = document.createElement('input');
    compositeInput.type = 'text';
    compositeInput.className = 'connector-dimension-composite';
    compositeInput.placeholder = 'leave empty for scalar';
    compositeInput.value = typeof dimension.composite === 'string' ? dimension.composite : '';
    compositeInput.addEventListener('input', () => {
        updateConnectorPreview();
        scheduleBindingPointRefresh(dimensionElement);
    });
    fieldset.appendChild(compositeLabel);
    fieldset.appendChild(compositeInput);

    const bindingsContainer = document.createElement('div');
    bindingsContainer.className = 'connector-bindings';
    fieldset.appendChild(bindingsContainer);

    const bindingsStatus = document.createElement('div');
    bindingsStatus.className = 'connector-binding-status muted-note';
    bindingsStatus.textContent = 'Set composite name to load binding points.';
    fieldset.appendChild(bindingsStatus);

    const addBindingButton = document.createElement('button');
    addBindingButton.type = 'button';
    addBindingButton.textContent = 'Add binding';
    addBindingButton.addEventListener('click', () => addConnectorBinding(addBindingButton));
    fieldset.appendChild(addBindingButton);

    const transformationsContainer = document.createElement('div');
    transformationsContainer.className = 'connector-transformations';
    fieldset.appendChild(transformationsContainer);

    const addTransformationButton = document.createElement('button');
    addTransformationButton.type = 'button';
    addTransformationButton.textContent = 'Add transformation';
    addTransformationButton.addEventListener('click', () => addConnectorTransformation(addTransformationButton));
    fieldset.appendChild(addTransformationButton);

    dimensionElement.appendChild(fieldset);
    container.appendChild(dimensionElement);

    if (dimension.bindings && typeof dimension.bindings === 'object' && !Array.isArray(dimension.bindings)) {
        Object.entries(dimension.bindings).forEach(([slot, target]) => {
            addConnectorBinding(addBindingButton, {
                slot,
                target: typeof target === 'string' ? target : ''
            });
        });
    }

    if (Array.isArray(dimension.transformations)) {
        dimension.transformations.forEach((transformation) => {
            addConnectorTransformation(addTransformationButton, transformation);
        });
    }

    scheduleBindingPointRefresh(dimensionElement, true);
    renumberConnectorDimensions();
}

export function addConnectorDimension() {
    appendConnectorDimension();
    updateConnectorPreview();
}

export function clearConnectorDimensions() {
    const container = document.getElementById('connectorDimensionsContainer');
    if (!container) {
        return;
    }
    container.innerHTML = '';
    updateConnectorPreview();
}

function constructStructuredConnector() {
    const name = document.getElementById('in_connectorName').value.trim();
    const dimensions = parseConnectorDimensionsFromForm();
    const condition_name = document.getElementById('in_connectorConditionName').value.trim();
    const condition_args = parseIntList(document.getElementById('in_connectorConditionArgs').value);

    return JSON.stringify({ name, dimensions, condition_name, condition_args }, null, 2);
}

export function updateConnectorPreview() {
    const json = constructStructuredConnector();
    document.getElementById('POST_connectorRequestBody').textContent = json;
}

export function populateStructuredConnector(jsonOrObject) {
    const data = typeof jsonOrObject === 'string' ? JSON.parse(jsonOrObject) : jsonOrObject;

    if (!data || typeof data !== 'object') {
        console.warn("Invalid connector data:", data);
        return;
    }

    document.getElementById('in_connectorName').value = data.name || '';

    clearConnectorDimensions();
    const dimensions = Array.isArray(data.dimensions) ? data.dimensions : [];
    dimensions.forEach((dimension) => appendConnectorDimension(dimension));

    document.getElementById('in_connectorConditionName').value = data.condition_name || '';
    document.getElementById('in_connectorConditionArgs').value = Array.isArray(data.condition_args)
        ? data.condition_args.join(', ')
        : '';

    updateConnectorPreview();
}

export async function sendStructuredConnector() {
    const requestBody = constructStructuredConnector();

    const responseCodeDiv = document.getElementById('POST_connectorResponseCode');
    const responseBodyDiv = document.getElementById('POST_connectorResponseBody');

    try {
        const res = await requestWithLogin(apiUrl('/connector'), {
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

export async function getConnector() {
    const name = document.getElementById('in_connectorName').value.trim();
    const address = document.getElementById('GET_connectorAddress').value.trim();

    const responseCodeDiv = document.getElementById('GET_connectorResponseCode');
    const responseBodyDiv = document.getElementById('GET_connectorResponseBody');

    if (!name) {
        alert("Connector name is required.");
        return;
    }

    const url = apiUrl(`/connector/${name}${address ? `/${address}` : ''}`);
    try {
        const res = await fetch(url);
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
        populateStructuredConnector(JSON.parse(text));
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
        const res = await fetch(apiUrl('/version'));
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
let totalAccountTransformationPages = 0;
let totalAccountConditionPages = 0;
let totalAccountConnectorPages = 0;

export function nextPage()
{
    const maxPages = Math.max(
        totalAccountTransformationPages,
        totalAccountConditionPages,
        totalAccountConnectorPages
    );

    if(currentAccountPage < maxPages - 1)
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
    const transformationsDiv = document.getElementById('accountTransformationsList');
    const conditionsDiv = document.getElementById('accountConditionsList');
    const connectorsDiv = document.getElementById('accountConnectorsList');

    transformationsDiv.textContent = 'Loading...';
    conditionsDiv.textContent = 'Loading...';
    connectorsDiv.textContent = 'Loading...';

    if (!address) {
        alert("Address is required.");
        transformationsDiv.textContent = '';
        conditionsDiv.textContent = '';
        connectorsDiv.textContent = '';
        return;
    }

    try {
        const res = await requestWithLogin(apiUrl(`/account/${address}?limit=${accountPageSize}&page=${currentAccountPage}`), {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        const data = await res.json();

        transformationsDiv.innerHTML = data.owned_transformations?.length
            ? data.owned_transformations.map((name) => `<div class="account-item"><code>${name}</code></div>`).join('')
            : '(none)';

        conditionsDiv.innerHTML = data.owned_conditions?.length
            ? data.owned_conditions.map((name) => `<div class="account-item"><code>${name}</code></div>`).join('')
            : '(none)';

        connectorsDiv.innerHTML = data.owned_connectors?.length
            ? data.owned_connectors.map((name) => `<div class="account-item"><code>${name}</code></div>`).join('')
            : '(none)';

        // Compute total pages based on backend totals
        totalAccountTransformationPages = Math.ceil((data.total_transformations ?? 0) / accountPageSize);
        totalAccountConditionPages = Math.ceil((data.total_conditions ?? 0) / accountPageSize);
        totalAccountConnectorPages = Math.ceil((data.total_connectors ?? 0) / accountPageSize);

        // Show page number as: Page X of Y
        const maxPages = Math.max(
            totalAccountTransformationPages,
            totalAccountConditionPages,
            totalAccountConnectorPages
        );
        const totalPages = Math.max(1, maxPages);

        document.getElementById('accountPageLabel').textContent =
            `Page ${Math.min(currentAccountPage + 1, totalPages)} of ${totalPages}`;

        // Disable next if on last page
        const isLastPage = currentAccountPage >= totalPages - 1;
        document.getElementById('btn_prevAccountPage').disabled = currentAccountPage === 0;
        document.getElementById('btn_nextAccountPage').disabled = isLastPage;

    } catch (err) {
        transformationsDiv.textContent = '❌ Failed to fetch account data';
        conditionsDiv.textContent = '';
        connectorsDiv.textContent = '';
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
        const nonceRes = await fetch(apiUrl(`/nonce/${address}`));
        const { nonce } = await nonceRes.json();

        const message = `Login nonce: ${nonce}`;
        const signature = await window.ethereum.request({
            method: 'personal_sign',
            params: [message, address],
        });

        const authRes = await fetch(apiUrl('/auth'), {
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
