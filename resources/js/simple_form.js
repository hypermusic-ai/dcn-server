import { setAccessToken } from "./auth";
import {
    requestWithLogin,
    formatJSON,
    configureLoginHandler,
    apiUrl,
    normalizeConnectorName,
    parseConnectorDimensions,
    getSortedBindingEntries
} from "./utils";

export { copyTextFromElement } from "./utils";

// --------------------------------------------------------------------------
// Transformation
// --------------------------------------------------------------------------
export async function getTransformation() {
    const name = document.getElementById('in_transformationName').value.trim();
    const address = document.getElementById('GET_transformationAddress').value.trim();

    const responseCodeDiv = document.getElementById('transformationResponseCode');
    const responseBodyDiv = document.getElementById('transformationResponseBody');

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
    syncTransformationFormAvailability();
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

    const responseCodeDiv = document.getElementById('transformationResponseCode');
    const responseBodyDiv = document.getElementById('transformationResponseBody');

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

    const responseCodeDiv = document.getElementById('conditionResponseCode');
    const responseBodyDiv = document.getElementById('conditionResponseBody');

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
    syncConditionFormAvailability();
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

    const responseCodeDiv = document.getElementById('conditionResponseCode');
    const responseBodyDiv = document.getElementById('conditionResponseBody');

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
let disabledFieldTooltipElement = null;
let disabledFieldTooltipInitialized = false;
let disabledFieldTooltipTarget = null;

function clearConnectorBindingCaches() {
    connectorDefinitionCache.clear();
    connectorBindingPointCache.clear();
}

function hideDisabledFieldTooltip() {
    if (!disabledFieldTooltipElement) {
        return;
    }

    disabledFieldTooltipElement.classList.remove('is-visible');
    disabledFieldTooltipTarget = null;
}

function positionDisabledFieldTooltip(clientX, clientY) {
    if (!disabledFieldTooltipElement) {
        return;
    }

    const offset = 14;
    const maxX = window.innerWidth - disabledFieldTooltipElement.offsetWidth - 12;
    const maxY = window.innerHeight - disabledFieldTooltipElement.offsetHeight - 12;
    const left = Math.max(12, Math.min(clientX + offset, maxX));
    const top = Math.max(12, Math.min(clientY + offset, maxY));

    disabledFieldTooltipElement.style.left = `${left}px`;
    disabledFieldTooltipElement.style.top = `${top}px`;
}

function showDisabledFieldTooltip(control, clientX, clientY) {
    if (!disabledFieldTooltipElement || !control) {
        return;
    }

    const reason = control.getAttribute('data-disabled-reason');
    if (!reason) {
        hideDisabledFieldTooltip();
        return;
    }

    if (disabledFieldTooltipTarget !== control || disabledFieldTooltipElement.textContent !== reason) {
        disabledFieldTooltipElement.textContent = reason;
        disabledFieldTooltipTarget = control;
    }

    disabledFieldTooltipElement.classList.add('is-visible');
    positionDisabledFieldTooltip(clientX, clientY);
}

function resolveDisabledReasonTarget(eventTarget) {
    if (!(eventTarget instanceof Element)) {
        return null;
    }

    const target = eventTarget.closest('[data-disabled-reason]');
    if (!target) {
        return null;
    }

    const disabled = (
        target instanceof HTMLInputElement ||
        target instanceof HTMLSelectElement ||
        target instanceof HTMLTextAreaElement ||
        target instanceof HTMLButtonElement
    ) && target.disabled;

    return disabled ? target : null;
}

function ensureDisabledFieldTooltip() {
    if (disabledFieldTooltipInitialized || typeof document === 'undefined') {
        return;
    }

    disabledFieldTooltipInitialized = true;

    const tooltipElement = document.createElement('div');
    tooltipElement.className = 'disabled-field-tooltip';
    document.body.appendChild(tooltipElement);
    disabledFieldTooltipElement = tooltipElement;

    document.addEventListener('mousemove', (event) => {
        const target = resolveDisabledReasonTarget(event.target);
        if (!target) {
            hideDisabledFieldTooltip();
            return;
        }

        showDisabledFieldTooltip(target, event.clientX, event.clientY);
    }, true);

    document.addEventListener('scroll', hideDisabledFieldTooltip, true);
    window.addEventListener('blur', hideDisabledFieldTooltip);
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            hideDisabledFieldTooltip();
        }
    });
}

function setControlAvailability(control, enabled, disabledReason) {
    if (!control) {
        return;
    }

    control.disabled = !enabled;
    control.classList.toggle('is-disabled-control', !enabled);

    if (enabled) {
        control.removeAttribute('data-disabled-reason');
        control.title = '';
        if (disabledFieldTooltipTarget === control) {
            hideDisabledFieldTooltip();
        }
        return;
    }

    const reason = typeof disabledReason === 'string' && disabledReason.trim().length > 0
        ? disabledReason.trim()
        : 'This field is disabled.';
    control.setAttribute('data-disabled-reason', reason);
    control.title = reason;
}

function syncTransformationFormAvailability() {
    ensureDisabledFieldTooltip();

    const nameInput = document.getElementById('in_transformationName');
    const sendButton = document.getElementById('btn_sendStructuredTransformation');
    const fetchButton = document.getElementById('btn_getTransformation');
    if (!nameInput) {
        return;
    }

    const hasName = nameInput.value.trim().length > 0;
    setControlAvailability(sendButton, hasName, 'Fill transformation name first.');
    setControlAvailability(fetchButton, hasName, 'Fill transformation name first.');
}

function syncConditionFormAvailability() {
    ensureDisabledFieldTooltip();

    const nameInput = document.getElementById('in_conditionName');
    const sendButton = document.getElementById('btn_sendStructuredCondition');
    const fetchButton = document.getElementById('btn_getCondition');
    if (!nameInput) {
        return;
    }

    const hasName = nameInput.value.trim().length > 0;
    setControlAvailability(sendButton, hasName, 'Fill condition name first.');
    setControlAvailability(fetchButton, hasName, 'Fill condition name first.');
}

export function updateExecuteActionAvailability() {
    ensureDisabledFieldTooltip();

    const nameInput = document.getElementById('executeName');
    const particleCountInput = document.getElementById('executeN');
    const fetchGraphButton = document.getElementById('btn_fetchBeforeExecute');
    const executeButton = document.getElementById('btn_execute');
    if (!nameInput || !executeButton) {
        return;
    }

    const hasName = nameInput.value.trim().length > 0;
    const hasParticleCount = Boolean(particleCountInput && particleCountInput.value.trim().length > 0);
    setControlAvailability(fetchGraphButton, hasName, 'Fill connector name first.');
    setControlAvailability(
        executeButton,
        hasName && hasParticleCount,
        hasName ? 'Fill particle count first.' : 'Fill connector name first.'
    );
}

function updateClearConnectorDimensionsAvailability() {
    const clearButton = document.getElementById('btn_clearConnectorDimensions');
    const dimensionsContainer = document.getElementById('connectorDimensionsContainer');
    if (!clearButton || !dimensionsContainer) {
        return;
    }

    const hasDimensions = dimensionsContainer.querySelector('.connector-dimension') !== null;
    setControlAvailability(clearButton, hasDimensions, 'Add at least one dimension first.');
}

function updateConnectorConditionArgsAvailability() {
    const conditionNameInput = document.getElementById('in_connectorConditionName');
    const conditionArgsInput = document.getElementById('in_connectorConditionArgs');
    if (!conditionNameInput || !conditionArgsInput) {
        return;
    }

    const hasConditionName = conditionNameInput.value.trim().length > 0;
    setControlAvailability(conditionArgsInput, hasConditionName, 'Fill condition name first.');
}

function updateBindingTargetAvailability(bindingElement) {
    if (!bindingElement) {
        return;
    }

    const slotInput = bindingElement.querySelector('.connector-binding-slot');
    const targetInput = bindingElement.querySelector('.connector-binding-target');
    if (!targetInput) {
        return;
    }

    const hasSelectedSlot = Boolean(slotInput && slotInput.value.trim().length > 0);
    setControlAvailability(targetInput, hasSelectedSlot, 'Select binding point id first.');
}

function updateTransformationArgsAvailability(transformationElement) {
    if (!transformationElement) {
        return;
    }

    const nameInput = transformationElement.querySelector('.connector-transformation-name');
    const argsInput = transformationElement.querySelector('.connector-transformation-args');
    if (!argsInput) {
        return;
    }

    const hasName = Boolean(nameInput && nameInput.value.trim().length > 0);
    setControlAvailability(argsInput, hasName, 'Fill transformation name first.');
}

function syncConnectorFormAvailability() {
    ensureDisabledFieldTooltip();
    updateClearConnectorDimensionsAvailability();
    updateConnectorConditionArgsAvailability();
    const connectorNameInput = document.getElementById('in_connectorName');
    const connectorSendButton = document.getElementById('btn_sendStructuredConnector');
    const connectorFetchButton = document.getElementById('btn_getConnector');
    const hasConnectorName = Boolean(connectorNameInput && connectorNameInput.value.trim().length > 0);
    setControlAvailability(connectorSendButton, hasConnectorName, 'Fill connector name first.');
    setControlAvailability(connectorFetchButton, hasConnectorName, 'Fill connector name first.');

    document
        .querySelectorAll('.connector-bindings > .connector-binding')
        .forEach((bindingElement) => updateBindingTargetAvailability(bindingElement));

    document
        .querySelectorAll('.connector-transformations > .connector-transformation')
        .forEach((transformationElement) => updateTransformationArgsAvailability(transformationElement));
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
        const dimensions = parseConnectorDimensions(connector);

        const labels = [];
        for (let dimId = 0; dimId < dimensions.length; dimId++) {
            const dimension = dimensions[dimId];
            const dimLabel = `${resolvedName}:${dimId}`;

            if (!dimension.composite) {
                labels.push(dimLabel);
                continue;
            }

            const childLabels = await computeOpenBindingPointLabels(dimension.composite, visiting, memo);
            const boundTargetsByChildSlot = new Map();
            getSortedBindingEntries(dimension.bindings).forEach(({ slotId, targetName }) => {
                if (slotId >= childLabels.length) {
                    throw new Error(
                        `Connector '${resolvedName}' has out-of-range binding slot ${slotId} ` +
                        `at dimension ${dimId} (child '${dimension.composite}' exports ${childLabels.length} slots)`
                    );
                }

                if (boundTargetsByChildSlot.has(slotId)) {
                    throw new Error(
                        `Connector '${resolvedName}' has duplicate canonical binding slot ${slotId} ` +
                        `at dimension ${dimId}`
                    );
                }

                boundTargetsByChildSlot.set(slotId, targetName);
            });

            // Export child slots in deterministic order.
            // If a child slot is statically bound, expand it through the binding target's exported slots.
            for (let childSlotId = 0; childSlotId < childLabels.length; childSlotId++) {
                const childLabel = childLabels[childSlotId];
                const boundTargetName = boundTargetsByChildSlot.get(childSlotId);
                if (!boundTargetName) {
                    labels.push(`${dimLabel}/${childLabel}`);
                    continue;
                }

                const boundTargetLabels = await computeOpenBindingPointLabels(boundTargetName, visiting, memo);
                for (const targetLabel of boundTargetLabels) {
                    labels.push(`${dimLabel}/${childLabel}/${targetLabel}`);
                }
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

function clearDimensionBindings(dimensionElement) {
    const bindingsContainer = dimensionElement.querySelector('.connector-bindings');
    if (!bindingsContainer) {
        return false;
    }

    if (!bindingsContainer.querySelector('.connector-binding')) {
        return false;
    }

    bindingsContainer.innerHTML = '';
    return true;
}

function updateDimensionBindingAvailability(dimensionElement, { clearBindingsWhenScalar = false } = {}) {
    const compositeInput = dimensionElement.querySelector('.connector-dimension-composite');
    const compositeName = compositeInput ? compositeInput.value.trim() : '';
    const hasComposite = compositeName.length > 0;

    const addBindingButton = dimensionElement.querySelector('.connector-binding-add');
    if (addBindingButton) {
        setControlAvailability(
            addBindingButton,
            hasComposite,
            'Fill composite connector name first.'
        );
    }

    let clearedBindings = false;
    if (!hasComposite) {
        if (clearBindingsWhenScalar) {
            clearedBindings = clearDimensionBindings(dimensionElement);
        }

        const slotSelects = dimensionElement.querySelectorAll('.connector-binding-slot');
        slotSelects.forEach((slotSelect) => {
            const preferredSlot = slotSelect.dataset.preferredSlot || slotSelect.value;
            setBindingSlotSelectOptions(slotSelect, [], preferredSlot, 'Enter composite name first');
            updateBindingTargetAvailability(slotSelect.closest('.connector-binding'));
        });

        setBindingPointStatus(
            dimensionElement,
            'Scalar dimension cannot define bindings. Set composite name to enable bindings.'
        );
    }

    if (hasComposite) {
        dimensionElement
            .querySelectorAll('.connector-bindings > .connector-binding')
            .forEach((bindingElement) => updateBindingTargetAvailability(bindingElement));
    }

    return { hasComposite, clearedBindings };
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
    const shouldDisableSlot = options.length === 0 && !hasSelectedOption;
    setControlAvailability(slotSelect, !shouldDisableSlot, emptyLabel);
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

    const { hasComposite } = updateDimensionBindingAvailability(dimensionElement, {
        clearBindingsWhenScalar: true
    });

    const slotSelects = Array.from(dimensionElement.querySelectorAll('.connector-binding-slot'));
    if (!hasComposite) {
        updateConnectorPreview();
        return;
    }

    if (slotSelects.length === 0) {
        setBindingPointStatus(dimensionElement, 'Binding points are loaded from selected composite.');
        return;
    }

    const compositeInput = dimensionElement.querySelector('.connector-dimension-composite');
    const compositeName = compositeInput ? compositeInput.value.trim() : '';

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
        const compositeName = compositeInput ? compositeInput.value.trim() : '';
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
            composite: compositeName,
            bindings: compositeName ? bindings : {},
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
    nameInput.addEventListener('input', () => {
        updateTransformationArgsAvailability(transformationElement);
        updateConnectorPreview();
    });
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

    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.className = 'connector-transformation-remove';
    removeButton.textContent = 'Remove';
    removeButton.addEventListener('click', () => {
        transformationElement.remove();
        updateConnectorPreview();
    });
    row.appendChild(removeButton);

    transformationElement.appendChild(row);
    container.appendChild(transformationElement);
    updateTransformationArgsAvailability(transformationElement);
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

    const compositeInput = dimensionElement.querySelector('.connector-dimension-composite');
    if (!compositeInput || !compositeInput.value.trim()) {
        updateDimensionBindingAvailability(dimensionElement, { clearBindingsWhenScalar: true });
        updateConnectorPreview();
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
    slotInput.addEventListener('change', () => {
        updateBindingTargetAvailability(bindingElement);
        updateConnectorPreview();
    });
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
    updateBindingTargetAvailability(bindingElement);
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

    const removeDimensionButton = document.createElement('button');
    removeDimensionButton.type = 'button';
    removeDimensionButton.className = 'connector-dimension-remove';
    removeDimensionButton.textContent = 'Remove dimension';
    removeDimensionButton.addEventListener('click', () => {
        const pendingRefreshTimeout = connectorBindingRefreshTimers.get(dimensionElement);
        if (pendingRefreshTimeout) {
            clearTimeout(pendingRefreshTimeout);
            connectorBindingRefreshTimers.delete(dimensionElement);
        }
        connectorBindingRefreshTokens.delete(dimensionElement);

        dimensionElement.remove();
        renumberConnectorDimensions();
        updateConnectorPreview();
    });

    const compositeLabel = document.createElement('label');
    compositeLabel.textContent = 'Composite connector name';
    const compositeInput = document.createElement('input');
    compositeInput.type = 'text';
    compositeInput.className = 'connector-dimension-composite';
    compositeInput.placeholder = 'leave empty for scalar';
    compositeInput.value = typeof dimension.composite === 'string' ? dimension.composite : '';
    compositeInput.addEventListener('input', () => {
        const { clearedBindings } = updateDimensionBindingAvailability(dimensionElement, {
            clearBindingsWhenScalar: true
        });
        updateConnectorPreview();
        if (clearedBindings) {
            setBindingPointStatus(
                dimensionElement,
                'Scalar dimension cannot define bindings. Existing bindings were cleared.'
            );
        }
        scheduleBindingPointRefresh(dimensionElement);
    });
    const compositeRow = document.createElement('div');
    compositeRow.className = 'connector-dimension-composite-row';
    compositeRow.appendChild(compositeInput);

    const addBindingButton = document.createElement('button');
    addBindingButton.type = 'button';
    addBindingButton.className = 'connector-binding-add';
    addBindingButton.textContent = 'Add binding';
    addBindingButton.addEventListener('click', () => addConnectorBinding(addBindingButton));
    compositeRow.appendChild(removeDimensionButton);

    fieldset.appendChild(compositeLabel);
    fieldset.appendChild(compositeRow);

    const bindingsStatus = document.createElement('div');
    bindingsStatus.className = 'connector-binding-status muted-note';
    bindingsStatus.textContent = 'Set composite name to load binding points.';
    fieldset.appendChild(bindingsStatus);

    const bindingsContainer = document.createElement('div');
    bindingsContainer.className = 'connector-bindings';
    fieldset.appendChild(bindingsContainer);
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

    updateDimensionBindingAvailability(dimensionElement, { clearBindingsWhenScalar: true });

    if (
        compositeInput.value.trim() &&
        dimension.bindings &&
        typeof dimension.bindings === 'object' &&
        !Array.isArray(dimension.bindings)
    ) {
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
    const condition_args = condition_name
        ? parseIntList(document.getElementById('in_connectorConditionArgs').value)
        : [];

    return JSON.stringify({ name, dimensions, condition_name, condition_args }, null, 2);
}

export function updateConnectorPreview() {
    syncConnectorFormAvailability();
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

    const responseCodeDiv = document.getElementById('connectorResponseCode');
    const responseBodyDiv = document.getElementById('connectorResponseBody');

    try {
        const res = await requestWithLogin(apiUrl('/connector'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: requestBody,
        });
        const text = await res.text();
        responseCodeDiv.textContent = res.status;
        responseBodyDiv.textContent = formatJSON(text);
        if (res.ok) {
            // Connector versions can change under the same name, which changes exported binding points.
            clearConnectorBindingCaches();
        }
    } catch (error) {
        responseCodeDiv.textContent = 'Error';
        responseBodyDiv.textContent = error.message;
    }
}

export async function getConnector() {
    const name = document.getElementById('in_connectorName').value.trim();
    const address = document.getElementById('GET_connectorAddress').value.trim();

    const responseCodeDiv = document.getElementById('connectorResponseCode');
    const responseBodyDiv = document.getElementById('connectorResponseBody');

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

function getAccountTotalPages() {
    const maxPages = Math.max(
        totalAccountTransformationPages,
        totalAccountConditionPages,
        totalAccountConnectorPages
    );
    return Math.max(1, maxPages);
}

function updateAccountPaginationControls(totalPages = getAccountTotalPages()) {
    const pageLabel = document.getElementById('accountPageLabel');
    const prevButton = document.getElementById('btn_prevAccountPage');
    const nextButton = document.getElementById('btn_nextAccountPage');
    if (!pageLabel || !prevButton || !nextButton) {
        return;
    }

    const safeTotalPages = Number.isInteger(totalPages) && totalPages > 0 ? totalPages : 1;
    if (currentAccountPage < 0) {
        currentAccountPage = 0;
    }
    if (currentAccountPage > safeTotalPages - 1) {
        currentAccountPage = safeTotalPages - 1;
    }

    pageLabel.textContent = `Page ${currentAccountPage + 1} of ${safeTotalPages}`;
    prevButton.disabled = currentAccountPage === 0;
    nextButton.disabled = currentAccountPage >= safeTotalPages - 1;
}

function updateAccountFetchAvailability() {
    const addressInput = document.getElementById('accountAddressInput');
    const fetchButton = document.getElementById('btn_fetchAccountData');
    if (!addressInput || !fetchButton) {
        return;
    }

    ensureDisabledFieldTooltip();
    const hasAddress = addressInput.value.trim().length > 0;
    setControlAvailability(fetchButton, hasAddress, 'Fill account address first.');
}

function initializeAccountControls() {
    const addressInput = document.getElementById('accountAddressInput');
    if (addressInput) {
        addressInput.addEventListener('input', () => {
            const hasAddress = addressInput.value.trim().length > 0;
            updateAccountFetchAvailability();
            if (!hasAddress) {
                currentAccountPage = 0;
                totalAccountTransformationPages = 0;
                totalAccountConditionPages = 0;
                totalAccountConnectorPages = 0;
                updateAccountPaginationControls(1);
            }
        });
    }

    updateAccountFetchAvailability();
    updateAccountPaginationControls(1);
}

function initializeEntityActionControls() {
    updateExecuteActionAvailability();
    syncTransformationFormAvailability();
    syncConditionFormAvailability();
    syncConnectorFormAvailability();
}

export function nextPage()
{
    const totalPages = getAccountTotalPages();

    if(currentAccountPage < totalPages - 1)
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
    const prevButton = document.getElementById('btn_prevAccountPage');
    const nextButton = document.getElementById('btn_nextAccountPage');

    transformationsDiv.textContent = 'Loading...';
    conditionsDiv.textContent = 'Loading...';
    connectorsDiv.textContent = 'Loading...';
    updateAccountFetchAvailability();
    if (prevButton) {
        prevButton.disabled = true;
    }
    if (nextButton) {
        nextButton.disabled = true;
    }

    if (!address) {
        alert("Address is required.");
        transformationsDiv.textContent = '';
        conditionsDiv.textContent = '';
        connectorsDiv.textContent = '';
        currentAccountPage = 0;
        totalAccountTransformationPages = 0;
        totalAccountConditionPages = 0;
        totalAccountConnectorPages = 0;
        updateAccountPaginationControls(1);
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

        updateAccountPaginationControls(getAccountTotalPages());

    } catch (err) {
        transformationsDiv.textContent = '❌ Failed to fetch account data';
        conditionsDiv.textContent = '';
        connectorsDiv.textContent = '';
        currentAccountPage = 0;
        totalAccountTransformationPages = 0;
        totalAccountConditionPages = 0;
        totalAccountConnectorPages = 0;
        updateAccountPaginationControls(1);
    }
}

initializeEntityActionControls();
initializeAccountControls();

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
