import { requestWithLogin, formatJSON, apiUrl } from "./utils";

let rootExecuteName;

function normalizeConnectorName(rawName) {
    if (typeof rawName !== 'string') {
        return '';
    }

    const trimmed = rawName.trim();
    if (!trimmed) {
        return '';
    }

    const pathNormalized = trimmed.replace(/\\/g, '/');
    let candidate = pathNormalized.split('/').pop() || pathNormalized;
    if (candidate.toLowerCase().endsWith('.json')) {
        candidate = candidate.slice(0, -5);
    }

    return candidate.trim();
}

// --------------------------------------------------------------------------
// Running Instances
// --------------------------------------------------------------------------
let runningInstanceData = [];
let currentNodeEditing = null;

function openRIPopup(nodeId, nodeData) {
    currentNodeEditing = nodeId;
    document.getElementById("popupRunningInstanceLabel").textContent = nodeData.name;
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
function drawExecuteTree(treeData) {
    const nodes = [];
    const edges = [];

    treeData.forEach((item) => {
        const isScalar = item.scalar === true;
        const isBoundConnector = item.bound === true && !isScalar;
        const nodeSegment = item.name.split('/').pop();
        const nodeType = isScalar
            ? 'Scalar'
            : (isBoundConnector ? `Bound connector (${item.boundKind || 'binding'})` : 'Connector');
        const titleSuffix = item.boundSlotLabel ? `\n${item.boundSlotLabel}` : '';

        nodes.push({
            id: item.id,
            label: isScalar ? '' : nodeSegment,
            title: `${item.name}\n${nodeType}${titleSuffix}`,
            color: isScalar
                ? {
                    background: '#fafcff',
                    border: '#cdd7e3',
                    highlight: { background: '#f4f8fd', border: '#b9c6d5' }
                }
                : (isBoundConnector
                    ? {
                        background: '#fff4d6',
                        border: '#c97500',
                        highlight: { background: '#ffe9b3', border: '#a85f00' }
                    }
                    : {
                        background: '#ffffff',
                        border: '#444444',
                        highlight: { background: '#ffffff', border: '#111111' }
                    }),
            borderWidth: isScalar ? 1 : (isBoundConnector ? 2 : 1),
            shapeProperties: { borderDashes: isScalar ? [3, 5] : false },
            size: isScalar ? 14 : undefined,
            font: isScalar ? { size: 1, color: 'rgba(0,0,0,0)' } : { color: '#000' },
            shape: isScalar ? 'circle' : 'box'
        });

        if (item.parent !== -1) {
            const isBindingEdge = isBoundConnector;
            const edgeLabel = isScalar
                ? nodeSegment
                : (isBindingEdge ? `binding • ${nodeSegment}` : undefined);
            edges.push({
                from: item.parent,
                to: item.id,
                dashes: isBindingEdge,
                color: isBindingEdge ? { color: '#c97500' } : { color: '#aaa' },
                label: edgeLabel,
                font: isBindingEdge
                    ? { align: 'middle', size: 10, color: '#a85f00', background: '#fff8e8' }
                    : (isScalar ? { align: 'middle', size: 10, color: '#7b8794', strokeWidth: 0 } : undefined),
                title: isScalar ? item.name : undefined
            });
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

function getConnectorDimensions(connector) {
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

async function fetchConnectorDepthFirst(rootName) {
    clearRunningInstances();

    let nodeIdCounter = 0;
    const childList = [];
    const connectorCache = new Map();
    const openSlotsCache = new Map();

    async function fetchConnectorByName(name) {
        if (connectorCache.has(name)) {
            return connectorCache.get(name);
        }

        const res = await fetch(apiUrl(`/connector/${encodeURIComponent(name)}`));
        if (!res.ok) {
            throw new Error(`Failed to fetch connector '${name}'`);
        }

        const connector = await res.json();
        connectorCache.set(name, connector);
        return connector;
    }

    async function computeOpenSlots(name, visiting = new Set()) {
        if (openSlotsCache.has(name)) {
            return openSlotsCache.get(name);
        }

        if (visiting.has(name)) {
            throw new Error(`Connector cycle detected at '${name}'`);
        }

        visiting.add(name);

        const connector = await fetchConnectorByName(name);
        const dimensions = getConnectorDimensions(connector);

        let openSlots = 0;
        for (const dimension of dimensions) {
            if (!dimension.composite) {
                openSlots += 1;
                continue;
            }

            const childOpenSlots = await computeOpenSlots(dimension.composite, visiting);
            openSlots += childOpenSlots;

            const staticBindings = new Map();
            getSortedBindingEntries(dimension.bindings).forEach(({ slotId, targetName }) => {
                if (slotId < childOpenSlots && !staticBindings.has(slotId)) {
                    staticBindings.set(slotId, targetName);
                }
            });
            openSlots -= staticBindings.size;
        }

        visiting.delete(name);
        openSlotsCache.set(name, openSlots);
        return openSlots;
    }

    async function expandConnector({
        connectorName,
        parentId,
        path,
        incomingBindings,
        boundDescriptor
    }) {
        const connector = await fetchConnectorByName(connectorName);
        const resolvedName = typeof connector.name === 'string' && connector.name.trim()
            ? connector.name.trim()
            : connectorName;
        const fullPath = `${path}/${resolvedName}`;
        const id = nodeIdCounter++;

        childList.push({
            parent: parentId,
            id,
            name: fullPath,
            scalar: false,
            bound: Boolean(boundDescriptor),
            boundKind: boundDescriptor ? boundDescriptor.kind : '',
            boundSlotLabel: boundDescriptor ? boundDescriptor.slotLabel : ''
        });
        runningInstanceData[id] = [0, 0];

        const dimensions = getConnectorDimensions(connector);
        let openSlotId = 0;

        for (let dimId = 0; dimId < dimensions.length; dimId++) {
            const dimension = dimensions[dimId];

            if (!dimension.composite) {
                const replacement = incomingBindings.get(openSlotId);
                if (replacement) {
                    const slotLabel = replacement.kind === 'forwarded'
                        ? `slot ${openSlotId} (forwarded from ${replacement.fromSlot})`
                        : `slot ${openSlotId} (static)`;

                    await expandConnector({
                        connectorName: replacement.targetName,
                        parentId: id,
                        path: `${fullPath}:${dimId}`,
                        incomingBindings: new Map(),
                        boundDescriptor: {
                            kind: replacement.kind,
                            slotLabel
                        }
                    });
                }
                else {
                    const scalarId = nodeIdCounter++;
                    childList.push({
                        parent: id,
                        id: scalarId,
                        name: `${fullPath}:${dimId}`,
                        scalar: true
                    });
                    runningInstanceData[scalarId] = [0, 0];
                }

                openSlotId += 1;
                continue;
            }

            const childOpenSlots = await computeOpenSlots(dimension.composite);
            const staticBindingMap = new Map();
            getSortedBindingEntries(dimension.bindings).forEach(({ slotId, targetName }) => {
                if (slotId < childOpenSlots && !staticBindingMap.has(slotId)) {
                    staticBindingMap.set(slotId, targetName);
                }
            });

            const childBindings = new Map();
            let childOpenSlotsInParent = 0;
            for (let childSlotId = 0; childSlotId < childOpenSlots; childSlotId++) {
                const staticTarget = staticBindingMap.get(childSlotId);
                if (staticTarget) {
                    childBindings.set(childSlotId, {
                        targetName: staticTarget,
                        kind: 'static',
                        fromSlot: childSlotId
                    });
                    continue;
                }

                const forwardedBinding = incomingBindings.get(openSlotId + childOpenSlotsInParent);
                if (forwardedBinding) {
                    childBindings.set(childSlotId, {
                        targetName: forwardedBinding.targetName,
                        kind: 'forwarded',
                        fromSlot: openSlotId + childOpenSlotsInParent
                    });
                }

                childOpenSlotsInParent += 1;
            }

            await expandConnector({
                connectorName: dimension.composite,
                parentId: id,
                path: `${fullPath}:${dimId}`,
                incomingBindings: childBindings,
                boundDescriptor: null
            });

            openSlotId += childOpenSlots - staticBindingMap.size;
        }
    }

    await expandConnector({
        connectorName: rootName,
        parentId: -1,
        path: '',
        incomingBindings: new Map(),
        boundDescriptor: null
    });

    updateRunningInstanceList();

    return childList;
}

export async function fetchBeforeExecute() {
    const executeNameInput = document.getElementById('executeName');
    rootExecuteName = normalizeConnectorName(executeNameInput.value);
    if (!rootExecuteName) {
        alert("Please provide a connector name.");
        return;
    }
    executeNameInput.value = rootExecuteName;

    try {
        const connectors = await fetchConnectorDepthFirst(rootExecuteName);
        drawExecuteTree(connectors);
    } catch (e) {
        console.error(e);

        const container = document.getElementById('treeContainer');
        container.style.display = 'block';
        container.innerHTML = `<p>Failed to fetch connectors. ${e.message} </p>`;
    }
}

export async function execute() {
    const connector_name = normalizeConnectorName(document.getElementById('executeName').value);
    const particles_count = document.getElementById('executeN').value.trim();
    const running_instances = runningInstanceData.map(([start_point, transformation_shift]) => ({ start_point, transformation_shift }));

    const codeDiv = document.getElementById('executeCode');
    const bodyDiv = document.getElementById('executeBody');
    if (!connector_name) {
        alert("Contract name is required.");
        return;
    }
    try {
        const res = await requestWithLogin(apiUrl('/execute'),
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ connector_name, particles_count, running_instances }),
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
