import { requestWithLogin, formatJSON, apiUrl } from "./utils";

let rootExecuteName;

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

function getCompositeNames(particle) {
    if (!particle || typeof particle !== 'object') {
        return [];
    }

    const rawComposites =
        Array.isArray(particle.composite_names) ? particle.composite_names :
            Array.isArray(particle.compositeNames) ? particle.compositeNames :
                Array.isArray(particle.composites) ? particle.composites : [];

    return rawComposites.map((entry) => {
        if (typeof entry === 'string') {
            return entry;
        }
        if (entry && typeof entry.name === 'string') {
            return entry.name;
        }
        return '';
    });
}

async function fetchParticleDepthFirst(rootName) {
    clearRunningInstances();

    let nodeIdCounter = 0;
    const childList = [];
    const stack = [{
        parent: -1,
        path: '',
        name: rootName,
        assignId: () => nodeIdCounter++,
        isScalarLeaf: false
    }];

    while (stack.length > 0) {
        const { parent, path, name, assignId, isScalarLeaf } = stack.pop();
        const id = assignId();

        if (isScalarLeaf) {
            const fullPath = `${path}/${name}`;
            childList.push({ parent, id, name: fullPath, scalar: true });
            runningInstanceData[id] = [0, 0];
            continue;
        }

        try {
            const res = await fetch(apiUrl(`/particle/${encodeURIComponent(name)}`));
            if (!res.ok) throw new Error(`Failed to fetch ${name}`);
            const particle = await res.json();

            const particleName = typeof particle.name === 'string' ? particle.name : name;
            const compositeNames = getCompositeNames(particle);
            const fullPath = `${path}/${particleName}`;
            const scalar = compositeNames.length === 0;

            // Push children in reverse so they’re visited left-to-right.
            for (let i = compositeNames.length - 1; i >= 0; i--) {
                const compositeName = typeof compositeNames[i] === 'string'
                    ? compositeNames[i].trim()
                    : '';
                if (compositeName !== '') {
                    stack.push({
                        parent: id,
                        path: fullPath,
                        name: compositeName,
                        assignId: () => nodeIdCounter++,
                        isScalarLeaf: false
                    });

                }
                else {
                    stack.push({
                        parent: id,
                        path: fullPath,
                        name: `${particleName}_${i}`,
                        assignId: () => nodeIdCounter++,
                        isScalarLeaf: true
                    });
                }
            }

            childList.push({ parent, id, name: fullPath, scalar });
            runningInstanceData[id] = [0, 0];
        } catch (e) {
            console.error(`Error fetching ${name}, ${e.message}`);
            throw e;
        }
    }

    updateRunningInstanceList();

    return childList;
}

export async function fetchBeforeExecute() {
    rootExecuteName = document.getElementById('executeName').value.trim();
    if (!rootExecuteName) {
        alert("Please provide a particle name.");
        return;
    }

    try {
        const particles = await fetchParticleDepthFirst(rootExecuteName);
        drawExecuteTree(particles);
    } catch (e) {
        console.error(e);

        const container = document.getElementById('treeContainer');
        container.style.display = 'block';
        container.innerHTML = `<p>Failed to fetch particles. ${e.message} </p>`;
    }
}

export async function execute() {
    const particle_name = document.getElementById('executeName').value.trim();
    const samples_count = document.getElementById('executeN').value.trim();
    const running_instances = runningInstanceData.map(([start_point, transformation_shift]) => ({ start_point, transformation_shift }));

    const codeDiv = document.getElementById('executeCode');
    const bodyDiv = document.getElementById('executeBody');
    if (!particle_name) {
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
                body: JSON.stringify({ particle_name, samples_count, running_instances }),
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
