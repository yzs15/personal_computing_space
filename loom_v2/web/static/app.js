const timeline = document.querySelector('#timeline');
const form = document.querySelector('#prompt-form');
const agentStatus = document.querySelector('#agent-status');

async function loadRuntimeStatus() {
  try {
    const response = await fetch('/api/v1/runtime');
    if (!response.ok) throw new Error(`runtime request failed: ${response.status}`);
    const runtime = await response.json();
    const label = runtime.coding_agent_label || runtime.coding_agent_backend || 'Unknown';
    const model = runtime.model ? ` · ${runtime.model}` : '';
    agentStatus.textContent = `Coding Agent: ${label}${model}`;
  } catch (_error) {
    agentStatus.textContent = 'Coding Agent: unavailable';
  }
}

void loadRuntimeStatus();

function appendEvent(text) {
  const line = document.createElement('p');
  line.textContent = text;
  timeline.appendChild(line);
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const prompt = document.querySelector('#prompt').value.trim();
  if (!prompt) return;
  appendEvent(`You: ${prompt}`);
  document.querySelector('#prompt').value = '';
  const response = await fetch('/api/v1/messages', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({text: prompt})});
  appendEvent(response.ok ? 'Assistant: refining closure…' : 'Assistant: request rejected');
});
