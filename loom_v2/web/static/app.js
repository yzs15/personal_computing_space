const timeline = document.querySelector('#timeline');
const form = document.querySelector('#prompt-form');
const promptInput = document.querySelector('#prompt');
const sendButton = form.querySelector('button');
const agentStatus = document.querySelector('#agent-status');
const conversationStatus = document.querySelector('#conversation-status');
const interruptButton = document.querySelector('#interrupt-conversation');
const conversationList = document.querySelector('#conversation-list');
const newConversationButton = document.querySelector('#new-conversation');
const candidatePackages = document.querySelector('#candidate-packages');

let conversationRef = null;
let conversationSummaries = [];
let statusPollTimer = null;
let statusPollInFlight = false;
let interruptPendingRef = null;

const statusLabels = {
  idle: 'Idle',
  thinking: 'Thinking',
  executing: 'Executing',
  completed: 'Completed',
  interrupted: 'Interrupted',
  failed: 'Failed',
};

function isActiveStatus(status) {
  return status === 'thinking' || status === 'executing';
}

function renderConversationStatus(status) {
  const normalized = Object.prototype.hasOwnProperty.call(statusLabels, status) ? status : 'idle';
  if (interruptPendingRef === conversationRef && isActiveStatus(normalized)) {
    conversationStatus.dataset.status = normalized;
    conversationStatus.textContent = 'Interrupt requested';
    interruptButton.disabled = true;
    return;
  }
  if (interruptPendingRef === conversationRef && !isActiveStatus(normalized)) {
    interruptPendingRef = null;
  }
  conversationStatus.dataset.status = normalized;
  conversationStatus.textContent = statusLabels[normalized];
  interruptButton.disabled = !isActiveStatus(normalized);
}

function stopStatusPolling() {
  if (statusPollTimer !== null) {
    globalThis.clearInterval(statusPollTimer);
    statusPollTimer = null;
  }
}

function startStatusPolling() {
  if (statusPollTimer !== null) return;
  statusPollTimer = globalThis.setInterval(async () => {
    if (!conversationRef || statusPollInFlight) return;
    statusPollInFlight = true;
    const ref = conversationRef;
    try {
      await loadConversation(ref);
    } catch (_error) {
      // The first turn may not have been committed yet; the next poll retries.
    } finally {
      statusPollInFlight = false;
    }
  }, 1500);
}

function syncStatusPolling(status) {
  if (isActiveStatus(status)) startStatusPolling();
  else stopStatusPolling();
}

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

function newConversationRef() {
  const uuid = globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function'
    ? globalThis.crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `conversation-${uuid}`;
}

function appendMessage(role, content) {
  const line = document.createElement('p');
  line.className = `message ${role}`;
  const prefix = role === 'user' ? 'You: ' : role === 'assistant' ? 'Assistant: ' : '';
  line.textContent = `${prefix}${content}`;
  timeline.appendChild(line);
  timeline.scrollTop = timeline.scrollHeight;
}

function showEmptyTimeline() {
  timeline.replaceChildren();
  const empty = document.createElement('p');
  empty.className = 'status';
  empty.textContent = 'Start a conversation to refine a task closure.';
  timeline.appendChild(empty);
}

function renderMessages(messages) {
  timeline.replaceChildren();
  if (!messages.length) {
    showEmptyTimeline();
    return;
  }
  for (const message of messages) {
    if (message.role === 'user' || message.role === 'assistant') {
      appendMessage(message.role, message.content || '');
    }
  }
}

function renderCandidatePackages(packages) {
  candidatePackages.replaceChildren();
  if (!packages || !packages.length) {
    candidatePackages.textContent = 'No candidates';
    return;
  }
  for (const pkg of packages) {
    const row = document.createElement('div');
    row.className = 'package-row';
    const label = document.createElement('span');
    label.textContent = `${pkg.package_id}@${pkg.package_version} · ${pkg.scope}/${pkg.publication_state}`;
    row.appendChild(label);
    if (pkg.publication_state === 'candidate') {
      const promote = document.createElement('button');
      promote.type = 'button';
      promote.textContent = 'Promote';
      promote.addEventListener('click', async () => {
        promote.disabled = true;
        await fetch(`/api/v1/capability-packages/${encodeURIComponent(pkg.package_id)}/promote`, {
          method: 'POST', headers: {'content-type': 'application/json'},
          body: JSON.stringify({approved_digest: pkg.package_digest}),
        });
        if (conversationRef) await loadConversation(conversationRef);
      });
      row.appendChild(promote);
      const abandon = document.createElement('button');
      abandon.type = 'button';
      abandon.textContent = 'Abandon';
      abandon.addEventListener('click', async () => {
        abandon.disabled = true;
        await fetch(`/api/v1/capability-packages/${encodeURIComponent(pkg.package_id)}/abandon`, {method: 'POST'});
        if (conversationRef) await loadConversation(conversationRef);
      });
      row.appendChild(abandon);
    }
    candidatePackages.appendChild(row);
  }
}

function renderConversationList() {
  conversationList.replaceChildren();
  if (!conversationSummaries.length) {
    const empty = document.createElement('span');
    empty.className = 'empty';
    empty.textContent = 'No saved conversations';
    conversationList.appendChild(empty);
    return;
  }
  for (const summary of conversationSummaries) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = summary.conversation_ref === conversationRef ? 'active' : '';
    const status = statusLabels[summary.status] || statusLabels.idle;
    button.textContent = `${summary.title || summary.conversation_ref} · ${status}`;
    button.title = summary.conversation_ref;
    button.addEventListener('click', () => selectConversation(summary.conversation_ref));
    conversationList.appendChild(button);
  }
}

async function loadConversation(ref) {
  const response = await fetch(`/api/v1/conversations/${encodeURIComponent(ref)}`);
  if (!response.ok) throw new Error(`conversation history failed: ${response.status}`);
  const conversation = await response.json();
  if (ref !== conversationRef) return;
  renderMessages(conversation.messages || []);
  renderConversationStatus(conversation.status || 'idle');
  const latestRun = (conversation.runs || []).at(-1);
  renderCandidatePackages(latestRun?.capability_packages || []);
  const summary = conversationSummaries.find((item) => item.conversation_ref === ref);
  if (summary && summary.status !== (conversation.status || 'idle')) {
    summary.status = conversation.status || 'idle';
    renderConversationList();
  }
  syncStatusPolling(conversation.status || 'idle');
}

async function loadConversations(selectLatest) {
  const response = await fetch('/api/v1/conversations');
  if (!response.ok) throw new Error(`conversation list failed: ${response.status}`);
  conversationSummaries = await response.json();
  if (selectLatest && conversationSummaries.length) {
    const latest = conversationSummaries[conversationSummaries.length - 1];
    conversationRef = latest.conversation_ref;
  }
  renderConversationList();
  if (conversationRef && conversationSummaries.some((item) => item.conversation_ref === conversationRef)) {
    await loadConversation(conversationRef);
  } else if (!conversationSummaries.length) {
    showEmptyTimeline();
  }
}

async function selectConversation(ref) {
  interruptPendingRef = null;
  conversationRef = ref;
  renderConversationList();
  try {
    await loadConversation(ref);
  } catch (error) {
    showEmptyTimeline();
    appendMessage('error', error.message);
  }
}

newConversationButton.addEventListener('click', () => {
  stopStatusPolling();
  interruptPendingRef = null;
  conversationRef = newConversationRef();
  renderConversationList();
  showEmptyTimeline();
  renderConversationStatus('idle');
  promptInput.focus();
});

interruptButton.addEventListener('click', async () => {
  if (!conversationRef || interruptButton.disabled) return;
  interruptPendingRef = conversationRef;
  interruptButton.disabled = true;
  conversationStatus.textContent = 'Interrupt requested';
  try {
    const response = await fetch(`/api/v1/conversations/${encodeURIComponent(conversationRef)}/interrupt`, {
      method: 'POST',
      headers: {'content-type': 'application/json'},
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || payload.code || `interrupt failed: ${response.status}`);
    startStatusPolling();
  } catch (error) {
    interruptPendingRef = null;
    appendMessage('error', `Assistant: interrupt failed (${error.message})`);
    try {
      await loadConversation(conversationRef);
    } catch (_reloadError) {
      renderConversationStatus('failed');
    }
  }
});

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const prompt = promptInput.value.trim();
  if (!prompt) return;
  if (!conversationRef) conversationRef = newConversationRef();
  interruptPendingRef = null;
  appendMessage('user', prompt);
  promptInput.value = '';
  sendButton.disabled = true;
  appendMessage('status', 'Assistant: working…');
  renderConversationStatus('thinking');
  startStatusPolling();
  try {
    const response = await fetch('/api/v1/messages', {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({conversation_ref: conversationRef, text: prompt}),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.code || payload.detail || `request failed: ${response.status}`);
    }
    await loadConversations(false);
    if (!payload.assistant_text) appendMessage('status', 'Assistant: run completed without a text reply.');
  } catch (error) {
    appendMessage('error', `Assistant: request failed (${error.message})`);
  } finally {
    sendButton.disabled = false;
  }
});

void loadRuntimeStatus();
void loadConversations(true).catch((error) => {
  conversationSummaries = [];
  renderConversationList();
  showEmptyTimeline();
  renderConversationStatus('idle');
  appendMessage('error', `Conversation history unavailable (${error.message})`);
});
