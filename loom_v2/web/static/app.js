const timeline = document.querySelector('#timeline');
const form = document.querySelector('#prompt-form');
const promptInput = document.querySelector('#prompt');
const sendButton = form.querySelector('button');
const agentStatus = document.querySelector('#agent-status');
const conversationList = document.querySelector('#conversation-list');
const newConversationButton = document.querySelector('#new-conversation');

let conversationRef = null;
let conversationSummaries = [];

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
    button.textContent = summary.title || summary.conversation_ref;
    button.title = summary.conversation_ref;
    button.addEventListener('click', () => selectConversation(summary.conversation_ref));
    conversationList.appendChild(button);
  }
}

async function loadConversation(ref) {
  const response = await fetch(`/api/v1/conversations/${encodeURIComponent(ref)}`);
  if (!response.ok) throw new Error(`conversation history failed: ${response.status}`);
  const conversation = await response.json();
  renderMessages(conversation.messages || []);
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
  conversationRef = newConversationRef();
  renderConversationList();
  showEmptyTimeline();
  promptInput.focus();
});

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const prompt = promptInput.value.trim();
  if (!prompt) return;
  if (!conversationRef) conversationRef = newConversationRef();
  appendMessage('user', prompt);
  promptInput.value = '';
  sendButton.disabled = true;
  appendMessage('status', 'Assistant: working…');
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
  appendMessage('error', `Conversation history unavailable (${error.message})`);
});
