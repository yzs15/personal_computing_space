const timeline = document.querySelector('#timeline');
const form = document.querySelector('#prompt-form');

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
