// consumer/src/public/app.js
async function fetchVideos() {
  const res = await fetch('/api/videos');
  return res.json();
}

function createCard(item) {
  const div = document.createElement('div');
  div.className = 'card';

  const video = document.createElement('video');
  video.src = item.url;
  video.muted = true;
  video.preload = 'metadata';

  // Preview first 10 seconds on hover
  let previewTimer = null;
  video.addEventListener('mouseenter', () => {
    video.currentTime = 0;
    video.play();
    previewTimer = setTimeout(() => {
      video.pause();
      video.currentTime = 0;
    }, 10000);
  });
  video.addEventListener('mouseleave', () => {
    video.pause();
    video.currentTime = 0;
    if (previewTimer) clearTimeout(previewTimer);
  });

  // Full playback on click (modal)
  video.addEventListener('click', () => openModal(item.url));

  const meta = document.createElement('div');
  meta.className = 'meta';
  meta.textContent = `${item.filename} • ${(item.sizeBytes/1024/1024).toFixed(2)} MB`;

  div.appendChild(video);
  div.appendChild(meta);
  return div;
}

function renderGallery(items) {
  const gallery = document.getElementById('gallery');
  gallery.innerHTML = '';
  items.forEach(item => gallery.appendChild(createCard(item)));
}

function openModal(url) {
  const modal = document.getElementById('playerModal');
  const player = document.getElementById('mainPlayer');
  player.src = url;
  modal.classList.remove('hidden');
}

document.getElementById('closeBtn').addEventListener('click', () => {
  const modal = document.getElementById('playerModal');
  const player = document.getElementById('mainPlayer');
  player.pause();
  player.src = '';
  modal.classList.add('hidden');
});

(async function init() {
  const items = await fetchVideos();
  renderGallery(items);
  // Optional: periodic refresh
  //setInterval(async () => renderGallery(await fetchVideos()), 5000);
})();
