const BASE = '/api/v1'

export async function fetchTorrents() {
  const r = await fetch(`${BASE}/torrents`)
  if (!r.ok) throw new Error('fetch torrents failed')
  return r.json()
}

export async function fetchStats() {
  const r = await fetch(`${BASE}/stats`)
  if (!r.ok) throw new Error('fetch stats failed')
  return r.json()
}

export async function addTorrentFile(file) {
  const form = new FormData()
  form.append('torrent', file)
  const r = await fetch(`${BASE}/torrents`, { method: 'POST', body: form })
  if (!r.ok) {
    const body = await r.json().catch(() => ({}))
    throw new Error(body.error || 'add torrent failed')
  }
  return r.json()
}

export async function addMagnet(magnet) {
  const r = await fetch(`${BASE}/torrents`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ magnet }),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => ({}))
    throw new Error(body.error || 'add magnet failed')
  }
  return r.json()
}

export async function removeTorrent(hash, deleteFiles = false) {
  const r = await fetch(`${BASE}/torrents/${hash}?deleteFiles=${deleteFiles}`, {
    method: 'DELETE',
  })
  if (!r.ok) throw new Error('remove failed')
}

export async function pauseTorrent(hash) {
  const r = await fetch(`${BASE}/torrents/${hash}/pause`, { method: 'POST' })
  if (!r.ok) throw new Error('pause failed')
}

export async function resumeTorrent(hash) {
  const r = await fetch(`${BASE}/torrents/${hash}/resume`, { method: 'POST' })
  if (!r.ok) throw new Error('resume failed')
}

export function openWebSocket(onMessage) {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws'
  const ws = new WebSocket(`${proto}://${location.host}/api/v1/ws`)
  ws.onmessage = (e) => {
    try { onMessage(JSON.parse(e.data)) } catch (_) {}
  }
  return ws
}
