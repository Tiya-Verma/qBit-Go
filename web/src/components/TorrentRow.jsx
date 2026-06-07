import { useState } from 'react'
import { pauseTorrent, resumeTorrent, removeTorrent } from '../api.js'

const STATE_COLORS = {
  downloading: 'bg-green-900 text-green-300',
  seeding:     'bg-blue-900 text-blue-300',
  paused:      'bg-yellow-900 text-yellow-300',
  fetching:    'bg-purple-900 text-purple-300',
  checking:    'bg-orange-900 text-orange-300',
  error:       'bg-red-900 text-red-300',
  stopped:     'bg-gray-800 text-gray-400',
}

function fmtSpeed(bps) {
  if (!bps) return '—'
  if (bps < 1024) return bps + ' B/s'
  if (bps < 1024 * 1024) return (bps / 1024).toFixed(1) + ' KB/s'
  return (bps / 1024 / 1024).toFixed(1) + ' MB/s'
}

function fmtSize(bytes) {
  if (!bytes) return '0 B'
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
  if (bytes < 1024 * 1024 * 1024) return (bytes / 1024 / 1024).toFixed(1) + ' MB'
  return (bytes / 1024 / 1024 / 1024).toFixed(2) + ' GB'
}

export default function TorrentRow({ torrent: t, onRefresh }) {
  const [busy, setBusy] = useState(false)

  async function act(fn) {
    setBusy(true)
    try { await fn() } finally { setBusy(false); onRefresh() }
  }

  const pct = Math.round((t.progress ?? 0) * 100)
  const isPaused = t.state === 'paused'
  const stateClass = STATE_COLORS[t.state] ?? 'bg-gray-800 text-gray-400'

  return (
    <tr className="bg-gray-950 hover:bg-gray-900 transition-colors">
      <td className="px-4 py-3 max-w-xs">
        <div className="truncate font-medium text-gray-100" title={t.name}>
          {t.name || t.hash}
        </div>
        <div className="text-xs text-gray-600 truncate font-mono">{t.hash}</div>
      </td>

      <td className="px-4 py-3">
        <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${stateClass}`}>
          {t.state}
        </span>
      </td>

      <td className="px-4 py-3">
        <div className="flex items-center gap-2">
          <div className="flex-1 bg-gray-800 rounded-full h-1.5 overflow-hidden">
            <div
              className="bg-green-500 h-full rounded-full transition-all"
              style={{ width: `${pct}%` }}
            />
          </div>
          <span className="text-xs text-gray-400 w-10 text-right">{pct}%</span>
        </div>
        <div className="text-xs text-gray-600 mt-0.5">
          {fmtSize(t.downloaded)} / {fmtSize(t.size)}
        </div>
      </td>

      <td className="px-4 py-3 text-right text-green-400 text-xs tabular-nums">
        {fmtSpeed(t.downloadSpeed)}
      </td>

      <td className="px-4 py-3 text-right text-blue-400 text-xs tabular-nums">
        {fmtSpeed(t.uploadSpeed)}
      </td>

      <td className="px-4 py-3 text-right">
        <div className="flex justify-end gap-1">
          {isPaused ? (
            <button
              disabled={busy}
              onClick={() => act(() => resumeTorrent(t.hash))}
              className="text-xs px-2 py-1 rounded bg-gray-800 hover:bg-green-800 text-gray-300 hover:text-green-200 transition-colors disabled:opacity-50"
            >
              Resume
            </button>
          ) : (
            <button
              disabled={busy || t.state === 'seeding' || t.state === 'fetching'}
              onClick={() => act(() => pauseTorrent(t.hash))}
              className="text-xs px-2 py-1 rounded bg-gray-800 hover:bg-yellow-800 text-gray-300 hover:text-yellow-200 transition-colors disabled:opacity-50"
            >
              Pause
            </button>
          )}
          <button
            disabled={busy}
            onClick={() => act(() => removeTorrent(t.hash))}
            className="text-xs px-2 py-1 rounded bg-gray-800 hover:bg-red-900 text-gray-300 hover:text-red-300 transition-colors disabled:opacity-50"
          >
            Remove
          </button>
        </div>
      </td>
    </tr>
  )
}
