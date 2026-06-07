import { useState, useEffect, useCallback } from 'react'
import TorrentList from './components/TorrentList.jsx'
import StatsBar from './components/StatsBar.jsx'
import AddModal from './components/AddModal.jsx'
import { fetchTorrents, fetchStats, openWebSocket } from './api.js'

export default function App() {
  const [torrents, setTorrents] = useState([])
  const [stats, setStats] = useState(null)
  const [showAdd, setShowAdd] = useState(false)
  const [error, setError] = useState(null)

  const refresh = useCallback(async () => {
    try {
      const [t, s] = await Promise.all([fetchTorrents(), fetchStats()])
      setTorrents(t)
      setStats(s)
      setError(null)
    } catch (e) {
      setError(e.message)
    }
  }, [])

  useEffect(() => {
    refresh()
    const ws = openWebSocket((msg) => {
      if (msg.type === 'torrent_update') setTorrents(msg.payload ?? [])
      if (msg.type === 'stats') setStats(msg.payload)
    })
    return () => ws.close()
  }, [refresh])

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 flex flex-col">
      <header className="bg-gray-900 border-b border-gray-800 px-6 py-3 flex items-center justify-between">
        <span className="font-bold text-lg tracking-tight text-green-400">qBit-Go</span>
        <button
          onClick={() => setShowAdd(true)}
          className="bg-green-600 hover:bg-green-500 text-white text-sm font-medium px-4 py-1.5 rounded transition-colors"
        >
          + Add Torrent
        </button>
      </header>

      {stats && <StatsBar stats={stats} />}

      {error && (
        <div className="mx-6 mt-4 bg-red-900/40 border border-red-700 text-red-300 text-sm px-4 py-2 rounded">
          {error}
        </div>
      )}

      <main className="flex-1 px-6 py-4">
        <TorrentList torrents={torrents} onRefresh={refresh} />
      </main>

      {showAdd && (
        <AddModal
          onClose={() => setShowAdd(false)}
          onAdded={() => { setShowAdd(false); refresh() }}
        />
      )}
    </div>
  )
}
