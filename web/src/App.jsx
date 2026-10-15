import { useState, useEffect, useCallback } from 'react'
import TorrentList from './components/TorrentList.jsx'
import SummaryCard from './components/SummaryCard.jsx'
import StatsBar from './components/StatsBar.jsx'
import AddModal from './components/AddModal.jsx'
import { fetchTorrents, fetchStats, openWebSocket } from './api.js'

export default function App() {
  const [torrents, setTorrents] = useState([])
  const [stats, setStats] = useState(null)
  const [showAdd, setShowAdd] = useState(false)
  const [error, setError] = useState(null)
  const [online, setOnline] = useState(false)

  const refresh = useCallback(async () => {
    try {
      const [t, s] = await Promise.all([fetchTorrents(), fetchStats()])
      setTorrents(t ?? [])
      setStats(s)
      setError(null)
    } catch (e) {
      setError(e.message)
    }
  }, [])

  useEffect(() => {
    let socket = null
    let retries = 0
    let reconnectTimer = null
    let unmounted = false

    const connect = () => {
      if (unmounted) return
      const ws = openWebSocket((msg) => {
        if (msg.type === 'torrent_update') setTorrents(msg.payload ?? [])
        if (msg.type === 'stats') setStats(msg.payload)
      })
      socket = ws

      ws.onopen = () => {
        // Ignore a socket that is no longer the live one. React remounts
        // effects in development, so a discarded socket's events can arrive
        // after its replacement has already connected.
        if (unmounted || socket !== ws) return
        retries = 0
        setOnline(true)
      }

      ws.onclose = () => {
        if (unmounted || socket !== ws) return
        setOnline(false)
        // Reconnect with backoff. Without this the page keeps showing stale
        // numbers forever after any dropped connection — a server restart,
        // a sleeping laptop — with no way back short of a manual reload.
        const delay = Math.min(1000 * 2 ** retries++, 15000)
        reconnectTimer = setTimeout(connect, delay)
      }
    }

    refresh()
    connect()

    return () => {
      unmounted = true
      clearTimeout(reconnectTimer)
      socket?.close()
    }
  }, [refresh])

  return (
    <div className="flex min-h-screen flex-col">
      {/* Toolbar: window title, then actions on the trailing side. Untinted,
          separated from the content layer by a hairline. */}
      <header className="sticky top-0 z-30 border-b border-separator bg-elevated/80 backdrop-blur-xl">
        <div className="mx-auto flex h-14 w-full max-w-3xl items-center gap-4 px-5">
          <span className="text-headline">qBit-Go</span>
          <button
            onClick={() => setShowAdd(true)}
            className="ml-auto flex h-11 items-center rounded-control bg-blue px-4 text-subhead font-medium text-white transition active:bg-blue/80 focus-visible:outline focus-visible:outline-[3px] focus-visible:outline-offset-1 focus-visible:outline-blue/50 sm:h-8"
          >
            Add Torrent
          </button>
        </div>
      </header>

      <main className="mx-auto w-full max-w-3xl flex-1 px-5 pb-10 pt-7">
        <h1 className="text-title1 font-semibold">Transfers</h1>
        <p className="mt-1 text-subhead text-secondary">
          {torrents.length === 0
            ? 'Nothing downloading right now'
            : `${torrents.length} ${torrents.length === 1 ? 'torrent' : 'torrents'} in your library`}
        </p>

        <div className="mt-5">
          <SummaryCard stats={stats} />
        </div>

        {error && (
          <div className="mt-5 rounded-group bg-elevated px-4 py-3.5 text-subhead text-red">
            {error}
          </div>
        )}

        <div className="mt-7">
          <TorrentList
            torrents={torrents}
            onRefresh={refresh}
            onAdd={() => setShowAdd(true)}
          />
        </div>
      </main>

      <StatsBar stats={stats} online={online} />

      {showAdd && (
        <AddModal
          onClose={() => setShowAdd(false)}
          onAdded={() => { setShowAdd(false); refresh() }}
        />
      )}
    </div>
  )
}
