function fmt(bytes) {
  if (bytes === 0) return '0 B/s'
  const units = ['B/s', 'KB/s', 'MB/s', 'GB/s']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i]
}

export default function StatsBar({ stats }) {
  return (
    <div className="bg-gray-900 border-b border-gray-800 px-6 py-2 flex gap-6 text-xs text-gray-400">
      <span>
        <span className="text-green-400 font-medium">↓</span> {fmt(stats.totalDownSpeed ?? 0)}
      </span>
      <span>
        <span className="text-blue-400 font-medium">↑</span> {fmt(stats.totalUpSpeed ?? 0)}
      </span>
      <span>Active: <span className="text-white">{stats.activeTorrents ?? 0}</span></span>
      <span>Seeding: <span className="text-white">{stats.seedingTorrents ?? 0}</span></span>
      <span>DHT peers: <span className="text-white">{stats.dhtPeers ?? 0}</span></span>
      <span>Port: <span className="text-white">{stats.listenPort ?? '—'}</span></span>
    </div>
  )
}
