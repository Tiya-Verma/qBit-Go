export default function StatsBar({ stats, online }) {
  return (
    <footer className="sticky bottom-0 border-t border-separator bg-elevated/80 backdrop-blur-xl">
      <div className="mx-auto flex w-full max-w-3xl flex-wrap items-center gap-x-5 gap-y-1 px-5 py-2.5 text-footnote text-secondary">
        <span>DHT Peers <span className="tabular-nums text-label">{stats?.dhtPeers ?? 0}</span></span>
        <span>Port <span className="tabular-nums text-label">{stats?.listenPort ?? '—'}</span></span>
        <span className="ml-auto flex items-center gap-1.5">
          <span className={`h-2 w-2 rounded-full ${online ? 'bg-green' : 'bg-tertiary'}`} />
          {online ? 'Connected' : 'Not Connected'}
        </span>
      </div>
    </footer>
  )
}
