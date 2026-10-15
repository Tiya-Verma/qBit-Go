import TorrentRow from './TorrentRow.jsx'

export default function TorrentList({ torrents, onRefresh, onAdd }) {
  if (torrents.length === 0) {
    return (
      <div className="rounded-group bg-elevated px-6 py-16 text-center">
        <h2 className="text-title3 font-semibold">No Transfers</h2>
        <p className="mx-auto mt-2 max-w-sm text-subhead text-secondary">
          Add a torrent file or paste a magnet link, and it will appear here
          with its progress.
        </p>
        <button
          onClick={onAdd}
          className="mt-6 h-9 rounded-control bg-fill px-4 text-subhead font-medium text-blue transition active:bg-fill-strong"
        >
          Add Torrent
        </button>
      </div>
    )
  }

  return (
    <section>
      <h2 className="px-1 pb-2.5 text-footnote font-medium uppercase tracking-[0.05em] text-secondary">
        All Torrents
      </h2>
      <ul className="overflow-hidden rounded-group bg-elevated">
        {torrents.map((t, i) => (
          <TorrentRow
            key={t.hash}
            torrent={t}
            onRefresh={onRefresh}
            last={i === torrents.length - 1}
          />
        ))}
      </ul>
    </section>
  )
}
