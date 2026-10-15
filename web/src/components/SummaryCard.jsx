import { fmtSpeed } from '../lib/format.js'

/**
 * Overview of the engine's current activity. These figures used to sit in the
 * status bar at the smallest legible size; promoting them gives the page a
 * substantial top section and puts the most important numbers first.
 */
export default function SummaryCard({ stats }) {
  const tiles = [
    { label: 'Download', value: fmtSpeed(stats?.totalDownSpeed) },
    { label: 'Upload',   value: fmtSpeed(stats?.totalUpSpeed) },
    { label: 'Peers',    value: String(stats?.connectedPeers ?? 0) },
    { label: 'Active',   value: String(stats?.activeTorrents ?? 0) },
  ]

  return (
    <dl className="grid grid-cols-2 overflow-hidden rounded-group bg-elevated sm:grid-cols-4">
      {tiles.map((t, i) => (
        <div
          key={t.label}
          className={`px-4 py-3.5 ${i % 2 === 1 ? 'border-l border-separator' : ''} ${
            i < 2 ? 'border-b border-separator sm:border-b-0' : ''
          } ${i === 2 ? 'sm:border-l sm:border-separator' : ''}`}
        >
          <dt className="text-footnote text-secondary">{t.label}</dt>
          <dd className="mt-0.5 truncate text-title3 font-medium tabular-nums">{t.value}</dd>
        </div>
      ))}
    </dl>
  )
}
