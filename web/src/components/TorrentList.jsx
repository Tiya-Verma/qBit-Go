import TorrentRow from './TorrentRow.jsx'

export default function TorrentList({ torrents, onRefresh }) {
  if (torrents.length === 0) {
    return (
      <div className="text-center text-gray-600 mt-24 text-sm">
        No torrents yet. Add one to get started.
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-gray-800 overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-gray-900 text-gray-500 text-xs uppercase tracking-wider">
          <tr>
            <th className="text-left px-4 py-2">Name</th>
            <th className="text-left px-4 py-2 w-32">State</th>
            <th className="text-left px-4 py-2 w-40">Progress</th>
            <th className="text-right px-4 py-2 w-24">Down</th>
            <th className="text-right px-4 py-2 w-24">Up</th>
            <th className="px-4 py-2 w-24"></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-800">
          {torrents.map((t) => (
            <TorrentRow key={t.hash} torrent={t} onRefresh={onRefresh} />
          ))}
        </tbody>
      </table>
    </div>
  )
}
