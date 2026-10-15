import { useState } from 'react'
import { pauseTorrent, resumeTorrent, removeTorrent } from '../api.js'
import { fmtSize, fmtSpeed, fmtRemaining, middleTruncate } from '../lib/format.js'
import Alert from './Alert.jsx'

// Status text carries the state. Color is applied only where it has a single,
// unambiguous meaning: red for a problem, green for a finished transfer.
const STATUS = {
  downloading: { label: 'Downloading',        tone: 'text-secondary' },
  seeding:     { label: 'Seeding',            tone: 'text-green' },
  fetching:    { label: 'Fetching metadata',  tone: 'text-secondary' },
  checking:    { label: 'Verifying files',    tone: 'text-secondary' },
  paused:      { label: 'Paused',             tone: 'text-secondary' },
  stopped:     { label: 'Stopped',            tone: 'text-secondary' },
  error:       { label: 'Unable to download', tone: 'text-red' },
}

export default function TorrentRow({ torrent: t, onRefresh, last }) {
  const [busy, setBusy] = useState(false)
  const [askRemove, setAskRemove] = useState(false)

  async function act(fn) {
    setBusy(true)
    try { await fn() } finally { setBusy(false); onRefresh() }
  }

  const status = STATUS[t.state] ?? { label: t.state, tone: 'text-secondary' }

  // `downloaded` only counts the current session, so a resumed torrent reports
  // 0 while progress is non-zero. Derive the completed size from progress.
  const size = t.size ?? 0
  const done = Math.max(t.downloaded ?? 0, Math.round((t.progress ?? 0) * size))
  const pct = Math.min(100, Math.round((t.progress ?? 0) * 100))

  const isPaused = t.state === 'paused' || t.state === 'stopped'
  const isIndeterminate = t.state === 'fetching'
  // Progress indicators are transient: they belong onscreen only while the
  // operation is ongoing, so a seeding (complete) torrent shows none.
  const showProgress = isIndeterminate || ['downloading', 'checking', 'paused', 'stopped'].includes(t.state)

  const remaining = t.state === 'downloading' ? fmtRemaining(size - done, t.downloadSpeed) : null
  // A transfer with no peers reads as stalled rather than merely slow, so say
  // so instead of showing a bare 0 KB/s.
  const active = t.state === 'downloading' || t.state === 'seeding'
  const details = [
    isIndeterminate ? null : `${fmtSize(done)} of ${fmtSize(size)}`,
    t.downloadSpeed > 0 ? `↓ ${fmtSpeed(t.downloadSpeed)}` : null,
    t.uploadSpeed > 0 ? `↑ ${fmtSpeed(t.uploadSpeed)}` : null,
    active && t.peers > 0 ? `${t.peers} ${t.peers === 1 ? 'peer' : 'peers'}` : null,
    active && !t.peers ? 'Looking for peers' : null,
    remaining,
  ].filter(Boolean)

  return (
    <li className="group px-5 transition-colors hover:bg-fill-sm">
      <div className={`py-4 ${last ? '' : 'border-b border-separator'}`}>
        <div className="flex items-baseline gap-4">
          <h3 className="min-w-0 flex-1 truncate text-body font-medium" title={t.name || t.hash}>
            {t.name || t.hash}
          </h3>
          {showProgress && !isIndeterminate && (
            <span className="shrink-0 text-subhead tabular-nums text-secondary">{pct}%</span>
          )}
        </div>

        {showProgress && (
          <div
            role="progressbar"
            aria-valuenow={isIndeterminate ? undefined : pct}
            aria-valuemin={0}
            aria-valuemax={100}
            className="mt-2.5 h-1.5 overflow-hidden rounded-full bg-fill"
          >
            {isIndeterminate ? (
              <div className="h-full w-2/5 animate-indeterminate rounded-full bg-blue" />
            ) : (
              <div
                className={`h-full rounded-full transition-[width] duration-500 ${isPaused ? 'bg-tertiary' : 'bg-blue'}`}
                style={{ width: `${pct}%` }}
              />
            )}
          </div>
        )}

        <div className="mt-2.5 flex flex-wrap items-center gap-x-4 gap-y-1">
          <p className="min-w-0 text-subhead tabular-nums text-secondary">
            <span className={status.tone}>{status.label}</span>
            {details.map((d) => (
              <span key={d}> · {d}</span>
            ))}
          </p>

          {/* Row actions reveal on hover for pointer input, and stay visible
              at touch sizes on narrow screens. */}
          <div className="ml-auto flex shrink-0 items-center gap-1 sm:opacity-0 sm:transition-opacity sm:group-hover:opacity-100 sm:group-focus-within:opacity-100">
            {isPaused ? (
              <RowButton disabled={busy} onClick={() => act(() => resumeTorrent(t.hash))}>
                Resume
              </RowButton>
            ) : (
              <RowButton
                disabled={busy || t.state === 'fetching' || t.state === 'checking'}
                onClick={() => act(() => pauseTorrent(t.hash))}
              >
                Pause
              </RowButton>
            )}
            <RowButton disabled={busy} onClick={() => setAskRemove(true)}>
              Remove
            </RowButton>
          </div>
        </div>
      </div>

      {/* Removing a torrent is uncommon and can't be undone, so it warrants
          an alert rather than silent destruction. */}
      {askRemove && (
        <Alert
          title="Remove Torrent?"
          message={`“${middleTruncate(t.name || t.hash, 60)}” will stop transferring and leave your list. You can choose whether to keep the files that have already downloaded.`}
          onCancel={() => setAskRemove(false)}
          actions={[
            {
              label: 'Remove, Keep Files',
              primary: true,
              onClick: () => { setAskRemove(false); act(() => removeTorrent(t.hash, false)) },
            },
            {
              label: 'Remove and Delete Files',
              destructive: true,
              onClick: () => { setAskRemove(false); act(() => removeTorrent(t.hash, true)) },
            },
          ]}
        />
      )}
    </li>
  )
}

function RowButton({ children, onClick, disabled }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="h-11 rounded-control px-3 text-subhead font-medium text-blue transition hover:bg-fill active:bg-fill-strong disabled:text-tertiary disabled:hover:bg-transparent focus-visible:outline focus-visible:outline-[3px] focus-visible:outline-offset-1 focus-visible:outline-blue/50 sm:h-8"
    >
      {children}
    </button>
  )
}
