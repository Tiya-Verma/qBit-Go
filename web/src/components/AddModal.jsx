import { useState, useRef, useEffect } from 'react'
import { addTorrentFile, addMagnet } from '../api.js'

/** A modal sheet: one focused task, dismissed with Cancel or completed with Add. */
export default function AddModal({ onClose, onAdded }) {
  const [tab, setTab] = useState('file')
  const [magnet, setMagnet] = useState('')
  const [file, setFile] = useState(null)
  const [dragging, setDragging] = useState(false)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const fileRef = useRef(null)

  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  async function submit() {
    setError(null)
    if (tab === 'file' && !file) { setError('Choose a torrent file to continue.'); return }
    if (tab === 'magnet' && !magnet.trim()) { setError('Enter a magnet link to continue.'); return }

    setBusy(true)
    try {
      if (tab === 'file') await addTorrentFile(file)
      else await addMagnet(magnet.trim())
      onAdded()
    } catch (e) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  function dropFile(e) {
    e.preventDefault()
    setDragging(false)
    const f = e.dataTransfer.files?.[0]
    if (f) { setFile(f); setError(null) }
  }

  return (
    <div
      onClick={onClose}
      className="fixed inset-0 z-50 flex animate-fade-in items-start justify-center bg-black/20 px-4 pt-16"
    >
      <div
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Add Torrent"
        className="w-full max-w-[460px] animate-sheet-in overflow-hidden rounded-alert bg-elevated shadow-[0_18px_60px_rgb(0_0_0/0.24)]"
      >
        <div className="px-6 pt-6">
          <h2 className="text-headline">Add Torrent</h2>
          <p className="mt-1 text-subhead text-secondary">
            Choose a torrent file, or paste a magnet link.
          </p>

          {/* Segmented control: equal-width segments, nouns, text only. */}
          <div className="mt-5 grid grid-cols-2 gap-0.5 rounded-[9px] bg-fill p-0.5">
            {[['file', 'File'], ['magnet', 'Magnet']].map(([key, labelText]) => (
              <button
                key={key}
                onClick={() => { setTab(key); setError(null) }}
                aria-pressed={tab === key}
                className={`h-8 rounded-[7px] text-subhead font-medium transition ${
                  tab === key
                    ? 'bg-elevated text-label shadow-[0_1px_3px_rgb(0_0_0/0.12)]'
                    : 'text-secondary active:bg-fill-sm'
                }`}
              >
                {labelText}
              </button>
            ))}
          </div>
        </div>

        <div className="px-6 py-5">
          {tab === 'file' ? (
            <>
              {/* The input lives outside the button: nesting it would make
                  input.click() bubble back and re-trigger the handler. */}
              <input
                ref={fileRef}
                type="file"
                accept=".torrent"
                className="hidden"
                onChange={(e) => { setFile(e.target.files?.[0] ?? null); setError(null) }}
              />
              <button
                type="button"
                onClick={() => fileRef.current?.click()}
                onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
                onDragLeave={() => setDragging(false)}
                onDrop={dropFile}
                className={`w-full rounded-group border border-dashed px-4 py-9 text-center transition ${
                  dragging ? 'border-blue bg-blue/5' : 'border-separator hover:bg-fill-sm'
                }`}
              >
                {file ? (
                  <span className="block truncate text-subhead font-medium">{file.name}</span>
                ) : (
                  <span className="block text-subhead text-secondary">
                    Drag a torrent file here, or click to choose one
                  </span>
                )}
              </button>
            </>
          ) : (
            <input
              type="text"
              autoFocus
              value={magnet}
              onChange={(e) => { setMagnet(e.target.value); setError(null) }}
              onKeyDown={(e) => { if (e.key === 'Enter') submit() }}
              placeholder="magnet:?xt=urn:btih:…"
              className="w-full rounded-control border border-separator bg-elevated px-3 py-2 text-subhead text-label placeholder:text-tertiary focus:border-blue focus:outline focus:outline-[3px] focus:outline-blue/30"
            />
          )}

          {error && <p className="mt-3 text-subhead text-red">{error}</p>}
        </div>

        {/* Trailing-aligned dismiss and confirm, default action last. */}
        <div className="flex justify-end gap-2 border-t border-separator px-6 py-4">
          <button
            onClick={onClose}
            className="h-9 rounded-control bg-fill px-4 text-subhead font-medium text-label transition active:bg-fill-strong"
          >
            Cancel
          </button>
          <button
            onClick={submit}
            disabled={busy}
            className="h-9 rounded-control bg-blue px-4 text-subhead font-medium text-white transition active:bg-blue/80 disabled:opacity-40"
          >
            {busy ? 'Adding…' : 'Add'}
          </button>
        </div>
      </div>
    </div>
  )
}
