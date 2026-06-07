import { useState, useRef } from 'react'
import { addTorrentFile, addMagnet } from '../api.js'

export default function AddModal({ onClose, onAdded }) {
  const [tab, setTab] = useState('file')
  const [magnet, setMagnet] = useState('')
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const fileRef = useRef(null)

  async function handleFile() {
    const file = fileRef.current?.files?.[0]
    if (!file) { setError('Select a .torrent file'); return }
    setBusy(true); setError(null)
    try {
      await addTorrentFile(file)
      onAdded()
    } catch (e) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  async function handleMagnet() {
    if (!magnet.trim()) { setError('Enter a magnet link'); return }
    setBusy(true); setError(null)
    try {
      await addMagnet(magnet.trim())
      onAdded()
    } catch (e) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
      <div className="bg-gray-900 border border-gray-700 rounded-xl w-full max-w-md mx-4 shadow-2xl">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-800">
          <h2 className="font-semibold text-gray-100">Add Torrent</h2>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-300 text-xl leading-none"
          >
            ×
          </button>
        </div>

        <div className="flex border-b border-gray-800">
          {['file', 'magnet'].map((t) => (
            <button
              key={t}
              onClick={() => { setTab(t); setError(null) }}
              className={`flex-1 py-2.5 text-sm font-medium transition-colors ${
                tab === t
                  ? 'text-green-400 border-b-2 border-green-500'
                  : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              {t === 'file' ? '.torrent File' : 'Magnet Link'}
            </button>
          ))}
        </div>

        <div className="p-5 space-y-4">
          {tab === 'file' ? (
            <div>
              <label className="block text-sm text-gray-400 mb-2">Select .torrent file</label>
              <input
                type="file"
                accept=".torrent"
                ref={fileRef}
                className="block w-full text-sm text-gray-400 file:mr-3 file:py-1.5 file:px-3 file:rounded file:border-0 file:bg-gray-700 file:text-gray-200 file:text-sm hover:file:bg-gray-600 cursor-pointer"
              />
            </div>
          ) : (
            <div>
              <label className="block text-sm text-gray-400 mb-2">Magnet URI</label>
              <input
                type="text"
                value={magnet}
                onChange={(e) => setMagnet(e.target.value)}
                placeholder="magnet:?xt=urn:btih:..."
                className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:border-green-600"
              />
            </div>
          )}

          {error && (
            <p className="text-red-400 text-xs">{error}</p>
          )}

          <div className="flex justify-end gap-3 pt-1">
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm text-gray-400 hover:text-gray-200 transition-colors"
            >
              Cancel
            </button>
            <button
              disabled={busy}
              onClick={tab === 'file' ? handleFile : handleMagnet}
              className="px-4 py-2 text-sm font-medium bg-green-600 hover:bg-green-500 text-white rounded transition-colors disabled:opacity-50"
            >
              {busy ? 'Adding…' : 'Add'}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
