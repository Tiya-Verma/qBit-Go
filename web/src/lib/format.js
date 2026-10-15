const SIZE_UNITS = ['bytes', 'KB', 'MB', 'GB', 'TB']

export function fmtSize(bytes) {
  const n = Number(bytes) || 0
  if (n < 1024) return `${n} bytes`
  const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), SIZE_UNITS.length - 1)
  const v = n / Math.pow(1024, i)
  return `${v < 10 ? v.toFixed(1) : Math.round(v)} ${SIZE_UNITS[i]}`
}

export function fmtSpeed(bps) {
  const n = Number(bps) || 0
  if (n === 0) return '0 KB/s'
  return `${fmtSize(n).replace('bytes', 'B')}/s`
}

// fmtRemaining spells out the estimate. The HIG asks for specific, accurate
// descriptions over vague ones, so this reads "About 8 minutes remaining".
export function fmtRemaining(remaining, bps) {
  if (!bps || remaining <= 0) return null
  const s = Math.round(remaining / bps)
  if (s < 60) return `About ${s} seconds remaining`
  const unit = (n, word) => `About ${n} ${word}${n === 1 ? '' : 's'} remaining`
  if (s < 3600) return unit(Math.round(s / 60), 'minute')
  if (s < 86400) return unit(Math.round(s / 3600), 'hour')
  return unit(Math.round(s / 86400), 'day')
}

// An ellipsis in the middle keeps both the beginning and the end of a name
// recognizable, which matters for torrent filenames that differ only in their
// trailing release tags. Used to keep alert titles within two lines.
export function middleTruncate(text, max = 44) {
  const s = String(text ?? '')
  if (s.length <= max) return s
  const head = Math.ceil((max - 1) / 2)
  const tail = Math.floor((max - 1) / 2)
  return `${s.slice(0, head)}…${s.slice(s.length - tail)}`
}
