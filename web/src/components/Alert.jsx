import { useEffect } from 'react'

/**
 * A modal alert in the macOS style: centered title, informative text, and up
 * to three stacked buttons with the default action first. Per the HIG, use it
 * only for uncommon destructive actions that can't be undone.
 */
export default function Alert({ title, message, actions = [], onCancel, cancelLabel = 'Cancel' }) {
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onCancel() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onCancel])

  return (
    <div
      onClick={onCancel}
      className="fixed inset-0 z-50 flex animate-fade-in items-center justify-center bg-black/20 px-4"
    >
      <div
        onClick={(e) => e.stopPropagation()}
        role="alertdialog"
        aria-modal="true"
        aria-label={title}
        className="w-[320px] animate-alert-in rounded-alert bg-elevated p-5 text-center shadow-[0_10px_40px_rgb(0_0_0/0.22)]"
      >
        <h2 className="text-headline">{title}</h2>
        {message && <p className="mt-2 text-footnote text-secondary">{message}</p>}

        <div className="mt-5 space-y-2">
          {actions.map((a) => (
            <button
              key={a.label}
              onClick={a.onClick}
              className={`h-9 w-full rounded-control px-3 text-subhead font-medium transition ${
                a.primary
                  ? 'bg-blue text-white active:bg-blue/80'
                  : a.destructive
                    ? 'bg-fill text-red active:bg-fill-strong'
                    : 'bg-fill text-label active:bg-fill-strong'
              }`}
            >
              {a.label}
            </button>
          ))}
          <button
            onClick={onCancel}
            autoFocus
            className="h-9 w-full rounded-control px-3 text-subhead font-medium text-label transition hover:bg-fill-sm active:bg-fill"
          >
            {cancelLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
