/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      fontFamily: {
        // San Francisco on Apple platforms; graceful fallbacks elsewhere.
        sans: ['-apple-system', 'BlinkMacSystemFont', 'SF Pro Text', 'Helvetica Neue', 'Helvetica', 'Arial', 'sans-serif'],
      },
      colors: {
        blue:   'rgb(var(--system-blue) / <alpha-value>)',
        green:  'rgb(var(--system-green) / <alpha-value>)',
        red:    'rgb(var(--system-red) / <alpha-value>)',
        orange: 'rgb(var(--system-orange) / <alpha-value>)',

        label:     'var(--label)',
        secondary: 'var(--secondary-label)',
        tertiary:  'var(--tertiary-label)',

        separator: 'var(--separator)',
        grouped:   'rgb(var(--grouped-background) / <alpha-value>)',
        elevated:  'rgb(var(--system-background) / <alpha-value>)',

        'fill-strong': 'var(--system-fill)',
        fill:          'var(--tertiary-fill)',
        'fill-sm':     'var(--quaternary-fill)',
      },
      fontSize: {
        // iOS/iPadOS Dynamic Type. The HIG puts the default body size at 17pt
        // with an 11pt floor; this is a browser UI read at arm's length, so
        // the 17pt scale fits better than the denser 13pt macOS one.
        'large-title': ['34px', { lineHeight: '41px', letterSpacing: '-0.01em' }],
        title1:        ['28px', { lineHeight: '34px', letterSpacing: '-0.01em' }],
        title2:        ['22px', { lineHeight: '28px' }],
        title3:        ['20px', { lineHeight: '25px' }],
        headline:      ['17px', { lineHeight: '22px', fontWeight: '600' }],
        body:          ['17px', { lineHeight: '22px' }],
        callout:       ['16px', { lineHeight: '21px' }],
        subhead:       ['15px', { lineHeight: '20px' }],
        footnote:      ['13px', { lineHeight: '18px' }],
        caption:       ['12px', { lineHeight: '16px' }],
      },
      borderRadius: {
        // Concentric radii: grouped containers 10, controls 6-7, alerts 12.
        control: '6px',
        group: '10px',
        alert: '12px',
      },
      keyframes: {
        'sheet-in':  { from: { opacity: '0', transform: 'translateY(-10px)' }, to: { opacity: '1', transform: 'translateY(0)' } },
        'alert-in':  { from: { opacity: '0', transform: 'scale(1.08)' },       to: { opacity: '1', transform: 'scale(1)' } },
        'fade-in':   { from: { opacity: '0' },                                  to: { opacity: '1' } },
        // Indeterminate progress bar, for tasks of unknown duration.
        indeterminate: { from: { transform: 'translateX(-60%)' }, to: { transform: 'translateX(260%)' } },
      },
      animation: {
        'sheet-in': 'sheet-in 0.22s cubic-bezier(0.32, 0.72, 0, 1)',
        'alert-in': 'alert-in 0.18s cubic-bezier(0.32, 0.72, 0, 1)',
        'fade-in':  'fade-in 0.15s ease-out',
        indeterminate: 'indeterminate 1.4s ease-in-out infinite',
      },
    },
  },
  plugins: [],
}
