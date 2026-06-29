import type { Config } from 'tailwindcss'

const config: Config = {
  content: ['./src/**/*.{js,ts,jsx,tsx,mdx}'],
  theme: {
    extend: {
      colors: {
        crypto: {
          bg:     'var(--crypto-bg)',
          card:   'var(--crypto-card)',
          border: 'var(--crypto-border)',
          hover:  'var(--crypto-hover)',
          text:   'var(--crypto-text)',
          dim:    'var(--crypto-dim)',
          accent: 'var(--crypto-accent)',
          green:  'var(--crypto-green)',
          red:    'var(--crypto-red)',
          yellow: 'var(--crypto-yellow)',
        },
      },
      fontFamily: {
        mono: ['Courier New', 'monospace'],
      },
      animation: {
        'pulse-slow': 'pulse 2s cubic-bezier(0.4,0,0.6,1) infinite',
      },
    },
  },
  plugins: [],
}

export default config
