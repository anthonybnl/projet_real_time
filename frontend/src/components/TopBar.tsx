'use client'
import { useEffect, useRef } from 'react'
import gsap from 'gsap'
import { LIVE_SYMBOL } from '@/types'
import { useTheme } from '@/contexts/ThemeContext'

interface TopBarProps {
  connected: boolean
  exchange: string
}

function SunIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="5" />
      <line x1="12" y1="1"  x2="12" y2="3" />
      <line x1="12" y1="21" x2="12" y2="23" />
      <line x1="4.22" y1="4.22"   x2="5.64"  y2="5.64" />
      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
      <line x1="1"  y1="12" x2="3"  y2="12" />
      <line x1="21" y1="12" x2="23" y2="12" />
      <line x1="4.22"  y1="19.78" x2="5.64"  y2="18.36" />
      <line x1="18.36" y1="5.64"  x2="19.78" y2="4.22" />
    </svg>
  )
}

function MoonIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  )
}

export default function TopBar({ connected, exchange }: TopBarProps) {
  const { theme, toggle } = useTheme()
  const dotRef    = useRef<HTMLSpanElement>(null)
  const iconRef   = useRef<HTMLSpanElement>(null)
  const [utc, setUtc] = [useRef(''), useRef<ReturnType<typeof setInterval>>()]

  // UTC clock — imperative to avoid re-renders
  const clockRef = useRef<HTMLSpanElement>(null)
  useEffect(() => {
    const tick = () => {
      if (clockRef.current)
        clockRef.current.textContent = new Date().toUTCString().slice(17, 25) + ' UTC'
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [])

  // Live dot pulse
  useEffect(() => {
    if (!dotRef.current) return
    gsap.to(dotRef.current, { scale: 1.5, opacity: 0.4, repeat: -1, yoyo: true, duration: 0.8, ease: 'power1.inOut' })
  }, [])

  const handleToggle = () => {
    if (iconRef.current) {
      gsap.fromTo(iconRef.current,
        { rotate: 0, scale: 1 },
        { rotate: 180, scale: 1.3, duration: 0.3, ease: 'back.out(2)',
          onComplete: () => gsap.to(iconRef.current, { scale: 1, duration: 0.15 }) }
      )
    }
    toggle()
  }

  return (
    <div className="flex items-center justify-between px-5 py-3 border-b border-crypto-border bg-crypto-card">
      {/* Left: logo + pair tabs */}
      <div className="flex items-center gap-5">
        <span className="text-crypto-text font-semibold text-sm tracking-wide flex items-center gap-2">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-crypto-accent">
            <polyline points="22 7 13.5 15.5 8.5 10.5 2 17" />
            <polyline points="16 7 22 7 22 13" />
          </svg>
          CryptoStream
        </span>

        {/* Vue unique BTC (binance + coinbase agreges) */}
        <span className="px-3 py-1 rounded-md text-xs font-medium bg-crypto-accent text-white">
          {LIVE_SYMBOL}
        </span>
      </div>

      {/* Right: theme toggle + status + clock */}
      <div className="flex items-center gap-3 text-xs text-crypto-dim">
        {/* Theme toggle */}
        <button
          onClick={handleToggle}
          className="p-1.5 rounded-md text-crypto-dim hover:text-crypto-text hover:bg-crypto-hover transition-colors"
          title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
        >
          <span ref={iconRef} className="inline-flex">
            {theme === 'dark' ? <SunIcon /> : <MoonIcon />}
          </span>
        </button>

        {/* Connection status */}
        <span className="flex items-center gap-1.5">
          <span
            ref={dotRef}
            className="inline-block w-2 h-2 rounded-full"
            style={{ background: connected ? 'var(--crypto-green)' : 'var(--crypto-red)' }}
          />
          {connected ? `Live — ${exchange}` : 'Reconnecting…'}
        </span>

        {/* UTC clock */}
        <span className="px-3 py-1.5 rounded-md bg-crypto-bg border border-crypto-border font-mono text-[11px]">
          <span ref={clockRef} />
        </span>
      </div>
    </div>
  )
}
