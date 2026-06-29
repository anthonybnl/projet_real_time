'use client'
import { useEffect, useRef } from 'react'
import gsap from 'gsap'
import type { AlertData } from '@/types'

function fmt(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString('en-US', {
    hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

const STYLES = {
  large_trade: {
    accent:    '#ef4444',
    bg:        'rgba(239,68,68,0.07)',
    badgeBg:   'rgba(239,68,68,0.18)',
    badgeText: '#fca5a5',
    label:     'WHALE',
  },
  price_spike: {
    accent:    '#f59e0b',
    bg:        'rgba(245,158,11,0.07)',
    badgeBg:   'rgba(245,158,11,0.18)',
    badgeText: '#fcd34d',
    label:     'SPIKE',
  },
  volume_spike: {
    accent:    '#3b82f6',
    bg:        'rgba(59,130,246,0.07)',
    badgeBg:   'rgba(59,130,246,0.18)',
    badgeText: '#93c5fd',
    label:     'VOL',
  },
} satisfies Record<AlertData['type'], object>

function alertBadgeLabel(a: AlertData): string {
  if (a.type === 'price_spike') {
    const dir = (a.variation_pct ?? 0) >= 0 ? '▲' : '▼'
    return `${dir} SPIKE`
  }
  if (a.type === 'large_trade') return '◆ WHALE'
  return '≈ VOL'
}

function alertMessage(a: AlertData): string {
  switch (a.type) {
    case 'large_trade':
      return `$${a.notional?.toLocaleString('en-US', { maximumFractionDigits: 0 })} notional · ${a.symbol}`
    case 'price_spike':
      return `${(a.variation_pct ?? 0) > 0 ? '+' : ''}${a.variation_pct?.toFixed(2)}% move · ${a.symbol}`
    case 'volume_spike':
      return `${a.sigma?.toFixed(1)}σ above avg · ${a.symbol}`
  }
}

function AlertRow({ alert }: { alert: AlertData }) {
  const rowRef = useRef<HTMLDivElement>(null)
  const s = STYLES[alert.type]

  useEffect(() => {
    if (!rowRef.current) return
    gsap.fromTo(
      rowRef.current,
      { x: -20, opacity: 0 },
      { x: 0, opacity: 1, duration: 0.35, ease: 'power2.out' },
    )
  }, [])

  return (
    <div
      ref={rowRef}
      className="flex items-center gap-2.5 px-2.5 py-2 rounded-lg"
      style={{
        background:  s.bg,
        borderLeft:  `2.5px solid ${s.accent}`,
      }}
    >
      {/* Type badge — text only, no emoji */}
      <span
        className="shrink-0 text-[10px] font-bold px-1.5 py-0.5 rounded leading-tight tabular-nums"
        style={{ background: s.badgeBg, color: s.badgeText }}
      >
        {alertBadgeLabel(alert)}
      </span>

      {/* Message */}
      <span className="flex-1 text-[11px] text-crypto-text truncate leading-tight">
        {alertMessage(alert)}
      </span>

      {/* Timestamp */}
      <span className="shrink-0 text-[10px] text-crypto-dim font-mono">
        {fmt(alert.timestamp)}
      </span>
    </div>
  )
}

interface AlertsListProps { alerts: AlertData[] }

function AlertsList({ alerts }: AlertsListProps) {
  return (
    <div
      className="bg-crypto-card border border-crypto-border rounded-xl p-4 flex flex-col"
      style={{ borderWidth: '0.5px' }}
    >
      <div className="flex justify-between items-center mb-3">
        <span className="text-xs font-medium text-crypto-text">Anomaly alerts</span>
        <div className="flex items-center gap-2 text-[10px] text-crypto-dim">
          <span className="w-1.5 h-1.5 rounded-full bg-crypto-green animate-pulse inline-block" />
          Live
        </div>
      </div>

      <div className="flex flex-col gap-1.5 overflow-y-auto max-h-48">
        {alerts.length === 0 ? (
          <p className="text-[11px] text-crypto-dim text-center py-4">No anomalies detected</p>
        ) : (
          alerts.map((a) => <AlertRow key={`${a.type}-${a.timestamp}`} alert={a} />)
        )}
      </div>
    </div>
  )
}

export default AlertsList
