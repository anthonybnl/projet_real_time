'use client'
import type { WindowStats } from '@/types'

interface MarketOverviewProps {
  stats60: Partial<WindowStats>
  stats300: Partial<WindowStats>
}

function Bar({ pct, color }: { pct: number; color: string }) {
  return (
    <div className="w-full h-1.5 rounded-full bg-crypto-bg overflow-hidden">
      <div className="h-full rounded-full transition-all duration-500" style={{ width: `${pct}%`, background: color }} />
    </div>
  )
}

export default function MarketOverview({ stats60, stats300 }: MarketOverviewProps) {
  const buyVol = stats300.vol_buy ?? 0
  const sellVol = stats300.vol_sell ?? 0
  const total = buyVol + sellVol
  const buyPct = total > 0 ? Math.round((buyVol / total) * 100) : 50
  const sellPct = 100 - buyPct

  const fmt = (n?: number) =>
    n !== undefined ? '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '—'

  const fmtVol = (n?: number) =>
    n !== undefined ? n.toFixed(4) : '—'

  const fmtNotional = (n?: number) =>
    n !== undefined
      ? '$' + (n >= 1_000_000 ? (n / 1_000_000).toFixed(2) + 'M' : n.toLocaleString('en-US', { maximumFractionDigits: 0 }))
      : '—'

  return (
    <div className="bg-crypto-card border border-crypto-border rounded-xl p-4 flex flex-col gap-3" style={{ borderWidth: '0.5px' }}>
      <div className="flex justify-between items-center">
        <span className="text-xs font-medium text-crypto-text">Market overview</span>
        <span className="text-[11px] text-crypto-dim">5 min</span>
      </div>

      {/* High / Low */}
      <div className="grid grid-cols-2 gap-2">
        <div className="bg-crypto-bg rounded-lg px-3 py-2">
          <div className="text-[10px] text-crypto-dim mb-0.5">High (5m)</div>
          <div className="text-sm font-mono text-crypto-green">{fmt(stats60.high)}</div>
        </div>
        <div className="bg-crypto-bg rounded-lg px-3 py-2">
          <div className="text-[10px] text-crypto-dim mb-0.5">Low (5m)</div>
          <div className="text-sm font-mono text-crypto-red">{fmt(stats60.low)}</div>
        </div>
      </div>

      {/* VWAP 5min */}
      <div className="bg-crypto-bg rounded-lg px-3 py-2">
        <div className="text-[10px] text-crypto-dim mb-0.5">VWAP (5 min)</div>
        <div className="text-sm font-mono text-crypto-text">{fmt(stats300.vwap)}</div>
      </div>

      {/* Volume 5min */}
      <div className="bg-crypto-bg rounded-lg px-3 py-2">
        <div className="flex justify-between items-center">
          <div>
            <div className="text-[10px] text-crypto-dim mb-0.5">Volume (5 min)</div>
            <div className="text-sm font-mono text-crypto-text">{fmtVol(stats300.total_volume)}</div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-crypto-dim mb-0.5">Notional</div>
            <div className="text-sm font-mono text-crypto-accent">{fmtNotional(stats300.total_notional)}</div>
          </div>
        </div>
      </div>

      {/* Buy / Sell ratio */}
      <div>
        <div className="flex justify-between text-[10px] mb-1.5">
          <span className="text-crypto-green">Buy {buyPct}%</span>
          <span className="text-crypto-red">Sell {sellPct}%</span>
        </div>
        <div className="flex gap-0.5">
          <div
            className="h-1.5 rounded-l-full transition-all duration-500"
            style={{ width: `${buyPct}%`, background: '#22c55e' }}
          />
          <div
            className="h-1.5 rounded-r-full transition-all duration-500"
            style={{ width: `${sellPct}%`, background: '#ef4444' }}
          />
        </div>
      </div>

      {/* Trade count */}
      <div className="flex justify-between text-[11px]">
        <span className="text-crypto-dim">Trades (5 min)</span>
        <span className="font-mono text-crypto-text">{stats60.count ?? 0}</span>
      </div>
    </div>
  )
}
