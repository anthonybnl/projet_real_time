'use client'
import { useRef, forwardRef, useImperativeHandle } from 'react'

export interface GlobalStats {
  avg_price_since_start?: number
  total_trades?: number
  uptime_seconds?: number
}

export interface GlobalChartHandle {
  push(global: GlobalStats): void
  reset(): void
}

function fmtUptime(s: number) {
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = Math.floor(s % 60)
  return (h ? `${h}h ` : '') + `${m.toString().padStart(2, '0')}m ${sec.toString().padStart(2, '0')}s`
}

const GlobalChart = forwardRef<GlobalChartHandle, Record<string, never>>((_, ref) => {
  const sessionAvgRef = useRef<HTMLSpanElement>(null)
  const tradesRef     = useRef<HTMLSpanElement>(null)
  const uptimeRef     = useRef<HTMLSpanElement>(null)

  useImperativeHandle(ref, () => ({
    push(global: GlobalStats) {
      if (sessionAvgRef.current && global.avg_price_since_start != null)
        sessionAvgRef.current.textContent = '$' + global.avg_price_since_start.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
      if (tradesRef.current && global.total_trades != null)
        tradesRef.current.textContent = global.total_trades.toLocaleString()
      if (uptimeRef.current && global.uptime_seconds != null)
        uptimeRef.current.textContent = fmtUptime(global.uptime_seconds)
    },
    reset() {
      if (sessionAvgRef.current) sessionAvgRef.current.textContent = '—'
      if (tradesRef.current)     tradesRef.current.textContent     = '—'
      if (uptimeRef.current)     uptimeRef.current.textContent     = '—'
    },
  }))

  return (
    <div className="bg-crypto-card border border-crypto-border rounded-xl p-4" style={{ borderWidth: '0.5px' }}>
      <div className="flex justify-between items-center mb-3">
        <span className="text-xs font-medium text-crypto-text">Session stats</span>
        <span className="text-[11px] text-crypto-dim">global · since start</span>
      </div>
      <div className="grid grid-cols-3 gap-3">
        <div className="flex flex-col gap-1">
          <span className="text-[10px] text-crypto-dim uppercase tracking-wide">Avg since start</span>
          <span ref={sessionAvgRef} className="text-sm font-mono tabular-nums text-crypto-text">—</span>
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-[10px] text-crypto-dim uppercase tracking-wide">Total trades</span>
          <span ref={tradesRef} className="text-sm font-mono tabular-nums text-crypto-text">—</span>
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-[10px] text-crypto-dim uppercase tracking-wide">Uptime</span>
          <span ref={uptimeRef} className="text-sm font-mono tabular-nums text-crypto-text">—</span>
        </div>
      </div>
    </div>
  )
})

GlobalChart.displayName = 'GlobalChart'
export default GlobalChart
