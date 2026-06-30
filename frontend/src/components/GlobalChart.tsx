'use client'
import { useRef, forwardRef, useImperativeHandle } from 'react'
import type { WindowAgg } from '@/types'

export interface GlobalChartHandle {
  push(w: WindowAgg): void
  reset(): void
}

const GlobalChart = forwardRef<GlobalChartHandle, Record<string, never>>((_, ref) => {
  const avgRef    = useRef<HTMLSpanElement>(null)
  const tradesRef = useRef<HTMLSpanElement>(null)
  const volRef    = useRef<HTMLSpanElement>(null)

  useImperativeHandle(ref, () => ({
    push(w: WindowAgg) {
      if (avgRef.current && w.avg_price != null)
        avgRef.current.textContent =
          '$' + w.avg_price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
      if (tradesRef.current) tradesRef.current.textContent = w.trades_count.toLocaleString()
      if (volRef.current)    volRef.current.textContent    = w.volume.toFixed(4)
    },
    reset() {
      if (avgRef.current)    avgRef.current.textContent    = '—'
      if (tradesRef.current) tradesRef.current.textContent = '—'
      if (volRef.current)    volRef.current.textContent    = '—'
    },
  }))

  return (
    <div className="bg-crypto-card border border-crypto-border rounded-xl p-4" style={{ borderWidth: '0.5px' }}>
      <div className="flex justify-between items-center mb-3">
        <span className="text-xs font-medium text-crypto-text">Last hour</span>
        <span className="text-[11px] text-crypto-dim">rolling 1h window</span>
      </div>
      <div className="grid grid-cols-3 gap-3">
        <div className="flex flex-col gap-1">
          <span className="text-[10px] text-crypto-dim uppercase tracking-wide">Avg price (1h)</span>
          <span ref={avgRef} className="text-sm font-mono tabular-nums text-crypto-text">—</span>
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-[10px] text-crypto-dim uppercase tracking-wide">Trades (1h)</span>
          <span ref={tradesRef} className="text-sm font-mono tabular-nums text-crypto-text">—</span>
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-[10px] text-crypto-dim uppercase tracking-wide">Volume (1h)</span>
          <span ref={volRef} className="text-sm font-mono tabular-nums text-crypto-text">—</span>
        </div>
      </div>
    </div>
  )
})

GlobalChart.displayName = 'GlobalChart'
export default GlobalChart
