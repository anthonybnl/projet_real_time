'use client'
import { useEffect, useRef, forwardRef, useImperativeHandle, useCallback } from 'react'
import {
  Chart,
  BarController,
  BarElement,
  LinearScale,
  CategoryScale,
  Tooltip,
} from 'chart.js'
import { useTheme } from '@/contexts/ThemeContext'

Chart.register(BarController, BarElement, LinearScale, CategoryScale, Tooltip)

const TV_FONT    = "'Trebuchet MS', Roboto, Ubuntu, sans-serif"
const BUCKET_COUNT   = 6
const BUCKET_SECONDS = 10

export interface VolumeChartHandle {
  push(volume: number, timestamp: number, side?: 'buy' | 'sell' | 'unknown'): void
  reset(): void
}

interface VolumeEntry { volume: number; timestamp: number; side: 'buy' | 'sell' | 'unknown' }

const LABELS = Array.from({ length: BUCKET_COUNT }, (_, i) =>
  `${i * BUCKET_SECONDS}–${(i + 1) * BUCKET_SECONDS}s`
)

const CHART_C = {
  dark:  { tooltipBg: '#0d1526', tooltipBorder: '#1a2540', text: '#cdd6f4', dim: '#6b7a99', border: '#1a2540', grid: 'rgba(26,37,64,0.6)'    },
  light: { tooltipBg: '#ffffff', tooltipBorder: '#d0dbf0', text: '#0f1729', dim: '#5a6a8a', border: '#d0dbf0', grid: 'rgba(208,219,240,0.6)' },
}

const VolumeChart = forwardRef<VolumeChartHandle, { symbol: string }>(({ symbol }, ref) => {
  const { theme }     = useTheme()
  const canvasRef     = useRef<HTMLCanvasElement>(null)
  const chartRef      = useRef<Chart | null>(null)
  const entriesRef    = useRef<VolumeEntry[]>([])
  const windowStart   = useRef(Date.now())
  const lastBuy       = useRef<number[]>(Array(BUCKET_COUNT).fill(0))
  const lastSell      = useRef<number[]>(Array(BUCKET_COUNT).fill(0))
  const deltaBadgeRef = useRef<HTMLSpanElement>(null)
  const totalBadgeRef = useRef<HTMLSpanElement>(null)

  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    const c = CHART_C[theme]
    const o = chart.options
    ;(o.plugins!.tooltip as any).backgroundColor = c.tooltipBg
    ;(o.plugins!.tooltip as any).borderColor     = c.tooltipBorder
    ;(o.plugins!.tooltip as any).titleColor      = c.dim
    ;(o.plugins!.tooltip as any).bodyColor       = c.text
    ;(o.scales!.x!.ticks as any).color           = c.dim
    ;(o.scales!.x!.border as any).color          = c.border
    ;(o.scales!.y!.ticks as any).color           = c.dim
    ;(o.scales!.y!.border as any).color          = c.border
    ;(o.scales!.y!.grid  as any).color           = c.grid
    chart.update('none')
  }, [theme])

  const computeBuckets = useCallback(() => {
    const now  = Date.now()
    const buy  = Array(BUCKET_COUNT).fill(0)
    const sell = Array(BUCKET_COUNT).fill(0)
    const elapsed = (now - windowStart.current) / 1000

    if (elapsed >= BUCKET_COUNT * BUCKET_SECONDS) {
      windowStart.current = now
      entriesRef.current  = []
      return { buy, sell }
    }

    for (const { volume, timestamp, side } of entriesRef.current) {
      const age = (timestamp * 1000 - windowStart.current) / 1000
      if (age < 0) continue
      const idx = Math.min(Math.floor(age / BUCKET_SECONDS), BUCKET_COUNT - 1)
      if      (side === 'buy')  buy[idx]  += volume
      else if (side === 'sell') sell[idx] += volume
      else { buy[idx] += volume / 2; sell[idx] += volume / 2 }
    }

    return {
      buy:  buy.map( (v) => Math.round(v * 10000) / 10000),
      sell: sell.map((v) => Math.round(v * 10000) / 10000),
    }
  }, [])

  const applyBuckets = useCallback(() => {
    const chart = chartRef.current
    if (!chart) return
    const { buy, sell } = computeBuckets()
    lastBuy.current  = buy
    lastSell.current = sell

    // Dataset 0: buy (positive, going up)
    // Dataset 1: sell (negative, going down — mirrored)
    chart.data.datasets[0].data = buy
    chart.data.datasets[1].data = sell
    chart.update('none')

    // Delta badge
    const totalBuy  = buy.reduce( (a, b) => a + b, 0)
    const totalSell = sell.reduce((a, b) => a + b, 0)
    const delta = totalBuy - totalSell
    if (deltaBadgeRef.current) {
      const sign = delta >= 0 ? '+' : ''
      deltaBadgeRef.current.textContent = `Δ ${sign}${delta.toFixed(3)}`
      deltaBadgeRef.current.style.color = delta >= 0 ? '#22c55e' : '#ef4444'
    }
    if (totalBadgeRef.current) {
      const total = totalBuy + totalSell
      totalBadgeRef.current.textContent = total > 0 ? `${total.toFixed(3)} total` : ''
    }
  }, [computeBuckets])

  useEffect(() => {
    if (!canvasRef.current) return
    const ctx = canvasRef.current.getContext('2d')!

    chartRef.current = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: LABELS,
        datasets: [
          // Dataset 0: buy (positive bars, green)
          {
            label: 'Buy',
            data: Array(BUCKET_COUNT).fill(0),
            backgroundColor: 'rgba(34,197,94,0.45)',
            borderColor:     'rgba(34,197,94,0.75)',
            borderWidth: 1,
            borderRadius: { topLeft: 3, topRight: 3, bottomLeft: 0, bottomRight: 0 },
            borderSkipped: 'bottom',
          },
          // Dataset 1: sell (positive bars, red — side-by-side with buy)
          {
            label: 'Sell',
            data: Array(BUCKET_COUNT).fill(0),
            backgroundColor: 'rgba(239,68,68,0.45)',
            borderColor:     'rgba(239,68,68,0.75)',
            borderWidth: 1,
            borderRadius: { topLeft: 3, topRight: 3, bottomLeft: 0, bottomRight: 0 },
            borderSkipped: 'bottom',
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 200 },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { display: false },
          tooltip: {
            enabled: true,
            backgroundColor: CHART_C.dark.tooltipBg,
            borderColor: CHART_C.dark.tooltipBorder,
            borderWidth: 1,
            titleColor: CHART_C.dark.dim,
            bodyColor: CHART_C.dark.text,
            padding: 10,
            displayColors: true,
            boxWidth: 8,
            boxHeight: 8,
            titleFont: { family: TV_FONT, size: 11 },
            bodyFont:  { family: TV_FONT, size: 11 },
            callbacks: {
              title: (items) => items[0]?.label ?? '',
              label: (ctx) => {
                const i    = ctx.dataIndex
                const b    = lastBuy.current[i]  ?? 0
                const s    = lastSell.current[i] ?? 0
                const total = b + s
                if (ctx.datasetIndex === 0) {
                  const pct = total > 0 ? ((b / total) * 100).toFixed(0) : '0'
                  return `  Buy  ▲  ${b.toFixed(4)}  (${pct}%)`
                }
                const pct = total > 0 ? ((s / total) * 100).toFixed(0) : '0'
                return `  Sell  ▼  ${s.toFixed(4)}  (${pct}%)`
              },
              afterBody: (items) => {
                const i     = items[0]?.dataIndex ?? 0
                const b     = lastBuy.current[i]  ?? 0
                const s     = lastSell.current[i] ?? 0
                const delta = b - s
                const sign  = delta >= 0 ? '+' : ''
                return [`  Delta  ${sign}${delta.toFixed(4)}`]
              },
            },
          },
        },
        scales: {
          x: {
            stacked: false,
            ticks: { color: CHART_C.dark.dim, font: { size: 10, family: TV_FONT } },
            grid:  { display: false },
            border: { color: CHART_C.dark.border },
          },
          y: {
            stacked: false,
            ticks: {
              color: CHART_C.dark.dim,
              font:  { size: 10, family: TV_FONT },
              callback: (v) => Number(v).toFixed(2),
            },
            grid:  { color: CHART_C.dark.grid },
            border: { color: CHART_C.dark.border },
          },
        },
      },
    })

    const interval = setInterval(applyBuckets, 1000)
    return () => {
      clearInterval(interval)
      chartRef.current?.destroy()
      chartRef.current = null
    }
  }, [applyBuckets])

  useImperativeHandle(ref, () => ({
    push(volume: number, timestamp: number, side: 'buy' | 'sell' | 'unknown' = 'unknown') {
      entriesRef.current.push({ volume, timestamp, side })
      applyBuckets()
    },
    reset() {
      entriesRef.current  = []
      windowStart.current = Date.now()
      lastBuy.current     = Array(BUCKET_COUNT).fill(0)
      lastSell.current    = Array(BUCKET_COUNT).fill(0)
      const chart = chartRef.current
      if (!chart) return
      chart.data.datasets[0].data = Array(BUCKET_COUNT).fill(0)
      chart.data.datasets[1].data = Array(BUCKET_COUNT).fill(0)
      chart.update('none')
      if (deltaBadgeRef.current) deltaBadgeRef.current.textContent = ''
      if (totalBadgeRef.current) totalBadgeRef.current.textContent = ''
    },
  }))

  return (
    <div className="bg-crypto-card border border-crypto-border rounded-xl p-4" style={{ borderWidth: '0.5px' }}>
      <div className="flex justify-between items-center mb-3">
        <div className="flex items-center gap-3">
          <span className="text-xs font-medium text-crypto-text">Volume per 10s</span>
          {/* Buy/sell legend */}
          <span className="flex items-center gap-2 text-[10px]">
            <span className="flex items-center gap-1">
              <span style={{ width: 7, height: 7, borderRadius: 2, background: 'rgba(34,197,94,0.7)', display: 'inline-block' }} />
              <span className="text-crypto-dim">buy ▲</span>
            </span>
            <span className="flex items-center gap-1">
              <span style={{ width: 7, height: 7, borderRadius: 2, background: 'rgba(239,68,68,0.7)', display: 'inline-block' }} />
              <span className="text-crypto-dim">sell ▼</span>
            </span>
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span ref={deltaBadgeRef} className="text-[11px] font-mono tabular-nums" />
          <span ref={totalBadgeRef} className="text-[11px] font-mono tabular-nums text-crypto-dim" />
          <span className="text-[11px] text-crypto-dim">{symbol}</span>
        </div>
      </div>
      <div style={{ position: 'relative', height: 200 }}>
        <canvas ref={canvasRef} />
      </div>
    </div>
  )
})

VolumeChart.displayName = 'VolumeChart'
export default VolumeChart
