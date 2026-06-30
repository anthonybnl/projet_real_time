'use client'
import { useEffect, useRef, useState, forwardRef, useImperativeHandle } from 'react'
import {
  createChart,
  ColorType,
  LineStyle,
  CrosshairMode,
  type IChartApi,
  type ISeriesApi,
  type Time,
} from 'lightweight-charts'
import { useTheme } from '@/contexts/ThemeContext'

const MAX_POINTS  = 120   // history kept in buffer (couleur/delta de reference)
const TV_FONT = "'Trebuchet MS', Roboto, Ubuntu, sans-serif"
const MA_COLOR = '#3b82f6'  // MMA 5 min (bleu plein)
const UP_COLOR   = '#22c55e'
const DOWN_COLOR = '#ef4444'

interface Candle {
  open:   number
  high:   number
  low:    number
  close:  number
  ma:     number | null  // moyenne mobile 5 min
  volume: number
  time:   number         // Unix seconds
}

const TC = {
  dark: {
    text: '#6b7a99', grid: 'rgba(26,37,64,0.55)', border: '#1a2540',
    labelBg: '#1a2540',
    tooltip: { bg: 'rgba(13,21,38,0.92)', border: '#1a2540', text: '#cdd6f4', dim: '#6b7a99' },
    btn:     { bg: '#111c35', border: '#1a2540', text: '#6b7a99' },
  },
  light: {
    text: '#5a6a8a', grid: 'rgba(208,219,240,0.6)', border: '#d0dbf0',
    labelBg: '#d0dbf0',
    tooltip: { bg: 'rgba(255,255,255,0.95)', border: '#d0dbf0', text: '#0f1729', dim: '#5a6a8a' },
    btn:     { bg: '#e4eaf8', border: '#d0dbf0', text: '#5a6a8a' },
  },
} as const

export interface PriceChartHandle {
  // Appele 1x/seconde : bougie 1s (prix=close, high/low 1s), MMA 5 min, volume 1s.
  push(price: number, high: number, low: number, ma: number | null, volume: number): void
  reset(): void
}

function fmt$(n: number) {
  return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

const PriceChart = forwardRef<PriceChartHandle, { symbol: string }>(({ symbol }, ref) => {
  const { theme }    = useTheme()
  const themeRef     = useRef(theme)
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef     = useRef<IChartApi | null>(null)
  const candleRef    = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const sessionRef   = useRef<ISeriesApi<'Line'> | null>(null)
  const volRef       = useRef<ISeriesApi<'Histogram'> | null>(null)
  const bufferRef    = useRef<Candle[]>([])
  const isLiveRef    = useRef(true)
  const legendRef = useRef<HTMLDivElement>(null)
  const badgeRef     = useRef<HTMLSpanElement>(null)
  const [isLive, setIsLive] = useState(true)

  useEffect(() => { themeRef.current = theme }, [theme])

  // ── Create chart ────────────────────────────────────────────────────────────
  useEffect(() => {
    const container = containerRef.current
    if (!container) return
    const tc = TC.dark   // initial dark theme; updated via applyOptions on theme change

    const chart = createChart(container, {
      width:  container.clientWidth,
      height: container.clientHeight,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor:  tc.text,
        fontSize:   11,
        fontFamily: TV_FONT,
      },
      grid: {
        vertLines: { visible: false },
        horzLines: { color: tc.grid, style: LineStyle.Solid },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: 'rgba(107,122,153,0.7)', style: LineStyle.Dashed, labelBackgroundColor: tc.labelBg },
        horzLine: { color: 'rgba(107,122,153,0.7)', style: LineStyle.Dashed, labelBackgroundColor: tc.labelBg },
      },
      rightPriceScale: {
        borderColor:   tc.border,
        scaleMargins:  { top: 0.06, bottom: 0.22 },
      },
      timeScale: {
        borderColor:                  tc.border,
        timeVisible:                  true,
        secondsVisible:               true,
        rightOffset:                  8,
        fixLeftEdge:                  false,
        fixRightEdge:                 false,
        lockVisibleTimeRangeOnResize: true,
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      handleScale:  { mouseWheel: true, axisPressedMouseMove: { time: true, price: true }, pinch: true },
    })

    // ── Candlestick: prix (bougies 1s) ──────────────────────────────────────
    const candleSeries = chart.addCandlestickSeries({
      upColor:       UP_COLOR,
      downColor:     DOWN_COLOR,
      borderUpColor: UP_COLOR,
      borderDownColor: DOWN_COLOR,
      wickUpColor:   UP_COLOR,
      wickDownColor: DOWN_COLOR,
      priceLineVisible:  true,
      priceLineStyle:    LineStyle.Dashed,
      priceLineColor:    'rgba(107,122,153,0.45)',
      lastValueVisible:  true,
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    })

    // ── Line: MMA 5 min (bleu plein) ────────────────────────────────────────
    const sessionSeries = chart.addLineSeries({
      color:     MA_COLOR,
      lineWidth: 2,
      lineStyle: LineStyle.Solid,
      crosshairMarkerVisible:          true,
      crosshairMarkerRadius:           3,
      crosshairMarkerBackgroundColor:  MA_COLOR,
      priceLineVisible:  false,
      lastValueVisible:  true,
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    })

    // ── Histogram: volume sub-pane ──────────────────────────────────────────
    const volSeries = chart.addHistogramSeries({
      color: 'rgba(34,197,94,0.45)',
      priceFormat: { type: 'volume' },
      priceScaleId: 'vol',
    })
    chart.priceScale('vol').applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
      borderVisible: false,
    })

    chartRef.current   = chart
    candleRef.current  = candleSeries
    sessionRef.current = sessionSeries
    volRef.current     = volSeries

    // ── TradingView-style fixed legend (top-left) ───────────────────────────
    chart.subscribeCrosshairMove((param) => {
      const legend = legendRef.current
      if (!legend) return
      if (!param.time || !param.point || param.point.x < 0 || param.point.y < 0) {
        legend.style.opacity = '0'
        return
      }
      const lp = param.seriesData.get(candleSeries)  as { open: number; close: number } | undefined
      const sp = param.seriesData.get(sessionSeries) as { value: number } | undefined
      const vp = param.seriesData.get(volSeries)     as { value: number } | undefined
      if (!lp) { legend.style.opacity = '0'; return }

      const closes = bufferRef.current.map((e) => e.close)
      const first  = closes[0] ?? lp.close
      const delta  = first > 0 ? ((lp.close - first) / first) * 100 : 0
      const sign   = delta >= 0 ? '+' : ''
      const lc     = lp.close >= lp.open ? UP_COLOR : DOWN_COLOR
      const curTc  = TC[themeRef.current].tooltip

      const t    = new Date((param.time as number) * 1000)
      const tStr = t.getHours().toString().padStart(2, '0') + ':' +
                   t.getMinutes().toString().padStart(2, '0') + ':' +
                   t.getSeconds().toString().padStart(2, '0')

      // Update legend content — position is fixed top-left, no cursor-following
      legend.style.background = curTc.bg
      legend.style.borderColor = curTc.border
      legend.innerHTML = `
        <span style="color:${curTc.dim};font-size:10px;margin-right:8px">${tStr}</span>
        <span style="display:inline-flex;align-items:center;gap:4px;margin-right:10px">
          <span style="width:9px;height:9px;background:${lc};display:inline-block;border-radius:2px"></span>
          <span style="color:${curTc.text};font-size:11px;font-weight:600">${fmt$(lp.close)}</span>
          <span style="color:${delta >= 0 ? UP_COLOR : DOWN_COLOR};font-size:10px">${sign}${delta.toFixed(3)}%</span>
        </span>
        ${sp ? `
        <span style="display:inline-flex;align-items:center;gap:4px;margin-right:10px">
          <span style="width:9px;height:2px;background:${MA_COLOR};display:inline-block;border-radius:1px"></span>
          <span style="color:${MA_COLOR};font-size:11px">${fmt$(sp.value)}</span>
        </span>` : ''}
        ${vp && vp.value > 0 ? `
        <span style="display:inline-flex;align-items:center;gap:4px">
          <span style="width:7px;height:7px;display:inline-block;border-radius:1px;background:rgba(107,122,153,0.6)"></span>
          <span style="color:${curTc.dim};font-size:10px">Vol ${vp.value.toFixed(3)}</span>
        </span>` : ''}
      `
      legend.style.opacity = '1'
    })

    // Detect user scrolling to turn off live mode
    chart.timeScale().subscribeVisibleLogicalRangeChange((_range) => {
      // lightweight-charts doesn't expose whether this was user-triggered vs programmatic,
      // so we rely on the Live toggle button for explicit control
    })

    // Responsive resize
    const ro = new ResizeObserver((entries) => {
      if (entries[0]) {
        chart.applyOptions({
          width:  entries[0].contentRect.width,
          height: entries[0].contentRect.height,
        })
      }
    })
    ro.observe(container)

    return () => {
      ro.disconnect()
      chart.remove()
      chartRef.current = candleRef.current = sessionRef.current = volRef.current = null
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Theme update ──────────────────────────────────────────────────────────
  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    const tc = TC[theme]
    chart.applyOptions({
      layout:    { textColor: tc.text },
      grid:      { horzLines: { color: tc.grid } },
      crosshair: {
        vertLine: { labelBackgroundColor: tc.labelBg },
        horzLine: { labelBackgroundColor: tc.labelBg },
      },
      rightPriceScale: { borderColor: tc.border },
      timeScale:       { borderColor: tc.border },
    })
  }, [theme])

  // ── Imperative handle ─────────────────────────────────────────────────────
  useImperativeHandle(ref, () => {
    const updateBadge = () => {
      const buf  = bufferRef.current
      const last = buf[buf.length - 1]
      if (!last || !badgeRef.current || buf.length < 2) return
      const first = buf[0].close
      const delta = first > 0 ? ((last.close - first) / first) * 100 : 0
      const sign  = delta >= 0 ? '+' : ''
      badgeRef.current.textContent = `${sign}${delta.toFixed(3)}%`
      badgeRef.current.style.color = last.close >= first ? UP_COLOR : DOWN_COLOR
    }

    const syncViewport = () => {
      const chart = chartRef.current
      if (!chart || !isLiveRef.current) return
      chart.timeScale().scrollToRealTime()
    }

    // Construit/met a jour la bougie de la seconde courante. high/low viennent de
    // l'agregat 1s (meches) ; open = close de la bougie precedente, close = prix 1s.
    const upsertBuffer = (price: number, high: number, low: number, ma: number | null, volume: number) => {
      const nowSec = Math.floor(Date.now() / 1000)
      const buf    = bufferRef.current
      const last   = buf[buf.length - 1]

      if (last && last.time === nowSec) {
        last.high   = Math.max(last.high, high, price)
        last.low    = Math.min(last.low, low, price)
        last.close  = price
        last.ma     = ma ?? last.ma
        last.volume = volume
      } else {
        const open = last ? last.close : price
        buf.push({
          time:   nowSec,
          open,
          high:   Math.max(high, open, price),
          low:    Math.min(low, open, price),
          close:  price,
          ma:     ma ?? null,
          volume,
        })
        if (buf.length > MAX_POINTS) buf.shift()
      }
    }

    // update() ne (re)dessine que le dernier point -> pas de redraw complet/seconde.
    const paint = () => {
      const last = bufferRef.current[bufferRef.current.length - 1]
      if (!last) return
      candleRef.current?.update({
        time:  last.time as Time,
        open:  last.open,
        high:  last.high,
        low:   last.low,
        close: last.close,
      })
      if (sessionRef.current && last.ma != null) {
        sessionRef.current.update({ time: last.time as Time, value: last.ma })
      }
      volRef.current?.update({
        time:  last.time as Time,
        value: last.volume,
        color: last.close >= last.open ? 'rgba(34,197,94,0.4)' : 'rgba(239,68,68,0.4)',
      })
    }

    return {
    push(price: number, high: number, low: number, ma: number | null, volume: number) {
      upsertBuffer(price, high, low, ma, volume)
      paint()
      updateBadge()
      syncViewport()
    },

    reset() {
      bufferRef.current = []
      candleRef.current?.setData([])
      sessionRef.current?.setData([])
      volRef.current?.setData([])
      if (badgeRef.current) badgeRef.current.textContent = ''
      if (legendRef.current) legendRef.current.style.opacity = '0'
    },
  }
  })

  const handleFit = () => chartRef.current?.timeScale().fitContent()

  const handleLiveToggle = () => {
    const next = !isLive
    setIsLive(next)
    isLiveRef.current = next
    if (next) chartRef.current?.timeScale().scrollToRealTime()
  }

  const tc = TC[theme]

  return (
    <div className="bg-crypto-card border border-crypto-border rounded-xl p-4 flex flex-col" style={{ borderWidth: '0.5px' }}>

      {/* ── Header ── */}
      <div className="flex justify-between items-center mb-2">
        <div className="flex items-center gap-4">
          <span className="text-xs font-medium text-crypto-text">Prix — bougies 1s</span>
          <span className="flex items-center gap-3 text-[10px]">
            <span className="flex items-center gap-1.5">
              <span style={{ width: 8, height: 12, background: UP_COLOR, borderRadius: 1, display: 'inline-block', verticalAlign: 'middle' }} />
              <span className="text-crypto-dim">bougies</span>
            </span>
            <span className="flex items-center gap-1.5">
              <span style={{ width: 16, height: 0, borderBottom: `2px solid ${MA_COLOR}`, display: 'inline-block', verticalAlign: 'middle' }} />
              <span className="text-crypto-dim">MMA 5 min</span>
            </span>
            <span className="flex items-center gap-1.5">
              <span style={{ width: 8, height: 8, background: 'rgba(34,197,94,0.5)', borderRadius: 2, display: 'inline-block', verticalAlign: 'middle' }} />
              <span className="text-crypto-dim">volume</span>
            </span>
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span ref={badgeRef} className="text-[11px] font-mono tabular-nums" />
          <span className="text-[11px] text-crypto-dim">{symbol}</span>
          <div className="flex items-center gap-1 ml-1">
            <button
              onClick={handleFit}
              className="px-2 py-0.5 rounded text-[10px] border transition-colors"
              style={{ background: tc.btn.bg, borderColor: tc.btn.border, color: tc.btn.text }}
              title="Fit all data"
            >
              Fit
            </button>
            <button
              onClick={handleLiveToggle}
              className="px-2 py-0.5 rounded text-[10px] border transition-colors"
              style={{
                background:  isLive ? 'rgba(34,197,94,0.12)' : tc.btn.bg,
                borderColor: isLive ? 'rgba(34,197,94,0.4)'  : tc.btn.border,
                color:       isLive ? '#22c55e'               : tc.btn.text,
              }}
            >
              {isLive ? '● Live' : '○ Paused'}
            </button>
          </div>
        </div>
      </div>

      {/* ── Chart container ── */}
      <div className="flex-1 relative" style={{ minHeight: 320 }}>
        <div ref={containerRef} style={{ width: '100%', height: '100%' }} />

        {/* TradingView-style legend — fixed top-left, fades in on crosshair */}
        <div
          ref={legendRef}
          style={{
            position:      'absolute',
            top:           6,
            left:          6,
            display:       'flex',
            alignItems:    'center',
            flexWrap:      'wrap',
            gap:           '2px 0',
            padding:       '5px 9px',
            borderRadius:  5,
            border:        `1px solid ${tc.tooltip.border}`,
            pointerEvents: 'none',
            zIndex:        10,
            opacity:       0,
            transition:    'opacity 0.1s',
            fontFamily:    TV_FONT,
            boxShadow:     '0 2px 8px rgba(0,0,0,0.25)',
            backdropFilter: 'blur(4px)',
          }}
        />
      </div>
    </div>
  )
})

PriceChart.displayName = 'PriceChart'
export default PriceChart
