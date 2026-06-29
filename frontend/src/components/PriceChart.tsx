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
const PRICE_ANIM_MS = 450 // smooth transition duration for live price
const TV_FONT = "'Trebuchet MS', Roboto, Ubuntu, sans-serif"

interface Entry {
  price:  number
  ma:     number | null  // moyenne mobile 1h
  volume: number
  time:   number         // Unix seconds
}

type ChartMode = 'line' | 'candles'

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
  // Appele 1x/seconde : prix 1s, moyenne mobile 1h (ligne secondaire), volume 1s.
  push(price: number, ma: number | null, volume: number): void
  reset(): void
}

function easeOutCubic(t: number) {
  return 1 - Math.pow(1 - t, 3)
}

function fmt$(n: number) {
  return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

const PriceChart = forwardRef<PriceChartHandle, { symbol: string }>(({ symbol }, ref) => {
  const { theme }    = useTheme()
  const themeRef     = useRef(theme)
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef     = useRef<IChartApi | null>(null)
  const liveRef      = useRef<ISeriesApi<'Area'> | null>(null)
  const candleRef    = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const sessionRef   = useRef<ISeriesApi<'Line'> | null>(null)
  const volRef       = useRef<ISeriesApi<'Histogram'> | null>(null)
  const chartModeRef = useRef<ChartMode>('line')
  const bufferRef    = useRef<Entry[]>([])
  const lineColorRef = useRef('#22c55e')
  const isLiveRef    = useRef(true)
  const displayPriceRef = useRef<number | null>(null)
  const animRef = useRef<{ from: number; to: number; start: number } | null>(null)
  const rafRef = useRef<number | null>(null)
  const legendRef = useRef<HTMLDivElement>(null)
  const badgeRef     = useRef<HTMLSpanElement>(null)
  const [isLive, setIsLive] = useState(true)
  const [chartMode, setChartMode] = useState<ChartMode>('line')

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

    // ── Area: live price ────────────────────────────────────────────────────
    const liveSeries = chart.addAreaSeries({
      lineColor:   '#22c55e',
      topColor:    'rgba(34,197,94,0.18)',
      bottomColor: 'rgba(34,197,94,0)',
      lineWidth:   2,
      crosshairMarkerRadius:          5,
      crosshairMarkerBorderColor:     '#080d1a',
      crosshairMarkerBackgroundColor: '#22c55e',
      crosshairMarkerBorderWidth:     2,
      priceLineVisible:  true,
      priceLineStyle:    LineStyle.Dashed,
      priceLineColor:    'rgba(107,122,153,0.45)',
      lastValueVisible:  true,
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    })

    // ── Candlesticks (hidden until toggled) ─────────────────────────────────
    const candleSeries = chart.addCandlestickSeries({
      upColor:        '#22c55e',
      downColor:      '#ef4444',
      borderVisible:  false,
      wickUpColor:    '#22c55e',
      wickDownColor:  '#ef4444',
      visible:        false,
      priceFormat:    { type: 'price', precision: 2, minMove: 0.01 },
    })

    // ── Line: session avg ───────────────────────────────────────────────────
    const sessionSeries = chart.addLineSeries({
      color:     '#8b5cf6',
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      crosshairMarkerVisible:          true,
      crosshairMarkerRadius:           3,
      crosshairMarkerBackgroundColor:  '#8b5cf6',
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
    liveRef.current    = liveSeries
    candleRef.current  = candleSeries
    sessionRef.current = sessionSeries
    volRef.current     = volSeries

    const entryClose = (e: Entry) => e.close

    // ── TradingView-style fixed legend (top-left) ───────────────────────────
    chart.subscribeCrosshairMove((param) => {
      const legend = legendRef.current
      if (!legend) return
      if (!param.time || !param.point || param.point.x < 0 || param.point.y < 0) {
        legend.style.opacity = '0'
        return
      }

      const mode = chartModeRef.current
      const lp = mode === 'line'
        ? param.seriesData.get(liveSeries) as { value: number } | undefined
        : undefined
      const cp = mode === 'candles'
        ? param.seriesData.get(candleSeries) as { open: number; high: number; low: number; close: number } | undefined
        : undefined
      const sp = mode === 'line'
        ? param.seriesData.get(sessionSeries) as { value: number } | undefined
        : undefined
      const vp = param.seriesData.get(volSeries) as { value: number } | undefined

      const refPrice = lp?.value ?? cp?.close
      if (refPrice == null) { legend.style.opacity = '0'; return }

      const prices = bufferRef.current.map((e) => e.price)
      const first  = prices[0] ?? lp.value
      const delta  = first > 0 ? ((lp.value - first) / first) * 100 : 0
      const sign   = delta >= 0 ? '+' : ''
      const lc     = refPrice >= (cp?.open ?? refPrice) ? '#22c55e' : '#ef4444'
      const curTc  = TC[themeRef.current].tooltip

      const t    = new Date((param.time as number) * 1000)
      const tStr = t.getHours().toString().padStart(2, '0') + ':' +
                   t.getMinutes().toString().padStart(2, '0') + ':' +
                   t.getSeconds().toString().padStart(2, '0')

      const priceBlock = mode === 'candles' && cp
        ? `<span style="color:${curTc.text};font-size:11px;font-weight:600">O ${fmt$(cp.open)} H ${fmt$(cp.high)} L ${fmt$(cp.low)} C ${fmt$(cp.close)}</span>`
        : `<span style="color:${curTc.text};font-size:11px;font-weight:600">${fmt$(refPrice)}</span>`

      legend.style.background = curTc.bg
      legend.style.borderColor = curTc.border
      legend.innerHTML = `
        <span style="color:${curTc.dim};font-size:10px;margin-right:8px">${tStr}</span>
        <span style="display:inline-flex;align-items:center;gap:4px;margin-right:10px">
          <span style="width:9px;height:2px;background:${lc};display:inline-block;border-radius:1px"></span>
          ${priceBlock}
          <span style="color:${delta >= 0 ? '#22c55e' : '#ef4444'};font-size:10px">${sign}${delta.toFixed(3)}%</span>
        </span>
        ${sp ? `
        <span style="display:inline-flex;align-items:center;gap:4px;margin-right:10px">
          <span style="width:9px;height:0;border-top:1.5px dashed #8b5cf6;display:inline-block"></span>
          <span style="color:#a78bfa;font-size:11px">${fmt$(sp.value)}</span>
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
      if (rafRef.current !== null) cancelAnimationFrame(rafRef.current)
      chart.remove()
      chartRef.current = liveRef.current = candleRef.current = sessionRef.current = volRef.current = null
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
    const applyLineColor = (price: number) => {
      const live = liveRef.current
      if (!live) return
      const prices = bufferRef.current.map((e) => e.close)
      const first  = prices[0] ?? price
      const isUp   = price >= first
      const lc     = isUp ? '#22c55e' : '#ef4444'
      lineColorRef.current = lc
      live.applyOptions({
        lineColor:   lc,
        topColor:    isUp ? 'rgba(34,197,94,0.18)'  : 'rgba(239,68,68,0.18)',
        bottomColor: isUp ? 'rgba(34,197,94,0)'     : 'rgba(239,68,68,0)',
        crosshairMarkerBackgroundColor: lc,
      })
    }

    const candlePoint = (e: Entry) => ({
      time: e.time as Time,
      open: e.open,
      high: e.high,
      low: e.low,
      close: e.close,
    })

    const updateBadge = (price: number) => {
      const prices = bufferRef.current.map((e) => e.close)
      const first  = prices[0] ?? price
      if (badgeRef.current && prices.length > 1) {
        const delta = first > 0 ? ((price - first) / first) * 100 : 0
        const sign  = delta >= 0 ? '+' : ''
        badgeRef.current.textContent = `${sign}${delta.toFixed(3)}%`
        badgeRef.current.style.color = price >= first ? '#22c55e' : '#ef4444'
      }
    }

    const syncViewport = () => {
      const chart = chartRef.current
      if (!chart || !isLiveRef.current) return
      // Suit le bord droit (temps reel). Independant du nb de points du buffer :
      // la serie du graphe grossit alors que le buffer est plafonne, donc un
      // range base sur buf.length finirait par pointer sur de vieux indices.
      chart.timeScale().scrollToRealTime()
    }

    const paintLivePoint = (price: number) => {
      const live  = liveRef.current
      const chart = chartRef.current
      const buf   = bufferRef.current
      if (!live || !chart || !buf.length) return

      const last = buf[buf.length - 1]
      last.close = price
      last.high = Math.max(last.high, price)
      last.low = Math.min(last.low, price)
      displayPriceRef.current = price
      applyLineColor(price)
      live.update({ time: last.time as Time, value: price })

      updateBadge(price)
      syncViewport()
    }

    const stopAnimation = () => {
      if (rafRef.current !== null) {
        cancelAnimationFrame(rafRef.current)
        rafRef.current = null
      }
      animRef.current = null
    }

    const animateToPrice = (target: number) => {
      const from = displayPriceRef.current ?? target
      if (Math.abs(from - target) < 0.005) {
        stopAnimation()
        paintLivePoint(target)
        return
      }

      animRef.current = { from, to: target, start: performance.now() }

      const tick = (now: number) => {
        const anim = animRef.current
        if (!anim) return
        const t = Math.min(1, (now - anim.start) / PRICE_ANIM_MS)
        const price = anim.from + (anim.to - anim.from) * easeOutCubic(t)
        paintLivePoint(price)

        if (t < 1) {
          rafRef.current = requestAnimationFrame(tick)
        } else {
          paintLivePoint(anim.to)
          stopAnimation()
        }
      }

      if (rafRef.current !== null) cancelAnimationFrame(rafRef.current)
      rafRef.current = requestAnimationFrame(tick)
    }

    const upsertBuffer = (price: number, ma: number | null, volume: number) => {
      const nowSec = Math.floor(Date.now() / 1000)
      const buf    = bufferRef.current
      const last   = buf[buf.length - 1]
      const isNewSecond = !last || last.time !== nowSec

      if (last && last.time === nowSec) {
        last.ma = ma ?? last.ma
        last.volume = volume
      } else {
        buf.push({ price, ma: ma ?? null, volume, time: nowSec })
        if (buf.length > MAX_POINTS) buf.shift()
      }
      return isNewSecond
    }

    // Maj de la ligne MMA 1h et de la barre de volume du point courant.
    // update() ajoute/modifie un seul point -> pas de redraw complet par seconde.
    const paintMaAndVolume = () => {
      const session = sessionRef.current
      const vol     = volRef.current
      const last    = bufferRef.current[bufferRef.current.length - 1]
      if (!last) return
      if (session && last.ma != null) session.update({ time: last.time as Time, value: last.ma })
      if (vol) vol.update({
        time:  last.time as Time,
        value: last.volume,
        color: last.volume > 0 ? 'rgba(107,122,153,0.45)' : 'rgba(107,122,153,0.15)',
      })
    }

    return {
    push(price: number, ma: number | null, volume: number) {
      upsertBuffer(price, ma, volume)
      paintMaAndVolume()
      // Animation fluide du prix : seul le dernier point est anime via update().
      animateToPrice(price)
    },

    reset() {
      stopAnimation()
      bufferRef.current    = []
      displayPriceRef.current = null
      lineColorRef.current = '#22c55e'
      liveRef.current?.setData([])
      candleRef.current?.setData([])
      sessionRef.current?.setData([])
      volRef.current?.setData([])
      liveRef.current?.applyOptions({
        lineColor: '#22c55e', topColor: 'rgba(34,197,94,0.18)', bottomColor: 'rgba(34,197,94,0)',
      })
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
          <span className="text-xs font-medium text-crypto-text">
            {chartMode === 'line' ? 'Price — rolling 60s' : 'Bougies — 60s'}
          </span>
          <span className="flex items-center gap-3 text-[10px]">
            <span className="flex items-center gap-1.5">
              <span style={{ width: 16, height: 0, borderBottom: '2px solid #22c55e', display: 'inline-block', verticalAlign: 'middle' }} />
              <span className="text-crypto-dim">live</span>
            </span>
            <span className="flex items-center gap-1.5">
              <span style={{ width: 16, height: 0, borderBottom: '2px dashed #8b5cf6', display: 'inline-block', verticalAlign: 'middle' }} />
              <span className="text-crypto-dim">1h MA</span>
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
              onClick={handleModeToggle}
              className="px-2 py-0.5 rounded text-[10px] border transition-colors"
              style={{
                background:  chartMode === 'candles' ? 'rgba(245,158,11,0.12)' : tc.btn.bg,
                borderColor: chartMode === 'candles' ? 'rgba(245,158,11,0.4)' : tc.btn.border,
                color:       chartMode === 'candles' ? '#f59e0b' : tc.btn.text,
              }}
              title={chartMode === 'line' ? 'Passer aux bougies' : 'Passer au graphique en ligne'}
            >
              {chartMode === 'line' ? 'Bougies' : 'Ligne'}
            </button>
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
