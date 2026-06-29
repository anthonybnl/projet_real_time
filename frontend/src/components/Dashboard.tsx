'use client'
import { useReducer, useCallback, useRef, useEffect } from 'react'
import gsap from 'gsap'

import TopBar from './TopBar'
import StatCard from './StatCard'
import PriceChart, { type PriceChartHandle } from './PriceChart'
import VolumeChart, { type VolumeChartHandle } from './VolumeChart'
import GlobalChart, { type GlobalChartHandle } from './GlobalChart'
import AlertsList from './AlertsList'
import TradesTable from './TradesTable'
import MarketOverview from './MarketOverview'
import { useWebSocket } from '@/hooks/useWebSocket'
import { SYMBOLS } from '@/types'
import type { Trade, AlertData, WindowStats, SnapshotData, AnalyticsData, WSMessage } from '@/types'

const API_BASE =
  typeof window !== 'undefined'
    ? (process.env.NEXT_PUBLIC_API_BASE ?? `http://${window.location.hostname}:8000`)
    : 'http://localhost:8000'

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
interface State {
  symbol: string
  connected: boolean
  stats60: Partial<WindowStats>
  stats300: Partial<WindowStats>
  trades: Trade[]
  alerts: AlertData[]
  priceChangePct: number
}

type Action =
  | { type: 'CONNECTED'; value: boolean }
  | { type: 'SET_SYMBOL'; symbol: string }
  | { type: 'TRADE'; trade: Trade }
  | { type: 'ALERT'; alert: AlertData }
  | { type: 'SNAPSHOT'; data: SnapshotData }
  | { type: 'ANALYTICS'; analytics: AnalyticsData }
  | { type: 'LOAD_STATS'; stats60: WindowStats; stats300: WindowStats }
  | { type: 'LOAD_TRADES'; trades: Trade[] }
  | { type: 'LOAD_ALERTS'; alerts: AlertData[] }

const MAX_TRADES = 15
const MAX_ALERTS = 10

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'CONNECTED':
      return { ...state, connected: action.value }

    case 'SET_SYMBOL':
      return { ...state, symbol: action.symbol, trades: [], stats60: {}, stats300: {}, priceChangePct: 0 }

    case 'TRADE': {
      if (action.trade.symbol !== state.symbol) return state
      return {
        ...state,
        trades: [action.trade, ...state.trades].slice(0, MAX_TRADES),
      }
    }

    case 'ALERT':
      return { ...state, alerts: [action.alert, ...state.alerts].slice(0, MAX_ALERTS) }

    case 'SNAPSHOT': {
      const symData = action.data[state.symbol]
      if (!symData) return state
      return {
        ...state,
        stats60: symData['60'] ?? state.stats60,
        stats300: symData['300'] ?? state.stats300,
      }
    }

    case 'ANALYTICS': {
      // Fired every ~1 second from mock loop and MongoDB change stream
      const a = action.analytics
      if (a.symbol !== state.symbol) return state
      const newPrice = a.window_1sec.avg_price
      const prevPrice = state.stats60.last_price
      const pct = prevPrice && newPrice ? ((newPrice - prevPrice) / prevPrice) * 100 : state.priceChangePct
      return {
        ...state,
        priceChangePct: pct,
        // Refresh trades list from the 1-second snapshot so the table always stays current
        trades: a.recent_trades?.length ? a.recent_trades : state.trades,
        // window_1sec → stats60 (the "live" 1-second bucket)
        stats60: {
          ...state.stats60,
          last_price: newPrice ?? state.stats60.last_price,
          avg_price:  newPrice ?? state.stats60.avg_price,
          // trades_per_second from the 1-sec window is not a count; keep existing
        },
        // window_5min → stats300 (the 5-minute aggregation)
        stats300: {
          ...state.stats300,
          avg_price:    a.window_5min.avg_price    ?? state.stats300.avg_price,
          vwap:         a.window_5min.avg_price    ?? state.stats300.vwap,
          total_volume: a.window_5min.volume       ?? state.stats300.total_volume,
          count:        a.window_5min.trades_count ?? state.stats300.count,
        },
      }
    }

    case 'LOAD_STATS':
      return { ...state, stats60: action.stats60, stats300: action.stats300 }

    case 'LOAD_TRADES':
      return { ...state, trades: action.trades }

    case 'LOAD_ALERTS':
      return { ...state, alerts: action.alerts }

    default:
      return state
  }
}

const initialState: State = {
  symbol: 'BTC-USD',
  connected: false,
  stats60: {},
  stats300: {},
  trades: [],
  alerts: [],
  priceChangePct: 0,
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
export default function Dashboard() {
  const [state, dispatch] = useReducer(reducer, initialState)
  const priceChartRef  = useRef<PriceChartHandle>(null)
  const volChartRef    = useRef<VolumeChartHandle>(null)
  const globalChartRef = useRef<GlobalChartHandle>(null)
  const symbolRef = useRef(state.symbol)
  const contentRef = useRef<HTMLDivElement>(null)

  useEffect(() => { symbolRef.current = state.symbol }, [state.symbol])

  // WebSocket message handler — uses refs to avoid stale closures
  const handleMessage = useCallback((msg: WSMessage) => {
    switch (msg.type) {
      case 'trade': {
        const trade = msg.data as Trade
        dispatch({ type: 'TRADE', trade })
        if (trade.symbol === symbolRef.current) {
          volChartRef.current?.push(trade.volume, trade.timestamp, trade.side)
          priceChartRef.current?.pushTrade(trade.volume, trade.side ?? 'unknown')
        }
        break
      }
      case 'alert':
        dispatch({ type: 'ALERT', alert: msg.data as AlertData })
        break
      case 'snapshot':
        dispatch({ type: 'SNAPSHOT', data: msg.data as SnapshotData })
        break
      case 'analytics': {
        // 1-second MongoDB update (production mode)
        const analytics = msg.data as AnalyticsData
        dispatch({ type: 'ANALYTICS', analytics })
        if (analytics.symbol === symbolRef.current) {
          const price      = analytics.window_1sec.avg_price
          const sessionAvg = analytics.global?.avg_price_since_start
          if (price) priceChartRef.current?.push(price, sessionAvg)
          if (analytics.global) globalChartRef.current?.push(analytics.global)
        }
        break
      }
    }
  }, [])

  const handleStatus = useCallback((connected: boolean) => {
    dispatch({ type: 'CONNECTED', value: connected })
  }, [])

  useWebSocket(handleMessage, handleStatus)

  // Fetch initial REST data on mount
  useEffect(() => {
    fetchSymbolData(state.symbol)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function fetchSymbolData(symbol: string) {
    try {
      const [s60, s300, tradesRes, alertsRes] = await Promise.all([
        fetch(`${API_BASE}/api/stats?symbol=${symbol}&window=60`).then((r) => r.json()),
        fetch(`${API_BASE}/api/stats?symbol=${symbol}&window=300`).then((r) => r.json()),
        fetch(`${API_BASE}/api/trades?symbol=${symbol}&limit=15`).then((r) => r.json()),
        fetch(`${API_BASE}/api/alerts?limit=10`).then((r) => r.json()),
      ])
      dispatch({ type: 'LOAD_STATS', stats60: s60, stats300: s300 })
      dispatch({ type: 'LOAD_TRADES', trades: tradesRes.trades ?? [] })
      dispatch({ type: 'LOAD_ALERTS', alerts: alertsRes.alerts ?? [] })
    } catch { /* backend not yet ready */ }
  }

  // Symbol tab switch with GSAP fade
  const handleSymbolChange = useCallback(async (symbol: string) => {
    if (symbol === symbolRef.current) return

    await gsap.to(contentRef.current, { opacity: 0.3, duration: 0.15 }).then()

    dispatch({ type: 'SET_SYMBOL', symbol })
    priceChartRef.current?.reset()
    volChartRef.current?.reset()
    globalChartRef.current?.reset()
    await fetchSymbolData(symbol)

    gsap.to(contentRef.current, { opacity: 1, duration: 0.25 })
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Derived values for stat cards
  const exchange = SYMBOLS.find((s) => s.key === state.symbol)?.exchange ?? ''
  const lastPrice = state.stats60.last_price
  const priceUp = state.priceChangePct > 0 ? true : state.priceChangePct < 0 ? false : null
  const volPct5m = state.stats300.total_volume

  return (
    <div className="min-h-screen bg-crypto-bg flex flex-col">
      <TopBar
        activeSymbol={state.symbol}
        connected={state.connected}
        exchange={exchange}
        onSymbolChange={handleSymbolChange}
      />

      <div ref={contentRef} className="flex-1 p-4 flex flex-col gap-3">
        {/* Stat cards */}
        <div className="grid grid-cols-4 gap-3">
          <StatCard
            label="Last price"
            value={lastPrice}
            format={(n) => '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            sub={lastPrice ? `${state.priceChangePct >= 0 ? '+' : ''}${state.priceChangePct.toFixed(3)}% last trade` : undefined}
            subUp={priceUp}
            icon={<span className="text-crypto-accent">₿</span>}
          />
          <StatCard
            label="Avg price (1 sec)"
            value={state.stats60.avg_price}
            format={(n) => '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            sub="1-second rolling avg"
            subUp={null}
            icon={<span className="text-crypto-accent">~</span>}
          />
          <StatCard
            label="Volume (5 min)"
            value={volPct5m}
            format={(n) => n.toFixed(4)}
            sub={state.symbol.includes('BTC') ? 'BTC traded' : 'ETH traded'}
            subUp={null}
            icon={<span className="text-crypto-accent">↑</span>}
          />
          <StatCard
            label="Trades (5 min)"
            value={state.stats300.count}
            format={(n) => Math.round(n).toString()}
            sub={state.stats300.count ? `~${((state.stats300.count ?? 0) / 300).toFixed(1)}/sec` : undefined}
            subUp={null}
            icon={<span className="text-crypto-accent">#</span>}
          />
        </div>

        {/* Charts */}
        <div className="grid gap-3" style={{ gridTemplateColumns: '3fr 1fr', minHeight: 420 }}>
          <PriceChart ref={priceChartRef} symbol={state.symbol} />
          <div className="flex flex-col gap-3">
            <VolumeChart ref={volChartRef} symbol={state.symbol} />
            <GlobalChart ref={globalChartRef} />
          </div>
        </div>

        {/* Bottom row */}
        <div className="grid grid-cols-3 gap-3">
          <AlertsList alerts={state.alerts} />
          <TradesTable trades={state.trades} />
          <MarketOverview
            stats60={state.stats60}
            stats300={state.stats300}
            trades={state.trades}
          />
        </div>
      </div>
    </div>
  )
}
