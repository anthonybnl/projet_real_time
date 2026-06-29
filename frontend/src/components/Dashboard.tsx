'use client'
import { useReducer, useCallback, useRef, useEffect } from 'react'

import TopBar from './TopBar'
import StatCard from './StatCard'
import PriceChart, { type PriceChartHandle } from './PriceChart'
import VolumeChart, { type VolumeChartHandle } from './VolumeChart'
import GlobalChart, { type GlobalChartHandle } from './GlobalChart'
import AnomaliesList from './AnomaliesList'
import TradesTable from './TradesTable'
import MarketOverview from './MarketOverview'
import { useWebSocket } from '@/hooks/useWebSocket'

import { LIVE_SYMBOL, LIVE_EXCHANGE } from '@/types'
import type { Trade, AnalyticsData, WSMessage,  AnomalyData, WindowStats, SnapshotData } from '@/types'


const API_BASE =
  typeof window !== 'undefined'
    ? (process.env.NEXT_PUBLIC_API_BASE ?? `http://${window.location.hostname}:8000`)
    : 'http://localhost:8000'

const MAX_TRADES = 15
const MAX_ANOMALIES = 10
const HISTORY_SECONDS = 60  // fenetre glissante pour high/low/trades client-side (1 min)

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
interface MarketDerived {
  high?: number
  low?: number
  trades1m?: number
}

interface State {
  connected: boolean
  analytics: AnalyticsData | null
  trades: Trade[]
  anomalies: AnomalyData[]
  priceChangePct: number
  market: MarketDerived
}

type Action =
  | { type: 'CONNECTED'; value: boolean }
  | { type: 'ANALYTICS'; analytics: AnalyticsData; market: MarketDerived }
  | { type: 'LOAD_TRADES'; trades: Trade[] }
  | { type: 'ANOMALY'; anomaly: AnomalyData }
  | { type: 'LOAD_ANOMALIES'; anomalies: AnomalyData[] }

const initialState: State = {
  connected: false,
  analytics: null,
  trades: [],
  anomalies: [],
  priceChangePct: 0,
  market: {},
}

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'CONNECTED':
      return { ...state, connected: action.value }

    case 'ANALYTICS': {
      const a = action.analytics
      const prevPrice = state.analytics?.window_1sec.avg_price
      const newPrice = a.window_1sec.avg_price
      const pct =
        prevPrice && newPrice ? ((newPrice - prevPrice) / prevPrice) * 100 : state.priceChangePct

      // recent_trades : ordre chronologique (ancien -> recent), sans doublon.
      // On met les plus recents en tete pour la table.
      const incoming = [...a.recent_trades].reverse()
      const trades = [...incoming, ...state.trades].slice(0, MAX_TRADES)

      return { ...state, analytics: a, trades, priceChangePct: pct, market: action.market }
    }

    case 'LOAD_TRADES':
      return { ...state, trades: action.trades }

    case 'ANOMALY': {
      // if (action.anomaly.symbol !== state.symbol) return state
      return { ...state, anomalies: [action.anomaly, ...state.anomalies].slice(0, MAX_ANOMALIES) }
    }

    case 'LOAD_ANOMALIES':
      return { ...state, anomalies: action.anomalies }

    default:
      return state
  }
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
export default function Dashboard() {
  const [state, dispatch] = useReducer(reducer, initialState)
  const priceChartRef  = useRef<PriceChartHandle>(null)
  const volChartRef    = useRef<VolumeChartHandle>(null)
  const globalChartRef = useRef<GlobalChartHandle>(null)
  // Historique glissant 1 min (prix 1s + nb trades 1s) pour high/low/trades client-side.
  const historyRef = useRef<{ time: number; price: number; trades: number }[]>([])

  const handleMessage = useCallback((msg: WSMessage) => {
    if (msg.type === 'anomaly'){
      const anomaly = msg.data as AnomalyData
      dispatch({ type: 'ANOMALY', anomaly })
      return
    }
    else if (msg.type !== 'analytics') return



    const a = msg.data
    const now = Date.now() / 1000

    // --- Courbe de prix : prix 1s + MMA 1h + volume 1s ---
    const price  = a.window_1sec.avg_price
    const ma1h   = a.window_1hour.avg_price
    const v1s    = a.window_1sec.volume
    if (price != null) priceChartRef.current?.push(price, ma1h, v1s)

    // --- Volume per 10s : split buy/sell approxime depuis recent_trades ---
    let buyV = 0, sellV = 0
    for (const t of a.recent_trades) {
      if (t.side === 'buy')       buyV  += t.volume
      else if (t.side === 'sell') sellV += t.volume
      else { buyV += t.volume / 2; sellV += t.volume / 2 }
    }
    const sample = buyV + sellV
    if (sample > 0) {
      volChartRef.current?.push(v1s * (buyV / sample),  now, 'buy')
      volChartRef.current?.push(v1s * (sellV / sample), now, 'sell')
    } else if (v1s > 0) {
      volChartRef.current?.push(v1s, now, 'unknown')
    }

    // --- Last hour ---
    globalChartRef.current?.push(a.window_1hour)

    // --- High/Low/Trades sur 1 min, calcules cote front (TODO: backend) ---
    const hist = historyRef.current
    if (price != null) hist.push({ time: now, price, trades: a.window_1sec.trades_count })
    historyRef.current = hist.filter((h) => h.time >= now - HISTORY_SECONDS)
    const prices = historyRef.current.map((h) => h.price)
    const market: MarketDerived = {
      high: prices.length ? Math.max(...prices) : undefined,
      low: prices.length ? Math.min(...prices) : undefined,
      trades1m: historyRef.current.reduce((s, h) => s + h.trades, 0),
    }

    dispatch({ type: 'ANALYTICS', analytics: a, market })
  }, [])

  const handleStatus = useCallback((connected: boolean) => {
    dispatch({ type: 'CONNECTED', value: connected })
  }, [])

  useWebSocket(handleMessage, handleStatus)

  // Seed la table de trades au montage (l'analytics WS prend ensuite le relais).
  useEffect(() => {
    fetch(`${API_BASE}/api/trades?limit=${MAX_TRADES}`)
      .then((r) => r.json())
      .then((res) => dispatch({ type: 'LOAD_TRADES', trades: res.trades ?? [] }))
      .catch(() => { /* backend pas encore pret */ })
    
    fetch(`${API_BASE}/api/anomalies?symbol=${'BTC-USD'}&limit=10`)
      .then((r) => r.json())
      .then((res) => dispatch({ type: 'LOAD_ANOMALIES', anomalies: res.anomalies ?? [] }))
      .catch(() => { /* backend pas encore pret */ })
  }, [])

  // Valeurs derivees
  const a = state.analytics
  const lastPrice = a?.window_1sec.avg_price ?? undefined
  const priceUp = state.priceChangePct > 0 ? true : state.priceChangePct < 0 ? false : null

  // Stats pour MarketOverview (high/low/trades = client-side ; vwap/notional approximes)
  const stats60 = { high: state.market.high, low: state.market.low, count: state.market.trades1m }
  const stats300 = a
    ? {
        vwap: a.window_5min.avg_price ?? undefined,
        total_volume: a.window_5min.volume,
        total_notional: (a.window_5min.avg_price ?? 0) * a.window_5min.volume,
      }
    : {}

  return (
    <div className="min-h-screen bg-crypto-bg flex flex-col">
      <TopBar connected={state.connected} exchange={LIVE_EXCHANGE} />

      <div className="flex-1 p-4 flex flex-col gap-3">
        {/* Stat cards */}
        <div className="grid grid-cols-4 gap-3">
          <StatCard
            label="Last price"
            value={lastPrice}
            format={(n) => '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            sub={lastPrice ? `${state.priceChangePct >= 0 ? '+' : ''}${state.priceChangePct.toFixed(3)}% / sec` : undefined}
            subUp={priceUp}
            icon={<span className="text-crypto-accent">₿</span>}
          />
          <StatCard
            label="Avg price (1 sec)"
            value={a?.window_1sec.avg_price ?? undefined}
            format={(n) => '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            sub="1-second rolling avg"
            subUp={null}
            icon={<span className="text-crypto-accent">~</span>}
          />
          <StatCard
            label="Volume (5 min)"
            value={a?.window_5min.volume ?? undefined}
            format={(n) => n.toFixed(4)}
            sub="BTC traded"
            subUp={null}
            icon={<span className="text-crypto-accent">↑</span>}
          />
          <StatCard
            label="Trades (5 min)"
            value={a?.window_5min.trades_count ?? undefined}
            format={(n) => Math.round(n).toString()}
            sub={a?.window_5min.trades_count ? `~${(a.window_5min.trades_count / 300).toFixed(1)}/sec` : undefined}
            subUp={null}
            icon={<span className="text-crypto-accent">#</span>}
          />
        </div>

        {/* Charts */}
        <div className="grid gap-3" style={{ gridTemplateColumns: '3fr 1fr', minHeight: 420 }}>
          <PriceChart ref={priceChartRef} symbol={LIVE_SYMBOL} />
          <div className="flex flex-col gap-3">
            <VolumeChart ref={volChartRef} symbol={LIVE_SYMBOL} />
            <GlobalChart ref={globalChartRef} />
          </div>
        </div>

        {/* Bottom row */}
        <div className="grid grid-cols-3 gap-3">
          <AnomaliesList anomalies={state.anomalies} />
          <TradesTable trades={state.trades} />
          <MarketOverview stats60={stats60} stats300={stats300} trades={state.trades} />
        </div>
      </div>
    </div>
  )
}
