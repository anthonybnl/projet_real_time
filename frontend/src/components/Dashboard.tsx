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

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
interface State {
  connected: boolean
  analytics: AnalyticsData | null
  trades: Trade[]
  anomalies: AnomalyData[]
  priceChangePct: number
}

type Action =
  | { type: 'CONNECTED'; value: boolean }
  | { type: 'ANALYTICS'; analytics: AnalyticsData }
  | { type: 'LOAD_TRADES'; trades: Trade[] }
  | { type: 'ANOMALY'; anomaly: AnomalyData }
  | { type: 'LOAD_ANOMALIES'; anomalies: AnomalyData[] }

const initialState: State = {
  connected: false,
  analytics: null,
  trades: [],
  anomalies: [],
  priceChangePct: 0,
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

      return { ...state, analytics: a, trades, priceChangePct: pct }
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

  const handleMessage = useCallback((msg: WSMessage) => {
    if (msg.type === 'anomaly'){
      const anomaly = msg.data as AnomalyData
      dispatch({ type: 'ANOMALY', anomaly })
      return
    }
    else if (msg.type !== 'analytics') return



    const a = msg.data
    const now = Date.now() / 1000

    // --- Bougies 1s : close=prix 1s, high/low 1s (meches) + MMA 5 min + volume 1s ---
    const price  = a.window_1sec.avg_price
    const high   = a.window_1sec.high ?? price
    const low    = a.window_1sec.low ?? price
    const ma5    = a.window_5min.avg_price
    const v1s    = a.window_1sec.volume
    if (price != null) priceChartRef.current?.push(price, high, low, ma5, v1s)

    // --- Volume 1s : split buy/sell exact depuis l'agregat backend ---
    const buyV  = a.window_1sec.vol_buy
    const sellV = a.window_1sec.vol_sell
    if (buyV > 0 || sellV > 0) {
      if (buyV > 0)  volChartRef.current?.push(buyV,  now, 'buy')
      if (sellV > 0) volChartRef.current?.push(sellV, now, 'sell')
    } else if (v1s > 0) {
      volChartRef.current?.push(v1s, now, 'unknown')
    }

    // --- Last hour ---
    globalChartRef.current?.push(a.window_1hour)

    dispatch({ type: 'ANALYTICS', analytics: a })
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

  // Stats pour MarketOverview, toutes issues de l'agregat backend 5 min
  // (high/low + split buy/sell exact ; notional approxime via vwap * volume).
  const stats60 = a
    ? {
        high: a.window_5min.high ?? undefined,
        low: a.window_5min.low ?? undefined,
        count: a.window_5min.trades_count,
      }
    : {}
  const stats300 = a
    ? {
        vwap: a.window_5min.avg_price ?? undefined,
        total_volume: a.window_5min.volume,
        total_notional: (a.window_5min.avg_price ?? 0) * a.window_5min.volume,
        vol_buy: a.window_5min.vol_buy,
        vol_sell: a.window_5min.vol_sell,
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
          <MarketOverview stats60={stats60} stats300={stats300} />
        </div>
      </div>
    </div>
  )
}
