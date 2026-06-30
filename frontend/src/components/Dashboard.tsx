'use client'
import { useReducer, useCallback, useRef, useEffect } from 'react'

import TopBar from './TopBar'
import StatCard from './StatCard'
import PriceChart, { type PriceChartHandle } from './PriceChart'
import VolumeChart, { type VolumeChartHandle } from './VolumeChart'
import GlobalChart, { type GlobalChartHandle } from './GlobalChart'
import AlertsList from './AlertsList'
import TradesTable from './TradesTable'
import MarketOverview from './MarketOverview'
import { useWebSocket } from '@/hooks/useWebSocket'
import { LIVE_SYMBOL, LIVE_EXCHANGE } from '@/types'
import type { Trade, AlertData, AnalyticsData, WSMessage } from '@/types'

const API_BASE =
  typeof window !== 'undefined'
    ? (process.env.NEXT_PUBLIC_API_BASE ?? `http://${window.location.hostname}:8000`)
    : 'http://localhost:8000'

const MAX_TRADES = 15

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
interface State {
  connected: boolean
  analytics: AnalyticsData | null
  trades: Trade[]
  priceChangePct: number
}

type Action =
  | { type: 'CONNECTED'; value: boolean }
  | { type: 'ANALYTICS'; analytics: AnalyticsData }
  | { type: 'LOAD_TRADES'; trades: Trade[] }

const initialState: State = {
  connected: false,
  analytics: null,
  trades: [],
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
      // On les met les plus recents en tete pour la table.
      const incoming = [...a.recent_trades].reverse()
      const trades = [...incoming, ...state.trades].slice(0, MAX_TRADES)

      return { ...state, analytics: a, trades, priceChangePct: pct }
    }

    case 'LOAD_TRADES':
      return { ...state, trades: action.trades }

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

  // WebSocket message handler — uses refs to avoid stale closures
  const handleMessage = useCallback((msg: WSMessage) => {
    if (msg.type !== 'analytics') return
    const a = msg.data
    dispatch({ type: 'ANALYTICS', analytics: a })

    const price  = a.window_1sec.avg_price
    const ma1h   = a.window_1hour.avg_price
    const volume = a.window_1sec.volume
    if (price != null) priceChartRef.current?.push(price, ma1h, volume)
  }, [])

  const handleStatus = useCallback((connected: boolean) => {
    dispatch({ type: 'CONNECTED', value: connected })
  }, [])

  useWebSocket(handleMessage, handleStatus)

  // Seed la table de trades au montage (l'analytics WS prendra le relais).
  useEffect(() => {
    fetch(`${API_BASE}/api/trades?limit=${MAX_TRADES}`)
      .then((r) => r.json())
      .then((res) => dispatch({ type: 'LOAD_TRADES', trades: res.trades ?? [] }))
      .catch(() => { /* backend pas encore pret */ })
  }, [])

  // Valeurs derivees pour les stat cards
  const a = state.analytics
  const lastPrice = a?.window_1sec.avg_price ?? undefined
  const priceUp = state.priceChangePct > 0 ? true : state.priceChangePct < 0 ? false : null
  const noAlerts: AlertData[] = []

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
          <AlertsList alerts={noAlerts} />
          <TradesTable trades={state.trades} />
          <MarketOverview stats60={{}} stats300={{}} trades={state.trades} />
        </div>
      </div>
    </div>
  )
}
