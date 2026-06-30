export interface Trade {
  symbol: string
  price: number
  volume: number
  notional: number
  side: 'buy' | 'sell' | 'unknown'
  source: string
  timestamp: number
}

export interface AlertData {
  type: 'large_trade' | 'price_spike' | 'volume_spike'
  symbol: string
  price?: number
  volume?: number
  notional?: number
  prev_price?: number
  variation_pct?: number
  mean_volume?: number
  sigma?: number
  timestamp: number
}

// Stats par fenetre, toujours partielles cote front (certains champs sont
// calcules localement : high/low, ou approximes : vwap/notional).
export interface WindowStats {
  symbol?: string
  window?: number
  count?: number
  last_price?: number
  vwap?: number
  avg_price?: number
  high?: number
  low?: number
  total_volume?: number
  total_notional?: number
}

// Une fenetre d'agregation telle que poussee par le backend (analytics v2).
export interface WindowAgg {
  volume: number
  avg_price: number | null
  trades_count: number
}

export interface AnalyticsData {
  timestamp: string
  window_1sec: WindowAgg
  window_5min: WindowAgg
  window_1hour: WindowAgg
  recent_trades: Trade[]
}

// Le backend ne pousse plus qu'un seul type de message (analytics, ~1/s).
export type WSMessage = { type: 'analytics'; data: AnalyticsData }

// Vue unique BTC : le backend agrege binance + coinbase ensemble.
export const LIVE_SYMBOL = 'BTC'
export const LIVE_EXCHANGE = 'Binance + Coinbase'
