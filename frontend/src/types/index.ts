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

export interface WindowStats {
  symbol: string
  window: number
  count: number
  last_price?: number
  vwap?: number
  avg_price?: number
  high?: number
  low?: number
  total_volume?: number
  total_notional?: number
}

export type SnapshotData = Record<string, Record<string, WindowStats>>

export interface AnalyticsData {
  timestamp: string
  symbol: string
  window_1sec: {
    avg_price?: number
    trades_per_second?: number
  }
  window_5min: {
    avg_price?: number
    volume?: number
    trades_count?: number
  }
  global?: {
    avg_price_since_start?: number
    total_trades?: number
    uptime_seconds?: number
  }
  recent_trades?: Trade[]
  window_60s_extra?: {
    high?: number
    low?: number
    count?: number
    total_notional?: number
  }
}

export type WSMessage =
  | { type: 'trade'; data: Trade }
  | { type: 'alert'; data: AlertData }
  | { type: 'snapshot'; data: SnapshotData }
  | { type: 'analytics'; data: AnalyticsData }

export interface SymbolConfig {
  label: string
  key: string
  exchange: string
}

export const SYMBOLS: SymbolConfig[] = [
  { label: 'BTC-USD', key: 'BTC-USD', exchange: 'Coinbase' },
  { label: 'BTC/USDT', key: 'BTCUSDT', exchange: 'Binance' },
  { label: 'ETH/USDT', key: 'ETHUSDT', exchange: 'Binance' },
]
