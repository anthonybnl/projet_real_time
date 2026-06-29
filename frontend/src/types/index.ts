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

export type WSMessage =
  | { type: 'trade'; data: Trade }
  | { type: 'alert'; data: AlertData }
  | { type: 'snapshot'; data: SnapshotData }
  | { type: 'analytics'; data: unknown }

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
