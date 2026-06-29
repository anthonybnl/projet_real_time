'use client'
import { useEffect, useRef, useCallback } from 'react'
import type { WSMessage } from '@/types'

const WS_URL =
  typeof window !== 'undefined'
    ? (process.env.NEXT_PUBLIC_WS_URL ?? `ws://${window.location.hostname}:8000/ws/stream`)
    : 'ws://localhost:8000/ws/stream'

export function useWebSocket(
  onMessage: (msg: WSMessage) => void,
  onStatusChange: (connected: boolean) => void,
) {
  const wsRef = useRef<WebSocket | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout>>()
  const onMessageRef = useRef(onMessage)
  const onStatusRef = useRef(onStatusChange)

  useEffect(() => { onMessageRef.current = onMessage }, [onMessage])
  useEffect(() => { onStatusRef.current = onStatusChange }, [onStatusChange])

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    const ws = new WebSocket(WS_URL)
    wsRef.current = ws

    ws.onopen = () => onStatusRef.current(true)

    ws.onmessage = (e) => {
      try {
        onMessageRef.current(JSON.parse(e.data) as WSMessage)
      } catch { /* ignore malformed frames */ }
    }

    ws.onclose = () => {
      onStatusRef.current(false)
      timerRef.current = setTimeout(connect, 2000)
    }

    ws.onerror = () => ws.close()
  }, [])

  useEffect(() => {
    connect()
    return () => {
      clearTimeout(timerRef.current)
      wsRef.current?.close()
    }
  }, [connect])
}
