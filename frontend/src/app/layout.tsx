import type { Metadata } from 'next'
import './globals.css'
import { ThemeProvider } from '@/contexts/ThemeContext'

export const metadata: Metadata = {
  title: 'CryptoStream — Real-time Market Monitor',
  description: 'Live BTC/ETH market data powered by Binance & Coinbase streams',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="dark" suppressHydrationWarning>
      <body>
        {/* Read saved theme before React hydrates to prevent flash */}
        <script dangerouslySetInnerHTML={{ __html: `(function(){try{var t=localStorage.getItem('crypto-theme');if(t)document.documentElement.setAttribute('data-theme',t)}catch(e){}})()` }} />
        <ThemeProvider>{children}</ThemeProvider>
      </body>
    </html>
  )
}
