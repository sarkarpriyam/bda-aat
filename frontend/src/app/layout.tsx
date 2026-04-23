import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Netflix - Watch TV Shows Online, Watch Movies Online',
  description: 'Stream movies and TV shows with real-time analytics tracking',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}