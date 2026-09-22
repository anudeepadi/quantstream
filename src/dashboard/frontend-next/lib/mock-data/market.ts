import type { MarketOverview, SectorPerformance, TopMover } from "@/lib/types/market-data"

export const MOCK_MARKET_OVERVIEW: readonly MarketOverview[] = [
  { symbol: "AAPL", name: "Apple Inc.", price: 180.50, change: 2.35, change_percent: 1.32, volume: 54200000, market_cap: 2800000000000, sector: "Technology" },
  { symbol: "GOOGL", name: "Alphabet Inc.", price: 2950.0, change: -15.20, change_percent: -0.51, volume: 1200000, market_cap: 1900000000000, sector: "Technology" },
  { symbol: "MSFT", name: "Microsoft Corp.", price: 420.75, change: 5.80, change_percent: 1.40, volume: 22100000, market_cap: 3100000000000, sector: "Technology" },
  { symbol: "AMZN", name: "Amazon.com Inc.", price: 3400.25, change: 45.50, change_percent: 1.36, volume: 3500000, market_cap: 1700000000000, sector: "Consumer Cyclical" },
  { symbol: "TSLA", name: "Tesla Inc.", price: 195.80, change: -8.40, change_percent: -4.11, volume: 89000000, market_cap: 620000000000, sector: "Automotive" },
  { symbol: "NVDA", name: "NVIDIA Corp.", price: 830.0, change: 22.40, change_percent: 2.77, volume: 42000000, market_cap: 2050000000000, sector: "Technology" },
  { symbol: "META", name: "Meta Platforms", price: 562.0, change: 8.15, change_percent: 1.47, volume: 15200000, market_cap: 1430000000000, sector: "Technology" },
  { symbol: "JPM", name: "JPMorgan Chase", price: 198.40, change: 1.20, change_percent: 0.61, volume: 8900000, market_cap: 570000000000, sector: "Financial" },
]

export const MOCK_SECTOR_PERFORMANCE: readonly SectorPerformance[] = [
  { sector: "Technology", change_percent: 2.15, market_cap: 12500000000000, volume: 185000000 },
  { sector: "Healthcare", change_percent: 0.85, market_cap: 7200000000000, volume: 62000000 },
  { sector: "Financial", change_percent: 1.20, market_cap: 8100000000000, volume: 95000000 },
  { sector: "Consumer Cyclical", change_percent: -0.45, market_cap: 5400000000000, volume: 45000000 },
  { sector: "Energy", change_percent: -1.80, market_cap: 4200000000000, volume: 72000000 },
  { sector: "Industrials", change_percent: 0.32, market_cap: 4800000000000, volume: 38000000 },
  { sector: "Real Estate", change_percent: -0.65, market_cap: 1200000000000, volume: 18000000 },
  { sector: "Utilities", change_percent: 0.15, market_cap: 1500000000000, volume: 12000000 },
]

export const MOCK_TOP_GAINERS: readonly TopMover[] = [
  { symbol: "NVDA", name: "NVIDIA Corp.", price: 830.0, change: 22.40, change_percent: 2.77, volume: 42000000 },
  { symbol: "META", name: "Meta Platforms", price: 562.0, change: 8.15, change_percent: 1.47, volume: 15200000 },
  { symbol: "MSFT", name: "Microsoft Corp.", price: 420.75, change: 5.80, change_percent: 1.40, volume: 22100000 },
  { symbol: "AMZN", name: "Amazon.com Inc.", price: 3400.25, change: 45.50, change_percent: 1.36, volume: 3500000 },
  { symbol: "AAPL", name: "Apple Inc.", price: 180.50, change: 2.35, change_percent: 1.32, volume: 54200000 },
]

export const MOCK_TOP_LOSERS: readonly TopMover[] = [
  { symbol: "TSLA", name: "Tesla Inc.", price: 195.80, change: -8.40, change_percent: -4.11, volume: 89000000 },
  { symbol: "BABA", name: "Alibaba Group", price: 88.20, change: -2.60, change_percent: -2.86, volume: 18500000 },
  { symbol: "PYPL", name: "PayPal Holdings", price: 62.40, change: -1.50, change_percent: -2.35, volume: 12000000 },
  { symbol: "DIS", name: "Walt Disney Co.", price: 95.60, change: -1.80, change_percent: -1.85, volume: 9800000 },
  { symbol: "NFLX", name: "Netflix Inc.", price: 485.0, change: -5.20, change_percent: -1.06, volume: 5600000 },
]
