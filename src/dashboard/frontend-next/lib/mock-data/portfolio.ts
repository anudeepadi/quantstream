import type { PortfolioPosition, PortfolioSummary, AllocationBreakdown, PerformanceMetrics } from "@/lib/types/portfolio"

export const MOCK_PORTFOLIO_SUMMARY: PortfolioSummary = {
  total_value: 284520.75,
  total_cost: 200000,
  total_pnl: 84520.75,
  total_pnl_percent: 42.26,
  daily_pnl: 3241.50,
  daily_pnl_percent: 1.15,
  cash: 25000,
  buying_power: 50000,
}

export const MOCK_POSITIONS: readonly PortfolioPosition[] = [
  { symbol: "AAPL", name: "Apple Inc.", quantity: 100, avg_cost: 150.0, current_price: 180.50, market_value: 18050, unrealized_pnl: 3050, unrealized_pnl_percent: 20.33, weight: 6.35, sector: "Technology" },
  { symbol: "GOOGL", name: "Alphabet Inc.", quantity: 50, avg_cost: 2800.0, current_price: 2950.0, market_value: 147500, unrealized_pnl: 7500, unrealized_pnl_percent: 5.36, weight: 51.84, sector: "Technology" },
  { symbol: "MSFT", name: "Microsoft Corp.", quantity: 75, avg_cost: 380.0, current_price: 420.75, market_value: 31556.25, unrealized_pnl: 3056.25, unrealized_pnl_percent: 10.72, weight: 11.09, sector: "Technology" },
  { symbol: "AMZN", name: "Amazon.com Inc.", quantity: 30, avg_cost: 3200.0, current_price: 3400.25, market_value: 102007.5, unrealized_pnl: 6007.5, unrealized_pnl_percent: 6.26, weight: 35.86, sector: "Consumer Cyclical" },
  { symbol: "TSLA", name: "Tesla Inc.", quantity: 25, avg_cost: 220.0, current_price: 195.80, market_value: 4895, unrealized_pnl: -605, unrealized_pnl_percent: -11.0, weight: 1.72, sector: "Automotive" },
  { symbol: "NVDA", name: "NVIDIA Corp.", quantity: 40, avg_cost: 650.0, current_price: 830.0, market_value: 33200, unrealized_pnl: 7200, unrealized_pnl_percent: 27.69, weight: 11.67, sector: "Technology" },
  { symbol: "JPM", name: "JPMorgan Chase", quantity: 60, avg_cost: 175.0, current_price: 198.40, market_value: 11904, unrealized_pnl: 1404, unrealized_pnl_percent: 13.37, weight: 4.18, sector: "Financial" },
  { symbol: "META", name: "Meta Platforms", quantity: 20, avg_cost: 480.0, current_price: 562.0, market_value: 11240, unrealized_pnl: 1640, unrealized_pnl_percent: 17.08, weight: 3.95, sector: "Technology" },
]

export const MOCK_ALLOCATION: readonly AllocationBreakdown[] = [
  { category: "Technology", value: 241546.25, percentage: 62.8, color: "var(--chart-1)" },
  { category: "Consumer Cyclical", value: 102007.5, percentage: 26.5, color: "var(--chart-2)" },
  { category: "Financial", value: 11904, percentage: 3.1, color: "var(--chart-3)" },
  { category: "Automotive", value: 4895, percentage: 1.3, color: "var(--chart-4)" },
  { category: "Cash", value: 25000, percentage: 6.5, color: "var(--chart-5)" },
]

export const MOCK_PERFORMANCE: readonly PerformanceMetrics[] = [
  { period: "1D", return_percent: 1.15, benchmark_return_percent: 0.82, alpha: 0.33, sharpe_ratio: 1.82, max_drawdown: -0.45, volatility: 12.5 },
  { period: "1W", return_percent: 3.24, benchmark_return_percent: 2.10, alpha: 1.14, sharpe_ratio: 1.75, max_drawdown: -1.20, volatility: 14.2 },
  { period: "1M", return_percent: 8.65, benchmark_return_percent: 5.40, alpha: 3.25, sharpe_ratio: 1.68, max_drawdown: -3.80, volatility: 16.8 },
  { period: "3M", return_percent: 15.20, benchmark_return_percent: 10.50, alpha: 4.70, sharpe_ratio: 1.55, max_drawdown: -6.20, volatility: 18.5 },
  { period: "1Y", return_percent: 42.26, benchmark_return_percent: 28.00, alpha: 14.26, sharpe_ratio: 1.42, max_drawdown: -12.50, volatility: 22.4 },
]
