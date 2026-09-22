import type { KpiCardData } from "@/lib/types"
import type { ChartDataPoint, TimeSeriesPoint } from "@/lib/types/market-data"
import type { RecentActivity } from "@/lib/types/portfolio"

export const MOCK_KPI_CARDS: readonly KpiCardData[] = [
  {
    title: "Portfolio Value",
    value: "$284,520.75",
    change: 12.5,
    changeLabel: "vs last month",
    icon: "DollarSign",
    trend: [180, 195, 210, 205, 220, 240, 235, 255, 265, 270, 280, 284],
  },
  {
    title: "Daily P&L",
    value: "+$3,241.50",
    change: 2.8,
    changeLabel: "vs yesterday",
    icon: "TrendingUp",
    trend: [100, 98, 105, 103, 110, 108, 115, 112, 120, 118, 125, 128],
  },
  {
    title: "Win Rate",
    value: "68.4%",
    change: -1.2,
    changeLabel: "vs last week",
    icon: "Target",
    trend: [65, 68, 70, 67, 72, 69, 71, 68, 66, 69, 70, 68],
  },
  {
    title: "Active Positions",
    value: "12",
    change: 3,
    changeLabel: "new this week",
    icon: "BarChart3",
    trend: [8, 9, 10, 9, 11, 10, 12, 11, 13, 12, 11, 12],
  },
]

export const MOCK_OVERVIEW_CHART: readonly ChartDataPoint[] = [
  { date: "Jan", portfolio: 200000, benchmark: 200000 },
  { date: "Feb", portfolio: 212000, benchmark: 206000 },
  { date: "Mar", portfolio: 208000, benchmark: 210000 },
  { date: "Apr", portfolio: 225000, benchmark: 215000 },
  { date: "May", portfolio: 235000, benchmark: 222000 },
  { date: "Jun", portfolio: 242000, benchmark: 228000 },
  { date: "Jul", portfolio: 238000, benchmark: 232000 },
  { date: "Aug", portfolio: 255000, benchmark: 240000 },
  { date: "Sep", portfolio: 262000, benchmark: 245000 },
  { date: "Oct", portfolio: 270000, benchmark: 252000 },
  { date: "Nov", portfolio: 278000, benchmark: 258000 },
  { date: "Dec", portfolio: 284520, benchmark: 264000 },
]

export const MOCK_REVENUE_CHART: readonly TimeSeriesPoint[] = [
  { date: "Mon", value: 4200 },
  { date: "Tue", value: 3800 },
  { date: "Wed", value: 5100 },
  { date: "Thu", value: 4600 },
  { date: "Fri", value: 6200 },
  { date: "Sat", value: 3100 },
  { date: "Sun", value: 2800 },
]

export const MOCK_RECENT_ACTIVITY: readonly RecentActivity[] = [
  {
    id: "1",
    type: "buy",
    symbol: "NVDA",
    description: "Bought 15 shares of NVIDIA",
    amount: -12450.0,
    timestamp: "2026-02-19T14:30:00Z",
  },
  {
    id: "2",
    type: "sell",
    symbol: "META",
    description: "Sold 20 shares of Meta Platforms",
    amount: 11240.0,
    timestamp: "2026-02-19T11:15:00Z",
  },
  {
    id: "3",
    type: "dividend",
    symbol: "AAPL",
    description: "Dividend payment from Apple Inc.",
    amount: 96.0,
    timestamp: "2026-02-18T09:00:00Z",
  },
  {
    id: "4",
    type: "buy",
    symbol: "GOOGL",
    description: "Bought 8 shares of Alphabet",
    amount: -14320.0,
    timestamp: "2026-02-17T15:45:00Z",
  },
  {
    id: "5",
    type: "deposit",
    symbol: "",
    description: "Cash deposit",
    amount: 10000.0,
    timestamp: "2026-02-16T10:00:00Z",
  },
]
