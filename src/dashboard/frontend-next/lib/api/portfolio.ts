import { apiGet, isMockMode } from "./client"
import type { PortfolioPosition, PortfolioSummary, AllocationBreakdown, PerformanceMetrics } from "@/lib/types/portfolio"
import { MOCK_POSITIONS, MOCK_PORTFOLIO_SUMMARY, MOCK_ALLOCATION, MOCK_PERFORMANCE } from "@/lib/mock-data/portfolio"
import type { RecentActivity } from "@/lib/types/portfolio"
import { MOCK_RECENT_ACTIVITY } from "@/lib/mock-data/dashboard"

export async function fetchPortfolioSummary(): Promise<PortfolioSummary> {
  if (isMockMode()) return MOCK_PORTFOLIO_SUMMARY
  return apiGet<PortfolioSummary>("/api/v1/portfolio/summary")
}

export async function fetchPositions(): Promise<readonly PortfolioPosition[]> {
  if (isMockMode()) return MOCK_POSITIONS
  return apiGet<PortfolioPosition[]>("/api/v1/portfolio/positions")
}

export async function fetchAllocation(): Promise<readonly AllocationBreakdown[]> {
  if (isMockMode()) return MOCK_ALLOCATION
  return apiGet<AllocationBreakdown[]>("/api/v1/portfolio/allocation")
}

export async function fetchPerformance(): Promise<readonly PerformanceMetrics[]> {
  if (isMockMode()) return MOCK_PERFORMANCE
  return apiGet<PerformanceMetrics[]>("/api/v1/portfolio/performance")
}

export async function fetchRecentActivity(): Promise<readonly RecentActivity[]> {
  if (isMockMode()) return MOCK_RECENT_ACTIVITY
  return apiGet<RecentActivity[]>("/api/v1/portfolio/activity")
}
