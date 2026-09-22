import { useQuery } from "@tanstack/react-query"
import { fetchPortfolioSummary, fetchPositions, fetchAllocation, fetchPerformance, fetchRecentActivity } from "@/lib/api/portfolio"

export function usePortfolioSummary() {
  return useQuery({
    queryKey: ["portfolio-summary"],
    queryFn: fetchPortfolioSummary,
    refetchInterval: 30_000,
  })
}

export function usePositions() {
  return useQuery({
    queryKey: ["positions"],
    queryFn: fetchPositions,
    refetchInterval: 30_000,
  })
}

export function useAllocation() {
  return useQuery({
    queryKey: ["allocation"],
    queryFn: fetchAllocation,
    refetchInterval: 60_000,
  })
}

export function usePerformance() {
  return useQuery({
    queryKey: ["performance"],
    queryFn: fetchPerformance,
    refetchInterval: 60_000,
  })
}

export function useRecentActivity() {
  return useQuery({
    queryKey: ["recent-activity"],
    queryFn: fetchRecentActivity,
    refetchInterval: 15_000,
  })
}
