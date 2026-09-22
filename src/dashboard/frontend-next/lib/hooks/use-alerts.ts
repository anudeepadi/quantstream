import { useQuery } from "@tanstack/react-query"
import { fetchAlerts, fetchAlertRules, fetchAlertStatistics } from "@/lib/api/alerts"

export function useAlerts() {
  return useQuery({
    queryKey: ["alerts"],
    queryFn: fetchAlerts,
    refetchInterval: 15_000,
  })
}

export function useAlertRules() {
  return useQuery({
    queryKey: ["alert-rules"],
    queryFn: fetchAlertRules,
  })
}

export function useAlertStatistics() {
  return useQuery({
    queryKey: ["alert-statistics"],
    queryFn: fetchAlertStatistics,
    refetchInterval: 30_000,
  })
}
