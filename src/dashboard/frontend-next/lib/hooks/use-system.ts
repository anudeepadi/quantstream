import { useQuery } from "@tanstack/react-query"
import { fetchSystemHealth } from "@/lib/api/system"

export function useSystemHealth() {
  return useQuery({
    queryKey: ["system-health"],
    queryFn: fetchSystemHealth,
    refetchInterval: 10_000,
  })
}
