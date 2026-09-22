import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fetchMarketOverview,
  fetchSectorPerformance,
  fetchTopGainers,
  fetchTopLosers,
  fetchHistoricalData,
  fetchTechnicalIndicators,
} from "@/lib/api/market-data";

export function useMarketOverview() {
  return useQuery({
    queryKey: ["market-overview"],
    queryFn: fetchMarketOverview,
    refetchInterval: 30_000,
  });
}

export function useSectorPerformance() {
  return useQuery({
    queryKey: ["sector-performance"],
    queryFn: fetchSectorPerformance,
    refetchInterval: 60_000,
  });
}

export function useTopGainers() {
  return useQuery({
    queryKey: ["top-gainers"],
    queryFn: fetchTopGainers,
    refetchInterval: 30_000,
  });
}

export function useTopLosers() {
  return useQuery({
    queryKey: ["top-losers"],
    queryFn: fetchTopLosers,
    refetchInterval: 30_000,
  });
}

export function useHistoricalData(symbol: string, days: number = 90) {
  const [now] = useState(() => Date.now());
  const end = new Date(now).toISOString().slice(0, 10);
  const start = new Date(now - days * 86_400_000)
    .toISOString()
    .slice(0, 10);
  return useQuery({
    queryKey: ["historical-data", symbol, days],
    queryFn: () => fetchHistoricalData(symbol, start, end, "1d"),
    refetchInterval: 300_000,
    enabled: !!symbol,
  });
}

export function useTechnicalIndicators(symbol: string) {
  return useQuery({
    queryKey: ["technical-indicators", symbol],
    queryFn: () => fetchTechnicalIndicators(symbol),
    refetchInterval: 60_000,
    enabled: !!symbol,
  });
}
