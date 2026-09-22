import { create } from "zustand"

interface AppState {
  readonly sidebarCollapsed: boolean
  readonly selectedSymbols: readonly string[]
  readonly timeFilter: "today" | "week" | "month" | "year"
  readonly toggleSidebar: () => void
  readonly setSelectedSymbols: (symbols: readonly string[]) => void
  readonly setTimeFilter: (filter: AppState["timeFilter"]) => void
}

export const useAppStore = create<AppState>((set) => ({
  sidebarCollapsed: false,
  selectedSymbols: ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
  timeFilter: "month",
  toggleSidebar: () =>
    set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
  setSelectedSymbols: (symbols) =>
    set({ selectedSymbols: [...symbols] }),
  setTimeFilter: (timeFilter) => set({ timeFilter }),
}))
