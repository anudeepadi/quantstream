export interface MarketDataPoint {
  readonly symbol: string;
  readonly timestamp: string;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly volume: number;
  readonly vwap: number;
}

export interface TechnicalIndicator {
  readonly name: string;
  readonly value: number;
  readonly signal: "buy" | "sell" | "neutral";
}

export interface MarketOverview {
  readonly symbol: string;
  readonly name: string;
  readonly price: number;
  readonly change: number;
  readonly change_percent: number;
  readonly volume: number;
  readonly market_cap: number;
  readonly sector: string;
}

export interface SectorPerformance {
  readonly sector: string;
  readonly change_percent: number;
  readonly market_cap: number;
  readonly volume: number;
}

export interface TopMover {
  readonly symbol: string;
  readonly name: string;
  readonly price: number;
  readonly change: number;
  readonly change_percent: number;
  readonly volume: number;
}

export interface TimeSeriesPoint {
  readonly date: string;
  readonly value: number;
}

export interface ChartDataPoint {
  readonly date: string;
  readonly [key: string]: string | number;
}

export interface HistoricalDataPoint {
  readonly date: string;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly volume: number;
}

export interface MarketDataUpdate {
  readonly symbol: string;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly volume: number;
  readonly change: number;
  readonly change_percent: number;
  readonly last_update: string;
}
