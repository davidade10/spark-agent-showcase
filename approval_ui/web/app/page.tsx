"use client";

import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import axios from "axios";
import {
  AlertTriangle, CheckCircle, XCircle, Clock, TrendingUp,
  RefreshCw, ChevronDown, ChevronUp,
  Activity, BellOff, Trash2, Ban, History, Download,
  Loader2,
} from "lucide-react";

const API = "http://localhost:8000";

// ── Types ─────────────────────────────────────────────────────────────────────

interface LLMCard {
  recommendation: "yes" | "no" | "conditional";
  confidence: number;
  summary: string;
  market_environment: string;
  rationale: string[];
  setup_specific_risks: string[];
  numbers_used: { dte: number; score: number; max_loss: number; net_credit: number };
  conditions_if_conditional: string[];
  red_flags: string[];
  _meta?: { model: string; context_block?: string };
}

interface CandidateJson {
  symbol: string; expiry: string; dte: number;
  underlying_price: number;
  short_put_strike: number; long_put_strike: number;
  short_call_strike: number; long_call_strike: number;
  short_put_delta: number; short_call_delta: number;
  net_credit: number; max_loss: number; spread_width: number;
  iv_rank?: number;
  strategy?: string;
  qty?: number;
}

interface Candidate {
  id: number; symbol: string; score: number; account_id: string;
  created_at: string; age_minutes: number; is_stale: boolean;
  candidate_json: CandidateJson; llm_card: LLMCard;
  strategy?: string;
}

interface Position {
  id: number; account_id: string; symbol: string; strategy: string;
  expiry: string; dte: number; fill_credit?: number; entry_credit?: number;
  net_credit?: number | null;
  spread_width?: number | null;
  unrealized_pnl: number; profit_pct: number | null;
  net_delta: number; max_risk: number; opened_at: string;
  legs: any; legs_json?: any; meta: any; position_key: string;
  long_put_strike?: number | null; short_put_strike?: number | null;
  short_call_strike?: number | null; long_call_strike?: number | null;
  qty?: number;
  mark?: number | null;
  status?: string;
}

interface ExitSignal {
  id: number; symbol: string; expiry: string; dte: number;
  reason: string; credit_received: number; debit_to_close: number;
  pnl_dollars: number; pnl_pct: number; status: string; age_minutes: number;
}

interface Account {
  account_id: string; open_positions: number;
  total_credit: number | null; total_margin: number | null; total_pnl: number | null;
  nav: number | null; buying_power: number | null;
  type?: string;
  daily_pnl?: number | null;
}

interface Event {
  symbol: string; event_type: string; event_ts: string; days_away: number;
}

interface TokenData {
  valid?: boolean;
  days_remaining?: number | null;
}

interface Health {
  status: string;
  checks: {
    database?: string;
    circuit_breaker: { state: string; failures: number; attempts: number };
    data_freshness: { last_snapshot_minutes_ago: number | null; is_stale: boolean };
    token?: TokenData | string | null;
    reconciler_log?: string | null;
  };
}

interface RefreshResult {
  ok: boolean;
  message: string;
  feed_status?: string;
  reason?: string;
  symbols_failed?: string[];
}

/** GET /shadow — blocked_reason from API JSONB */
interface BlockedReason {
  rule?: string;
  detail?: string;
  actual?: number | null;
  threshold?: number | null;
  operator?: string | null;
}

interface GateKillBin {
  rule: string;
  label: string;
  count: number;
}

interface ShadowBlocked {
  id: number;
  symbol: string;
  score: number;
  net_credit?: string | number | null;
  expiry?: string | null;
  blocked_reason?: BlockedReason | Record<string, unknown> | null;
  created_at: string;
  snapshot_id?: number | null;
  strategy?: string | null;
  gate_rule_label?: string | null;
  long_put_strike?: number | null;
  short_put_strike?: number | null;
  short_call_strike?: number | null;
  long_call_strike?: number | null;
  qty?: number | null;
}

interface ShadowResponse {
  blocked: ShadowBlocked[];
  count: number;
  hours: number;
  cutoff: string;
  gate_kill_distribution?: GateKillBin[];
}

/** GET /pipeline-stats — funnel for shadow window */
interface PipelineStats {
  scanned: number;
  passed_gates: number;
  blocked: number;
  llm_evaluated: number;
  circuit_broken: number;
  approved: number;
  rejected: number;
  expired: number;
  awaiting_operator_decision: number;
  hours: number;
  cutoff: string;
}

/** GET /history */
interface HistoryRow {
  id: number;
  symbol: string;
  score: number;
  account_id?: string | null;
  created_at: string;
  net_credit?: unknown;
  expiry?: string | null;
  decision: string;
  decided_at: string;
  reason?: string | null;
  pnl?: number | null;
  exit_reason?: string | null;
  closed_at?: string | null;
  gate_diagnostics?: Record<string, unknown> | null;
  llm_recommendation?: string | null;
  llm_confidence?: number | null;
  llm_reasoning?: string | null;
  llm_model?: string | null;
  llm_latency?: number | null;
  strategy?: string | null;
  long_put_strike?: number | null;
  short_put_strike?: number | null;
  short_call_strike?: number | null;
  long_call_strike?: number | null;
  qty?: number | null;
}

interface HistoryResponse {
  history: HistoryRow[];
  count: number;
  days: number;
}

// ── Freshness helpers ─────────────────────────────────────────────────────────

type FreshnessLevel = "live" | "delayed" | "stale" | "unknown";

function getFreshnessLevel(minutesAgo: number | null): FreshnessLevel {
  if (minutesAgo === null) return "unknown";
  if (minutesAgo < 2) return "live";
  if (minutesAgo < 15) return "delayed";
  return "stale";
}

function formatAge(minutesAgo: number | null): string {
  if (minutesAgo === null) return "unknown";
  if (minutesAgo < 1) return "<1m ago";
  return `${minutesAgo}m ago`;
}

function formatDataAge(seconds: number | null): string {
  if (seconds === null) return "--";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
}

// "2026-03-20" → "Mar 20"
function formatExpiry(isoDate: string): string {
  const parts = isoDate.split("-");
  if (parts.length < 3) return isoDate;
  const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const month = months[parseInt(parts[1], 10) - 1] ?? parts[1];
  return `${month} ${parseInt(parts[2], 10)}`;
}

// ── Condor leg / breakeven helpers ───────────────────────────────────────────

function parseLegsRaw(val: unknown): unknown {
  if (val == null) return null;
  if (typeof val === "string") {
    try { return JSON.parse(val); } catch { return null; }
  }
  return val;
}

/** Per-leg prices: prefer GET /positions `legs` (positions.legs JSONB); else condor-shaped `legs_json` only. */
function getCondorLegPrices(legs: unknown, legsJson: unknown): { lp: number | null; sp: number | null; sc: number | null; lc: number | null } {
  const out = { lp: null as number | null, sp: null as number | null, sc: null as number | null, lc: null as number | null };

  // Primary: strategy_engine + reconciler — { long_put, short_put, short_call, long_call } with avg_price / market_value
  let legsObj = parseLegsRaw(legs) as Record<string, { avg_price?: number; market_value?: number }> | null;
  const legsEmpty =
    legsObj == null ||
    (typeof legsObj === "object" &&
      !Array.isArray(legsObj) &&
      Object.keys(legsObj as object).length === 0);
  // Fallback: object-shaped legs_json only (not the non-condor array form).
  if (legsEmpty) {
    const lj = parseLegsRaw(legsJson) as Record<string, unknown> | null;
    if (lj && typeof lj === "object" && !Array.isArray(lj) && "long_put" in lj && "short_put" in lj) {
      legsObj = lj as Record<string, { avg_price?: number; market_value?: number }>;
    }
  }
  if (legsObj && typeof legsObj === "object" && !Array.isArray(legsObj)) {
    const price = (l: { avg_price?: number; market_value?: number } | undefined) => {
      if (!l) return null;
      const v = l.avg_price ?? l.market_value;
      return typeof v === "number" && !Number.isNaN(v) ? v : null;
    };
    out.lp = price(legsObj.long_put as { avg_price?: number; market_value?: number } | undefined);
    out.sp = price(legsObj.short_put as { avg_price?: number; market_value?: number } | undefined);
    out.sc = price(legsObj.short_call as { avg_price?: number; market_value?: number } | undefined);
    out.lc = price(legsObj.long_call as { avg_price?: number; market_value?: number } | undefined);
    if (out.lp != null || out.sp != null || out.sc != null || out.lc != null) return out;
  }

  // legs_json: array of { option_type, side, strike } — no per-leg price; use null
  const arr = parseLegsRaw(legsJson);
  if (Array.isArray(arr) && arr.length > 0) {
    // legs_json has no price; we'd need option_quotes. Leave as — for now.
    return out;
  }
  return out;
}

function safeNumericCredit(credit: unknown): number | null {
  if (credit == null) return null;
  if (typeof credit === "number" && !Number.isNaN(credit)) return credit;
  if (typeof credit === "string") {
    const n = parseFloat(credit.replace(/[$,]/g, ""));
    return !Number.isNaN(n) ? n : null;
  }
  return null;
}

function computeBreakevens(
  shortPutStrike: number | null | undefined,
  shortCallStrike: number | null | undefined,
  credit: number | null
): { put: number | null; call: number | null } {
  const c = safeNumericCredit(credit);
  if (c == null) return { put: null, call: null };
  const sp = typeof shortPutStrike === "number" && !Number.isNaN(shortPutStrike) ? shortPutStrike : null;
  const sc = typeof shortCallStrike === "number" && !Number.isNaN(shortCallStrike) ? shortCallStrike : null;
  return {
    put:  sp != null ? sp - c : null,
    call: sc != null ? sc + c : null,
  };
}

/** Iron condor max loss dollars: (spread_width − net_credit) × contracts × 100 */
function computeIronCondorMaxRiskUsd(p: Position): number | null {
  if ((p.strategy ?? "").toUpperCase() !== "IRON_CONDOR") return null;
  const sw = p.spread_width;
  const cr = p.fill_credit ?? p.entry_credit ?? p.net_credit;
  const q = p.qty;
  if (sw == null || cr == null || q == null) return null;
  const swN = Number(sw);
  const crN = Number(cr);
  const qN = Number(q);
  if (!Number.isFinite(swN) || !Number.isFinite(crN) || !Number.isFinite(qN) || qN <= 0) return null;
  const per = swN - crN;
  if (!Number.isFinite(per)) return null;
  const usd = per * qN * 100;
  return Math.round(usd * 100) / 100;
}

/** Position-level |Δ| exposure tone (PDF bands, applied to stored net_delta magnitude). */
function directionalDeltaTone(absDelta: number): string {
  if (!Number.isFinite(absDelta)) return "text-secondary";
  if (absDelta < 0.05) return "text-success";
  if (absDelta < 0.15) return "text-warning";
  return "text-danger";
}

function ivrIronCondorColor(ivr: number): string {
  if (!Number.isFinite(ivr)) return "text-zinc-400";
  if (ivr > 40) return "text-emerald-400";
  if (ivr >= 25) return "text-amber-400";
  return "text-orange-400";
}

/** 2–3 lines / soft cap for LLM summary above the fold */
function previewLlmSummary(text: string | undefined, maxLines = 3, softCap = 280): string {
  if (!text?.trim()) return "";
  const trimmed = text.trim();
  const lines = trimmed.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  if (lines.length >= 2) {
    let joined = lines.slice(0, maxLines).join("\n");
    if (joined.length > softCap) joined = joined.slice(0, softCap).trimEnd() + "…";
    return joined;
  }
  if (trimmed.length <= softCap) return trimmed;
  const slice = trimmed.slice(0, softCap);
  const sp = slice.lastIndexOf(" ");
  return (sp > 48 ? slice.slice(0, sp) : slice) + "…";
}

/** Strike display for condor cards (API fields). Whole dollars: $220; otherwise minimal decimals e.g. $222.5. Never NaN. */
function formatCondorLegStrike(val: unknown): string {
  if (val == null || val === "") return "—";
  const n =
    typeof val === "number" && Number.isFinite(val)
      ? val
      : parseFloat(String(val).trim().replace(/[$,]/g, ""));
  if (!Number.isFinite(n)) return "—";
  const rounded = Math.round(n);
  if (Math.abs(n - rounded) < 1e-6) return `$${rounded}`;
  const t = parseFloat(n.toFixed(4));
  return `$${String(t)}`;
}

/** Matches backend /shadow gate-kill bucket sentinel + display map */
const LEGACY_UNKNOWN_RULE = "__UNKNOWN_LEGACY__";

const GATE_RULE_LABELS_MAP: Record<string, string> = {
  [LEGACY_UNKNOWN_RULE]: "Unknown / Legacy",
  max_open_condors: "Max Open Positions",
  daily_loss_kill: "Daily Loss Kill Switch",
  net_credit: "Net Credit Minimum",
  short_delta: "Short Delta Limit",
  fomc_proximity: "FOMC Proximity",
  earnings_proximity: "Earnings Overlap",
  position_risk: "Position Risk vs NAV",
  correlated_risk: "Correlated Risk",
  underlying_volume: "Underlying Volume (ADV)",
  open_interest: "Open Interest / Liquidity",
  iv_rank: "IV Rank",
};

function blockedRuleRawKey(br: unknown): string {
  if (!br || typeof br !== "object") return LEGACY_UNKNOWN_RULE;
  const r = (br as BlockedReason).rule;
  if (r != null && String(r).trim()) return String(r).trim();
  return LEGACY_UNKNOWN_RULE;
}

function gateRuleHumanLabel(rawKey: string): string {
  if (!rawKey || rawKey === LEGACY_UNKNOWN_RULE) return "Unknown / Legacy";
  const k = rawKey.trim();
  if (k in GATE_RULE_LABELS_MAP) return GATE_RULE_LABELS_MAP[k];
  const norm = k.toLowerCase().replace(/ /g, "_").replace(/-/g, "_");
  if (norm in GATE_RULE_LABELS_MAP) return GATE_RULE_LABELS_MAP[norm];
  return norm.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
}

function isIronCondorStrategy(strategy: string | null | undefined): boolean {
  const s = (strategy ?? "").toUpperCase().replace(/-/g, "_");
  return s === "IRON_CONDOR";
}

function formatContractsSuffix(qty: unknown): string {
  if (qty == null || qty === "") return "—";
  const n = typeof qty === "number" ? qty : parseFloat(String(qty).replace(/,/g, ""));
  if (!Number.isFinite(n) || n <= 0) return "—";
  const i = Math.round(n);
  if (Math.abs(n - i) < 1e-6) return `${i}x`;
  return "—";
}

function IronCondorSpreadSummary({
  strategy,
  long_put_strike,
  short_put_strike,
  short_call_strike,
  long_call_strike,
  expiry,
  qty,
  className,
}: {
  strategy?: string | null;
  long_put_strike?: unknown;
  short_put_strike?: unknown;
  short_call_strike?: unknown;
  long_call_strike?: unknown;
  expiry?: string | null;
  qty?: unknown;
  className?: string;
}) {
  if (!isIronCondorStrategy(strategy)) return null;
  const exp = expiry ? formatExpiry(String(expiry)) : "—";
  const q = formatContractsSuffix(qty);
  const hasQty = q !== "—";
  return (
    <span className={`spread-condor-summary font-numeric ${className ?? ""}`.trim()}>
      <span className="condor-lab">LP</span>
      <span className="condor-str">{formatCondorLegStrike(long_put_strike)}</span>
      <span className="condor-sep" aria-hidden>|</span>
      <span className="condor-lab">SP</span>
      <span className="condor-str">{formatCondorLegStrike(short_put_strike)}</span>
      <span className="condor-sep" aria-hidden>/</span>
      <span className="condor-lab">SC</span>
      <span className="condor-str">{formatCondorLegStrike(short_call_strike)}</span>
      <span className="condor-sep" aria-hidden>|</span>
      <span className="condor-lab">LC</span>
      <span className="condor-str">{formatCondorLegStrike(long_call_strike)}</span>
      <span className="condor-tail font-numeric">
        <span className="condor-sep" aria-hidden>·</span>
        <span className="condor-meta">{exp}</span>
        {hasQty && (
          <>
            <span className="condor-sep" aria-hidden>·</span>
            <span className="condor-meta">{q}</span>
          </>
        )}
      </span>
    </span>
  );
}

function formatAccountDisplay(accountId: string | null | undefined): string {
  if (accountId == null || accountId === "") return "—";
  if (accountId === "primary") return "primary (legacy · pre–account scoping)";
  return accountId;
}

function gateDetail(br: unknown): string {
  if (!br || typeof br !== "object") return "";
  const d = (br as BlockedReason).detail;
  return d != null ? String(d) : "";
}

function gateBlockedMetrics(br: unknown): { actual: number | null; threshold: number | null; operator: string | null } {
  if (!br || typeof br !== "object") return { actual: null, threshold: null, operator: null };
  const o = br as BlockedReason;
  const num = (v: unknown): number | null => {
    if (v == null) return null;
    if (typeof v === "number" && !Number.isNaN(v)) return v;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };
  return {
    actual:    num(o.actual),
    threshold: num(o.threshold),
    operator:  o.operator != null && String(o.operator).trim() ? String(o.operator) : null,
  };
}

function formatGateMetric(rule: string, which: "actual" | "threshold", v: number | null): string {
  if (v == null || Number.isNaN(v)) return "—";
  if (rule === "net_credit") return `$${v.toFixed(2)}`;
  if (rule === "short_delta") return v.toFixed(3);
  if (rule === "position_risk" || rule === "correlated_risk") return `${(v * 100).toFixed(2)}%`;
  if (rule === "underlying_volume") return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (rule === "max_open_condors" || rule === "open_interest") return `${Math.round(v)}`;
  return String(v);
}

function clientGateKillDistribution(blocked: ShadowBlocked[]): GateKillBin[] {
  const counts = new Map<string, number>();
  for (const r of blocked) {
    const key = blockedRuleRawKey(r.blocked_reason);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([rule, count]) => ({
      rule,
      label: gateRuleHumanLabel(rule),
      count,
    }));
}

function mergeGateKillDist(server: GateKillBin[] | undefined, blocked: ShadowBlocked[]): GateKillBin[] {
  if (server && server.length > 0) return server;
  return clientGateKillDistribution(blocked);
}

type HistoryDecisionBucket = "approved" | "rejected" | "other";

function historyDecisionBucket(decision: string | null | undefined): HistoryDecisionBucket {
  const s = (decision ?? "").toLowerCase();
  if (s.includes("approv")) return "approved";
  if (s.includes("reject")) return "rejected";
  return "other";
}

function formatIsoShort(iso: string | undefined | null): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso.slice(0, 16);
    return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  } catch {
    return String(iso).slice(0, 16);
  }
}

function downloadCsv(filename: string, columns: { key: string; header: string }[], rows: Record<string, unknown>[]) {
  const esc = (v: unknown) => {
    const s = v == null ? "" : String(v);
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const header = columns.map(c => c.header).join(",");
  const body = rows.map(r => columns.map(c => esc(r[c.key])).join(",")).join("\n");
  const blob = new Blob([`${header}\n${body}`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// Extract first sentence — skips decimal points (e.g. "1.06"), capped at maxLen.
// Uses a character-walk instead of lookbehind regex for Safari/WebKit compatibility.
function firstSentence(text: string | undefined, maxLen = 140): string {
  if (!text) return "";
  for (let i = 1; i < Math.min(text.length, maxLen); i++) {
    if (text[i] === "." && (i + 1 >= text.length || text[i + 1] === " ") && !/\d/.test(text[i - 1])) {
      return text.slice(0, i + 1);
    }
  }
  if (text.length <= maxLen) return text;
  const lastSpace = text.slice(0, maxLen).lastIndexOf(" ");
  return (lastSpace > 0 ? text.slice(0, lastSpace) : text.slice(0, maxLen)) + "…";
}

// ── Position status chip (Correction 4) ──────────────────────────────────────

type StatusChip = "TARGET" | "DANGER" | "WATCH" | "HOLD";

function getChip(
  mark: number | null | undefined,
  fillCredit: number | null | undefined,
  dte: number | null,
): StatusChip {
  if (mark != null && fillCredit != null) {
    if (mark <= fillCredit * 0.5) return "TARGET";
    if (mark >= fillCredit * 2.0) return "DANGER";
  }
  if (dte != null && dte < 14) return "WATCH";
  return "HOLD";
}

const chipStyle: Record<StatusChip, string> = {
  TARGET: "text-success bg-positive/10",
  DANGER: "text-danger  bg-negative/10",
  WATCH:  "text-warning bg-warning/10",
  HOLD:   "text-tertiary bg-white/5",
};

// Strategies that carry premium credit (used to gate totalCredit computation)
const PREMIUM_STRATEGIES = new Set(["IRON_CONDOR", "SHORT_OPTION", "VERTICAL_SPREAD", "STRANGLE", "STRADDLE"]);

function getPositionStatus(p: Position): "on_track" | "watch" | "at_risk" | "neutral" {
  const credit = p.fill_credit ?? p.entry_credit;
  const mark   = p.mark;
  const dte    = typeof p.dte === "number" && !Number.isNaN(p.dte) ? p.dte : null;
  if (mark != null && credit != null) {
    if (mark >= credit * 2.0) return "at_risk";
    if (mark <= credit * 0.5) return "on_track";
  }
  if (dte != null && dte < 14) return "watch";
  return "neutral";
}

// ── Candidate helpers ─────────────────────────────────────────────────────────

const recLabel    = (rec: string) => rec.toUpperCase();
const recTextColor = (rec: string) => rec === "yes" ? "text-emerald-400" : rec === "no" ? "text-red-400" : "text-amber-400";
const recBorderBg  = (rec: string) => rec === "yes"
  ? "border-emerald-500/30 bg-emerald-500/5"
  : rec === "no"
    ? "border-red-500/30 bg-red-500/5"
    : "border-amber-500/30 bg-amber-500/5";
const recBadgeBg   = (rec: string) => rec === "yes"
  ? "bg-emerald-400 text-black"
  : rec === "no"
    ? "bg-red-500 text-white"
    : "bg-amber-400 text-black";

const scoreColor    = (s: number) => s >= 70 ? "text-emerald-400" : s >= 50 ? "text-amber-400" : "text-red-400";
const scoreBarColor = (s: number) => s >= 70 ? "bg-emerald-400" : s >= 50 ? "bg-amber-400" : "bg-red-400";
const scoreDotBg    = (s: number) => s >= 70 ? "bg-emerald-400" : s >= 50 ? "bg-amber-400" : "bg-red-400";
const confPct  = (c: number) => `${Math.round((c ?? 0) * 100)}%`;

function pop(putDelta: number, callDelta: number): string {
  const p = Math.max(0, Math.min(100, (1 - Math.abs(putDelta) - Math.abs(callDelta)) * 100));
  return `${p.toFixed(0)}%`;
}
function rr(credit: number, maxLoss: number): string {
  if (!maxLoss || maxLoss === 0) return "—";
  return `1:${(maxLoss / credit).toFixed(2)}`;
}

// ── Correction 1: Operator Bar ───────────────────────────────────────────────

function OperatorBar({ dataAgeSeconds, systemLevel, openCount, pendingCount, alertCount, reconciled, polling, refreshRefreshing, refreshResult, onRefresh }: {
  dataAgeSeconds: number | null;
  systemLevel: FreshnessLevel;
  openCount: number;
  pendingCount: number;
  alertCount: number;
  reconciled: string | null;
  polling: boolean;
  refreshRefreshing?: boolean;
  refreshResult?: RefreshResult | null;
  onRefresh: () => void;
}) {
  const dotColor: Record<FreshnessLevel, string> = {
    live:    "text-positive",
    delayed: "text-warning",
    stale:   "text-danger",
    unknown: "text-tertiary",
  };
  const systemLabel: Record<FreshnessLevel, string> = {
    live:    "LIVE",
    delayed: "DELAYED",
    stale:   "STALE",
    unknown: "—",
  };

  return (
    <div className="h-10 flex items-center justify-between px-6 bg-card border-b border-subtle font-mono text-xs">
      {/* Left — brand */}
      <div className="flex items-center gap-2.5">
        <Activity size={13} className="text-positive" />
        <span className="font-black tracking-widest text-primary">SPARK</span>
        <span className="text-tertiary hidden sm:inline">/ Iron Condor Desk</span>
      </div>

      {/* Right — operational fields separated by dividers */}
      <div className="flex items-center divide-x divide-subtle">
        <span className={`flex items-center gap-1.5 pr-4 ${dotColor[systemLevel]}`}>
          <span>●</span>
          System: <span className="font-bold ml-0.5">{systemLabel[systemLevel]}</span>
        </span>
        <span className="text-secondary px-4">
          Data: <span className="text-primary">{formatDataAge(dataAgeSeconds)}</span>
        </span>
        <span className="text-secondary px-4">
          <span className="text-primary font-bold">{openCount}</span> Open
        </span>
        <span className="text-secondary px-4">
          <span className="text-primary font-bold">{pendingCount}</span> Pending
        </span>
        <span className={alertCount > 0 ? "text-warning px-4 font-bold" : "text-tertiary px-4"}>
          {alertCount} Alert{alertCount !== 1 ? "s" : ""}
        </span>
        <span className="text-tertiary px-4">Reconciled: {reconciled ?? "—"}</span>
        {refreshResult && (
          <span className={`px-4 text-xs ${
            refreshResult.feed_status === "fresh" ? "text-positive" :
            refreshResult.feed_status === "market_closed" ? "text-secondary" :
            refreshResult.feed_status === "upstream_error" ? "text-danger" :
            refreshResult.ok ? "text-warning" : "text-danger"
          }`} title={refreshResult.symbols_failed?.length ? refreshResult.symbols_failed.join(", ") : undefined}>
            {refreshResult.message}
            {refreshResult.symbols_failed?.length ? (
              <span className="ml-1 opacity-80 text-[10px]">({refreshResult.symbols_failed.join(", ")})</span>
            ) : null}
          </span>
        )}
        <button
          onClick={onRefresh}
          disabled={refreshRefreshing}
          className="flex items-center gap-1 pl-4 text-secondary hover:text-primary transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <RefreshCw size={10} className={refreshRefreshing ? "animate-spin" : ""} />
          REFRESH
        </button>
      </div>
    </div>
  );
}

// ── NAV Dashboard (Correction 3: 4-row layout) ───────────────────────────────

function AccountCard({ label, accountId, nav, dailyPnl, unrealizedPnl, buyingPower, totalMargin, openPositions, isLive }: {
  label: string;
  accountId?: string;
  nav: number | null;
  dailyPnl?: number | null;
  unrealizedPnl: number | null;
  buyingPower?: number | null;
  totalMargin: number | null;
  openPositions: number;
  isLive?: boolean;
}) {
  const navStr = nav != null
    ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(nav)
    : "—";
  const marginPct = nav != null && nav > 0 && totalMargin != null ? (totalMargin / nav) * 100 : null;

  return (
    <div className={`p-5 card-surface rounded-none border-0 font-mono ${isLive ? "bg-blue-950/10" : ""}`}>
      {/* Row 1: Label + NAV */}
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="text-[10px] text-tertiary uppercase">{label}</div>
          {accountId && (
            <div className={`text-xs mt-0.5 ${isLive ? "text-info" : "text-secondary"}`}>
              {accountId === "PAPER" ? "PAPER" : `···${accountId.slice(-4)}`}
            </div>
          )}
        </div>
        <div className="text-right">
          <div className="text-xl font-black text-primary">{navStr}</div>
          <div className="text-[10px] text-tertiary">NAV</div>
        </div>
      </div>

      {/* Row 2: Daily P/L | Unrealized P/L */}
      <div className="grid grid-cols-2 gap-3 text-xs mb-3">
        <div>
          <div className="text-[10px] text-tertiary mb-0.5">Daily P/L</div>
          <div className={dailyPnl == null ? "text-tertiary" : dailyPnl >= 0 ? "text-success font-bold" : "text-danger font-bold"}>
            {dailyPnl == null ? "—" : `${dailyPnl >= 0 ? "+" : ""}$${dailyPnl.toFixed(2)}`}
          </div>
        </div>
        <div>
          <div className="text-[10px] text-tertiary mb-0.5">Unrealized P/L</div>
          <div className={unrealizedPnl == null ? "text-tertiary" : unrealizedPnl >= 0 ? "text-success font-bold" : "text-danger font-bold"}>
            {unrealizedPnl == null ? "—" : `${unrealizedPnl >= 0 ? "+" : ""}$${unrealizedPnl.toFixed(2)}`}
          </div>
        </div>
      </div>

      {/* Row 3: Buying Power | Margin Used % */}
      <div className="grid grid-cols-2 gap-3 text-xs mb-3">
        <div>
          <div className="text-[10px] text-tertiary mb-0.5">Buying Power</div>
          <div className="text-secondary">
            {buyingPower != null
              ? `$${buyingPower.toLocaleString("en-US", { maximumFractionDigits: 0 })}`
              : "—"}
          </div>
        </div>
        <div>
          <div className="text-[10px] text-tertiary mb-0.5">Margin Used</div>
          {marginPct != null ? (
            <>
              <div className="text-secondary mb-1">{marginPct.toFixed(1)}%</div>
              <div className="h-1 rounded bg-subtle overflow-hidden">
                <div
                  className="h-full rounded bg-accent"
                  style={{ width: `${Math.min(100, marginPct)}%` }}
                />
              </div>
            </>
          ) : (
            <div className="text-tertiary">—</div>
          )}
        </div>
      </div>

      {/* Row 4: Positions | Synced */}
      <div className="grid grid-cols-2 gap-3 text-xs">
        <div className="text-tertiary">Positions: <span className="text-secondary">{openPositions}</span></div>
        <div className="text-tertiary">Synced: --</div>
      </div>
    </div>
  );
}

function NavDashboard({ accounts, liveNav }: { accounts: Account[]; liveNav: number | null }) {
  const combined = accounts.reduce((acc, a) => ({
    open_positions: acc.open_positions + (a.open_positions ?? 0),
    total_margin:   a.total_margin   != null ? acc.total_margin   + a.total_margin   : acc.total_margin,
    total_pnl:      a.total_pnl      != null ? acc.total_pnl      + a.total_pnl      : acc.total_pnl,
    has_margin:     acc.has_margin || a.total_margin != null,
    has_pnl:        acc.has_pnl    || a.total_pnl    != null,
  }), { open_positions: 0, total_margin: 0, total_pnl: 0, has_margin: false, has_pnl: false });

  // COMBINED NAV: use liveNav (broker total) + paper accounts when available,
  // otherwise fall back to summing all account navs from reconciler.
  const paperNav = accounts.filter(a => a.type === "PAPER").reduce((sum, a) => sum + (a.nav ?? 0), 0);
  const combinedNav = liveNav != null
    ? paperNav + liveNav
    : accounts.some(a => a.nav != null) ? accounts.reduce((sum, a) => sum + (a.nav ?? 0), 0) : null;

  return (
    <div className="w-full rounded-xl border border-subtle overflow-hidden mb-6 shadow-lg">
      <div className="flex items-center px-4 py-2 bg-card border-b border-subtle">
        <span className="text-[10px] font-mono font-semibold text-tertiary tracking-widest">ACCOUNT SUMMARY</span>
      </div>
      <div
        className="grid divide-x divide-subtle"
        style={{ gridTemplateColumns: `repeat(${accounts.length + 1}, 1fr)` }}
      >
        {accounts.map((a) => (
          <AccountCard
            key={a.account_id}
            label={a.type || "ACCOUNT"}
            accountId={a.account_id}
            nav={a.nav}
            dailyPnl={a.daily_pnl}
            unrealizedPnl={a.total_pnl}
            buyingPower={a.buying_power}
            totalMargin={a.total_margin}
            openPositions={a.open_positions}
            isLive={a.type === "LIVE"}
          />
        ))}

        {/* Combined — purple accent border */}
        <div className="border-l border-accent/20">
          <AccountCard
            label="COMBINED"
            nav={combinedNav != null && combinedNav > 0 ? combinedNav : null}
            dailyPnl={null}
            unrealizedPnl={combined.has_pnl ? combined.total_pnl : null}
            buyingPower={null}
            totalMargin={combined.has_margin ? combined.total_margin : null}
            openPositions={combined.open_positions}
          />
        </div>
      </div>
    </div>
  );
}

// ── Events Context Panel ──────────────────────────────────────────────────────

function ContextPanel({ symbol }: { symbol: string }) {
  const [events, setEvents] = useState<Event[]>([]);
  useEffect(() => {
    axios.get(`${API}/events/${symbol}`)
      .then(r => setEvents(r.data.events))
      .catch(() => {});
  }, [symbol]);

  if (!events.length) return null;

  return (
    <div className="mx-5 mb-4 px-4 py-3 bg-card rounded-lg border border-subtle">
      <div className="text-[10px] font-mono text-tertiary mb-2 tracking-widest">MARKET CONTEXT</div>
      <div className="flex flex-wrap gap-4">
        {events.map((e, i) => {
          const isEarnings = e.event_type?.toLowerCase().includes("earnings");
          const isFomc     = e.event_type?.toLowerCase().includes("fomc");
          const urgent     = e.days_away <= 7;
          const color      = urgent ? "text-danger" : isEarnings ? "text-warning" : "text-secondary";
          return (
            <span key={i} className={`flex items-center gap-1.5 text-xs font-mono ${color}`}>
              <span>{isEarnings ? "📅" : isFomc ? "🏛" : "📌"}</span>
              <span>{e.event_type?.toUpperCase()} in {e.days_away}d</span>
            </span>
          );
        })}
      </div>
    </div>
  );
}

// ── Score Bar ─────────────────────────────────────────────────────────────────

function ScoreBar({ score }: { score: number }) {
  return (
    <div className="flex items-center gap-3">
      <div className="flex-1 h-1.5 bg-zinc-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-700 ${scoreBarColor(score)}`}
          style={{ width: `${score}%` }}
        />
      </div>
      <span className={`text-sm font-numeric font-bold tabular-nums w-12 text-right ${scoreColor(score)}`}>
        {score?.toFixed(1)}
      </span>
    </div>
  );
}

// 5-dot score indicator: filled = min(5, round(score/20))
function ScoreDots({ score }: { score: number }) {
  const filled = Math.min(5, Math.round(score / 20));
  const color  = score >= 70 ? "text-emerald-400" : score >= 50 ? "text-amber-400" : "text-red-400";
  const tip =
    "Score out of 100 — higher = stronger setup. Dots = min(5, round(score÷20)): " +
    "5 filled ≈ 90+, 4 ≈ 70–89, 3 ≈ 50–69, 2 ≈ 30–49, 1 ≈ 10–29, 0 = under 10.";
  return (
    <span className={`text-[10px] font-numeric tracking-tighter ${color}`} title={tip}>
      {"●".repeat(filled)}{"○".repeat(5 - filled)}
    </span>
  );
}

// ── Trade Card (Changes 3 + 6) ────────────────────────────────────────────────

function TradeCard({ candidate, freshnessLevel, onApprove, onReject }: {
  candidate: Candidate;
  freshnessLevel: FreshnessLevel;
  onApprove: (id: number) => void | Promise<void>;
  onReject: (id: number) => void | Promise<void>;
}) {
  const [expanded, setExpanded] = useState(false);
  const [acting, setActing]     = useState(false);
  const [isRejecting, setIsRejecting] = useState(false);
  const [isDelegating, setIsDelegating] = useState(false);
  const [delegateSuccess, setDelegateSuccess] = useState(false);
  const delegateSuccessTimerRef = useRef<number | null>(null);

  const c    = typeof candidate.candidate_json === "string" ? JSON.parse(candidate.candidate_json) : candidate.candidate_json;
  const card = typeof candidate.llm_card        === "string" ? JSON.parse(candidate.llm_card)        : candidate.llm_card;

  const rec    = card?.recommendation ?? "no";
  const ivRank = c?.iv_rank;

  useEffect(() => {
    return () => {
      if (delegateSuccessTimerRef.current != null) {
        window.clearTimeout(delegateSuccessTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (acting || isRejecting || isDelegating) return;
      // 'A' only fires when data is live or delayed — not stale
      if (e.key.toLowerCase() === "a" && freshnessLevel !== "stale" && !candidate.is_stale) {
        setActing(true); onApprove(candidate.id);
      }
      if (e.key.toLowerCase() === "r") {
        void (async () => {
          setIsRejecting(true);
          try {
            await onReject(candidate.id);
          } finally {
            setIsRejecting(false);
          }
        })();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [candidate.id, candidate.is_stale, freshnessLevel, acting, isRejecting, isDelegating, onApprove, onReject]);

  return (
    <div className={`card-shadow border rounded-xl overflow-hidden ${recBorderBg(rec)}`}>

      {/* Row 1: Symbol · LLM badge · Score · freshness pill */}
      <div className="flex items-center justify-between px-3 pt-3 pb-2 border-b border-zinc-800/60">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-2xl font-black font-mono tracking-tight text-white">{candidate.symbol}</span>
          <span className={`text-xs font-mono font-bold px-2 py-0.5 rounded ${recBadgeBg(rec)}`}>
            {recLabel(rec)}
          </span>
          {freshnessLevel === "delayed" && (
            <span
              className="text-[10px] font-mono px-1.5 py-0.5 rounded-full flex items-center gap-1 text-warning bg-warning/10 border border-warning/20 cursor-help"
              title="Market data is delayed — prices may have moved since this candidate was scored."
            >
              <Clock size={7} /> Delayed
            </span>
          )}
          {freshnessLevel === "stale" && (
            <span className="text-[10px] font-mono px-1.5 py-0.5 rounded-full flex items-center gap-1 text-danger bg-negative/10 border border-negative/20">
              <Clock size={7} /> Stale
            </span>
          )}
        </div>
        <div className="flex flex-col items-end gap-0.5">
          <span className={`text-3xl font-black font-numeric tabular-nums ${scoreColor(candidate.score)}`}>
            {candidate.score?.toFixed(0)} <span className="text-lg opacity-70">/ 100</span>
          </span>
          <ScoreDots score={candidate.score} />
        </div>
      </div>

      {/* Row 2: Expiry · DTE · price · IV rank */}
      <div className="px-3 py-1.5 text-xs font-mono text-zinc-400 border-b border-zinc-800/60 space-y-1">
        <div>
          {c?.expiry} · {c?.dte} DTE · ${c?.underlying_price?.toFixed(2)}
          {ivRank != null && (
            <span
              className={`ml-2 font-bold ${
                isIronCondorStrategy(candidate.strategy ?? c?.strategy)
                  ? ivrIronCondorColor(Number(ivRank))
                  : "text-zinc-300"
              }`}
            >
              IVR {ivRank.toFixed(0)}
            </span>
          )}
        </div>
        <IronCondorSpreadSummary
          strategy={candidate.strategy ?? c?.strategy}
          long_put_strike={c?.long_put_strike}
          short_put_strike={c?.short_put_strike}
          short_call_strike={c?.short_call_strike}
          long_call_strike={c?.long_call_strike}
          expiry={c?.expiry}
          qty={c?.qty}
          className="text-[10px] leading-snug"
        />
      </div>

      {/* Row 3: Strike strip */}
      {c && (
        <div className="grid grid-cols-4 gap-px bg-zinc-800/40 mx-3 my-2 rounded-lg overflow-hidden">
          {[
            { label: "LONG PUT",   val: c.long_put_strike,   delta: null },
            { label: "SHORT PUT",  val: c.short_put_strike,  delta: c.short_put_delta },
            { label: "SHORT CALL", val: c.short_call_strike, delta: c.short_call_delta },
            { label: "LONG CALL",  val: c.long_call_strike,  delta: null },
          ].map(({ label, val, delta }) => (
            <div key={label} className="bg-zinc-900 px-2 py-2 text-center">
              <div className="text-[9px] font-mono text-zinc-600 mb-1">{label}</div>
              <div className="text-sm font-bold font-mono text-white">${val}</div>
              {delta != null && (
                <div className="text-[10px] font-mono text-zinc-500 mt-0.5">Δ {delta?.toFixed(3)}</div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Row 4: Metrics — Credit · Max Loss · R:R · PoP · Width */}
      {c && (
        <div className="flex items-center gap-4 px-3 pb-2 text-xs font-mono border-b border-zinc-800/60 flex-wrap">
          <span><span className="text-[10px] text-zinc-600 mr-1">CR</span><span className="text-emerald-400 font-bold">${c.net_credit?.toFixed(2)}</span></span>
          <span><span className="text-[10px] text-zinc-600 mr-1">LOSS</span><span className="text-red-400 font-bold">${c.max_loss?.toFixed(2)}</span></span>
          <span><span className="text-[10px] text-zinc-600 mr-1">R:R</span><span className="text-zinc-300 font-bold">{rr(c.net_credit, c.max_loss)}</span></span>
          <span><span className="text-[10px] text-zinc-600 mr-1">PoP</span><span className="text-zinc-300 font-bold">{pop(c.short_put_delta, c.short_call_delta)}</span></span>
          <span><span className="text-[10px] text-zinc-600 mr-1">W</span><span className="text-zinc-300 font-bold">${c.spread_width}</span></span>
        </div>
      )}

      {/* Row 5: LLM summary preview (2–3 lines) above the fold */}
      {card?.summary && (
        <p className="text-zinc-300 text-xs px-3 py-2 border-b border-zinc-800/60 whitespace-pre-line leading-relaxed">
          {previewLlmSummary(card.summary)}
        </p>
      )}

      {/* Context panel (market events — keeps own fetch) */}
      {c?.symbol && <ContextPanel symbol={c.symbol} />}

      {/* Row 6: Action buttons — 3 states based on freshnessLevel */}
      {freshnessLevel === "delayed" && !candidate.is_stale && (
        <div className="px-3 pt-2 text-[10px] font-mono text-warning/90 border-b border-zinc-800/40 bg-amber-950/10">
          Data may be delayed — verify current prices before acting.
        </div>
      )}
      <div className="grid grid-cols-2 gap-2 px-3 py-3 bg-zinc-950">
        {(freshnessLevel === "stale" || candidate.is_stale) ? (
          <button
            disabled
            className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg font-mono font-bold text-sm cursor-not-allowed opacity-50 bg-zinc-800 text-zinc-500"
          >
            <CheckCircle size={13} /> Refresh to Approve
          </button>
        ) : freshnessLevel === "delayed" ? (
          <button
            onClick={async () => {
              if (acting || isRejecting) return;
              setActing(true);
              try {
                await onApprove(candidate.id);
              } finally {
                setActing(false);
              }
            }}
            disabled={acting || isRejecting}
            title="Data is delayed — verify before approving"
            className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg border border-amber-500/60 text-amber-400 hover:bg-amber-900/30 font-mono font-bold text-sm transition-all disabled:opacity-50"
          >
            <CheckCircle size={13} /> Approve · Delayed Data
          </button>
        ) : (
          <button
            onClick={async () => {
              if (acting || isRejecting || isDelegating) return;
              setActing(true);
              try {
                await onApprove(candidate.id);
              } finally {
                setActing(false);
              }
            }}
            disabled={acting || isRejecting || isDelegating}
            className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-mono font-bold text-sm transition-all disabled:opacity-50"
          >
            <CheckCircle size={13} /> APPROVE (A)
          </button>
        )}
        <button
          onClick={async () => {
            if (acting || isRejecting || isDelegating) return;
            setIsRejecting(true);
            try {
              await onReject(candidate.id);
            } finally {
              setIsRejecting(false);
            }
          }}
          disabled={acting || isRejecting || isDelegating}
          className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg border border-red-500/40 text-red-400 hover:bg-red-900/50 font-mono font-bold text-sm transition-all disabled:opacity-50"
        >
          <XCircle size={13} /> {isRejecting ? "Rejecting..." : "REJECT (R)"}
        </button>
      </div>

      {/* Ask Sparky — live data only; full-width below Approve/Reject grid */}
      {freshnessLevel !== "stale" && !candidate.is_stale && freshnessLevel !== "delayed" && (
        <div className="px-3 pb-3 bg-zinc-950">
          <button
            type="button"
            disabled={acting || isRejecting || isDelegating || delegateSuccess}
            onClick={async () => {
              if (acting || isRejecting || isDelegating || delegateSuccess) return;
              setIsDelegating(true);
              try {
                await axios.post(`${API}/candidates/${candidate.id}/delegate`);
                if (delegateSuccessTimerRef.current != null) {
                  window.clearTimeout(delegateSuccessTimerRef.current);
                }
                setDelegateSuccess(true);
                delegateSuccessTimerRef.current = window.setTimeout(() => {
                  setDelegateSuccess(false);
                  delegateSuccessTimerRef.current = null;
                }, 1500);
              } catch (err: unknown) {
                const e = err as { response?: { data?: { error?: string; detail?: unknown } }; message?: string };
                const d = e?.response?.data;
                const msg =
                  (typeof d?.error === "string" && d.error) ||
                  (typeof d?.detail === "string" && d.detail) ||
                  e?.message ||
                  "Ask Sparky failed.";
                alert(msg);
              } finally {
                setIsDelegating(false);
              }
            }}
            className={`w-full flex items-center justify-center gap-2 py-2.5 rounded-lg font-mono font-bold text-sm transition-all disabled:opacity-50 disabled:cursor-not-allowed border ${
              delegateSuccess
                ? "border-emerald-500/60 bg-emerald-950/30 text-emerald-300"
                : "border-violet-500/45 text-violet-200 bg-violet-950/20 hover:bg-violet-950/35"
            }`}
          >
            {isDelegating ? (
              <>
                <Loader2 size={14} className="animate-spin shrink-0" />
                Sending...
              </>
            ) : delegateSuccess ? (
              <>
                <CheckCircle size={14} className="shrink-0" />
                Sent to Sparky
              </>
            ) : (
              "Ask Sparky"
            )}
          </button>
        </div>
      )}

      {/* Show Full Analysis — kept exactly as before */}
      <button
        onClick={() => setExpanded(e => !e)}
        className="flex items-center gap-1.5 text-xs text-zinc-500 hover:text-zinc-300 transition-colors font-mono px-3 py-2 w-full border-t border-zinc-800/60"
      >
        {expanded ? <ChevronUp size={11} /> : <ChevronDown size={11} />}
        {expanded ? "HIDE FULL ANALYSIS" : "SHOW FULL ANALYSIS"}
      </button>

      {expanded && card && (
        <div className="px-5 pb-4 space-y-4 text-sm border-t border-zinc-800/60 pt-4">
          {card.market_environment && (
            <div>
              <div className="text-xs font-mono text-zinc-600 mb-1.5 tracking-widest">MARKET ENVIRONMENT</div>
              <p className="text-zinc-300 leading-relaxed">{card.market_environment}</p>
            </div>
          )}
          {card.rationale?.length > 0 && (
            <div>
              <div className="text-xs font-mono text-zinc-600 mb-1.5 tracking-widest">RATIONALE</div>
              <ul className="space-y-1">
                {card.rationale.map((r: string, i: number) => (
                  <li key={i} className="flex gap-2 text-zinc-300 leading-relaxed">
                    <span className="text-emerald-500 shrink-0 mt-0.5">›</span>{r}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {card.setup_specific_risks?.length > 0 && (
            <div>
              <div className="text-xs font-mono text-zinc-600 mb-1.5 tracking-widest">SETUP-SPECIFIC RISKS</div>
              <ul className="space-y-1">
                {card.setup_specific_risks.map((r: string, i: number) => (
                  <li key={i} className="flex gap-2 text-zinc-300 leading-relaxed">
                    <span className="text-red-400 shrink-0 mt-0.5">›</span>{r}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {rec === "conditional" && card.conditions_if_conditional?.filter((s: string) => s && !s.toLowerCase().includes("only populate")).length > 0 && (
            <div>
              <div className="text-xs font-mono text-amber-500 mb-1.5 tracking-widest">CONDITIONS TO APPROVE</div>
              <ul className="space-y-1">
                {card.conditions_if_conditional
                  .filter((s: string) => s && !s.toLowerCase().includes("only populate"))
                  .map((cond: string, i: number) => (
                    <li key={i} className="flex gap-2 text-amber-300 leading-relaxed">
                      <span className="shrink-0 mt-0.5">›</span>{cond}
                    </li>
                  ))}
              </ul>
            </div>
          )}
          {card.red_flags?.filter((f: string) => f).length > 0 && (
            <div>
              <div className="text-xs font-mono text-red-400 mb-1.5 tracking-widest">RED FLAGS</div>
              <ul className="space-y-1">
                {card.red_flags.map((f: string, i: number) => (
                  <li key={i} className="flex gap-2 text-red-300 leading-relaxed">
                    <span className="shrink-0">⚑</span>{f}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {card._meta?.model && (
            <div className="text-zinc-700 text-xs font-mono pt-1">
              {card._meta.model} · {candidate.age_minutes.toFixed(0)}m ago
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Shadow / History rows (GET /shadow, GET /history) ─────────────────────────

function PipelineFunnelBar({
  stats,
  windowHours,
  onWindowHours,
  cutoffIso,
}: {
  stats: PipelineStats | null;
  windowHours?: number;
  onWindowHours?: (h: number) => void;
  cutoffIso?: string;
}) {
  if (!stats) {
    const cutoffHumanNs = cutoffIso ? formatIsoShort(cutoffIso) : "";
    return (
      <div className="card-surface border-l-2 border-info/30 p-4 mb-4 text-[10px] text-tertiary font-numeric space-y-2">
        {windowHours != null && onWindowHours && (
          <div className="flex flex-wrap items-center gap-2 font-numeric">
            <span className="text-[10px] text-tertiary tracking-widest shrink-0">PIPELINE FUNNEL</span>
            <div className="flex flex-wrap items-center gap-1" title={cutoffHumanNs ? `Since ${cutoffHumanNs}` : undefined}>
              <span className="text-tertiary text-[10px]">·</span>
              {([24, 48, 72, 168] as const).map(h => (
                <button
                  key={h}
                  type="button"
                  onClick={() => onWindowHours(h)}
                  className={`px-2 py-0.5 rounded border text-[10px] transition-colors ${
                    windowHours === h
                      ? "border-accent text-accent bg-accent/10"
                      : "border-subtle text-secondary hover:border-focus"
                  }`}
                >
                  {h === 168 ? "7d" : `${h}h`}
                </button>
              ))}
            </div>
            {cutoffHumanNs ? (
              <span className="text-[10px] text-tertiary ml-auto" title={cutoffIso}>
                Since {cutoffHumanNs}
              </span>
            ) : null}
          </div>
        )}
        <div>
          Pipeline funnel unavailable — <code className="text-tertiary">/pipeline-stats</code> did not return data.
        </div>
      </div>
    );
  }
  const max = Math.max(1, stats.scanned);
  const awaiting = stats.awaiting_operator_decision ?? 0;
  const cutoffHuman = cutoffIso ? formatIsoShort(cutoffIso) : (stats.cutoff ? formatIsoShort(stats.cutoff) : "");
  const row = (label: string, n: number, color: string) => (
    <div key={label} className="flex items-center gap-2 text-[10px]">
      <span className="text-tertiary w-40 shrink-0 truncate" title={label}>{label}</span>
      <div className="flex-1 h-2 bg-white/5 rounded overflow-hidden min-w-0">
        <div className={`h-full rounded ${color}`} style={{ width: `${Math.min(100, (n / max) * 100)}%` }} />
      </div>
      <span className="font-numeric text-secondary w-10 text-right shrink-0">{n}</span>
    </div>
  );
  return (
    <div className="card-surface border-l-2 border-info/35 p-4 mb-4 space-y-1.5">
      <div className="flex flex-wrap items-center gap-2 mb-2 font-numeric">
        <span className="text-[10px] text-tertiary tracking-widest shrink-0">PIPELINE FUNNEL</span>
        {windowHours != null && onWindowHours && (
          <div className="flex flex-wrap items-center gap-1" title={cutoffHuman ? `Since ${cutoffHuman}` : undefined}>
            <span className="text-tertiary text-[10px]">·</span>
            {([24, 48, 72, 168] as const).map(h => (
              <button
                key={h}
                type="button"
                onClick={() => onWindowHours(h)}
                className={`px-2 py-0.5 rounded border text-[10px] transition-colors ${
                  windowHours === h
                    ? "border-accent text-accent bg-accent/10"
                    : "border-subtle text-secondary hover:border-focus"
                }`}
              >
                {h === 168 ? "7d" : `${h}h`}
              </button>
            ))}
          </div>
        )}
        {cutoffHuman ? (
          <span className="text-[10px] text-tertiary ml-auto" title={cutoffIso ?? stats.cutoff}>
            Since {cutoffHuman}
          </span>
        ) : null}
      </div>
      {row("Scanned", stats.scanned, "bg-zinc-500/90")}
      {row("Blocked @ gate", stats.blocked, "bg-amber-500/85")}
      {row("Passed gates", stats.passed_gates, "bg-cyan-600/80")}
      {row("LLM evaluated", stats.llm_evaluated, "bg-violet-500/80")}
      {stats.circuit_broken > 0 ? row("Circuit gap (no LLM card)", stats.circuit_broken, "bg-red-500/70") : null}
      {row("Operator approved", stats.approved, "bg-emerald-500/85")}
      {row("Operator rejected", stats.rejected, "bg-red-400/55")}
      {awaiting > 0 ? row("Awaiting operator choice", awaiting, "bg-slate-500/80") : null}
      {stats.expired > 0 ? row("Expired in UI", stats.expired, "bg-amber-600/50") : null}
      <p className="text-[9px] text-tertiary font-numeric leading-relaxed pt-1 border-t border-subtle/60">
        After LLM: approved + rejected + awaiting (+ expired UI) accounts for candidates still in-flight or unresolved.
        Awaiting = LLM-ready rows in this window with no operator decision yet.
      </p>
    </div>
  );
}

function GateKillHeatmap({
  bins,
  usedClientFallback,
}: {
  bins: GateKillBin[];
  usedClientFallback: boolean;
}) {
  if (!bins.length) {
    return (
      <div className="text-[10px] text-tertiary font-numeric mb-3">
        No gate-kill distribution for this window.
      </div>
    );
  }
  const max = bins[0]?.count ?? 1;
  return (
    <div className="card-surface border-l-2 border-warning/25 p-4 mb-4">
      <div className="text-[10px] text-tertiary tracking-widest mb-2 font-numeric">GATE KILL HEATMAP</div>
      {usedClientFallback && (
        <p className="text-[9px] text-warning/80 mb-2 font-numeric">
          Using client tally from loaded shadow rows (≤100); full-window totals come from the API when available.
        </p>
      )}
      <div className="space-y-1.5">
        {bins.map((b, i) => (
          <div key={`${b.rule}-${i}`} className="flex items-center gap-2 text-[10px]">
            <span className="text-secondary truncate max-w-[160px] shrink-0" title={b.label}>{b.label}</span>
            <div className="flex-1 h-3 bg-white/5 rounded overflow-hidden min-w-0">
              <div
                className="h-full bg-warning/65 rounded"
                style={{ width: `${Math.max(4, (b.count / max) * 100)}%` }}
              />
            </div>
            <span className="font-numeric text-secondary w-8 text-right shrink-0">{b.count}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ShadowBlockedRow({ row }: { row: ShadowBlocked }) {
  const [open, setOpen] = useState(false);
  const rawRule = blockedRuleRawKey(row.blocked_reason);
  const ruleHuman = row.gate_rule_label ?? gateRuleHumanLabel(rawRule);
  const detail = gateDetail(row.blocked_reason);
  const hasDetail = detail.trim().length > 0;
  const { actual, threshold, operator } = gateBlockedMetrics(row.blocked_reason);
  const hasMetrics = actual != null || threshold != null || operator != null;
  const metricRuleKey = rawRule === LEGACY_UNKNOWN_RULE ? "" : rawRule;

  return (
    <div className="card-surface mb-2 overflow-hidden w-full border-l-[3px] border-cyan-400/70 opacity-[0.92]">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-start gap-3 px-4 py-3 hover:bg-white/[0.02] transition-colors text-left"
      >
        {open ? <ChevronUp size={13} className="text-tertiary shrink-0 mt-0.5" /> : <ChevronDown size={13} className="text-tertiary shrink-0 mt-0.5" />}
        <div className="flex-1 min-w-0 flex flex-col gap-1.5 text-xs">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-primary font-bold">{row.symbol}</span>
            <span className="inline-flex items-center gap-1.5 text-[10px] px-1.5 py-0.5 rounded font-numeric border border-subtle bg-white/5">
              <span className={`inline-block w-2 h-2 rounded-full shrink-0 ${scoreDotBg(row.score)}`} title="Score band" />
              <span className={scoreColor(row.score)}>{row.score.toFixed(0)}</span>
            </span>
            <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-warning/10 text-warning border border-warning/25 max-w-[220px] truncate" title={ruleHuman}>
              {ruleHuman}
            </span>
            <span className="text-tertiary font-numeric">{formatIsoShort(row.created_at)}</span>
          </div>
          <IronCondorSpreadSummary
            strategy={row.strategy}
            long_put_strike={row.long_put_strike}
            short_put_strike={row.short_put_strike}
            short_call_strike={row.short_call_strike}
            long_call_strike={row.long_call_strike}
            expiry={row.expiry}
            qty={row.qty}
            className="text-[10px] leading-snug"
          />
        </div>
      </button>
      {open && (
        <div className="border-t border-subtle bg-zinc-950 px-4 py-3 space-y-3 text-xs">
          <div>
            <div className="text-[10px] text-tertiary tracking-widest mb-1 font-numeric">BLOCKING RULE</div>
            <div className="text-secondary">{ruleHuman}</div>
          </div>
          {hasMetrics && (
            <div className="rounded-lg border border-subtle/80 bg-white/[0.02] px-3 py-2 space-y-1 font-numeric">
              <div className="text-[10px] text-tertiary tracking-widest">THRESHOLD CHECK</div>
              {operator && (
                <div className="text-secondary">
                  Pass if actual <span className="text-info">{operator}</span> threshold
                </div>
              )}
              <div className="grid grid-cols-2 gap-2 text-[11px]">
                <div><span className="text-tertiary">Actual · </span><span className="text-primary">{formatGateMetric(metricRuleKey, "actual", actual)}</span></div>
                <div><span className="text-tertiary">Threshold · </span><span className="text-primary">{formatGateMetric(metricRuleKey, "threshold", threshold)}</span></div>
              </div>
            </div>
          )}
          {hasDetail && (
            <div>
              <div className="text-[10px] text-tertiary tracking-widest mb-1">DETAIL</div>
              <div className="text-secondary whitespace-pre-wrap leading-relaxed">{detail}</div>
            </div>
          )}
          <div className="grid grid-cols-2 gap-2 text-secondary font-numeric">
            <div><span className="text-tertiary">Net credit · </span>{row.net_credit != null && row.net_credit !== "" ? String(row.net_credit) : "—"}</div>
            <div><span className="text-tertiary">Expiry · </span>{row.expiry ?? "—"}</div>
            <div><span className="text-tertiary">Snapshot · </span>{row.snapshot_id ?? "—"}</div>
            <div><span className="text-tertiary">Candidate id · </span>{row.id}</div>
          </div>
          <p className="text-[10px] text-secondary border-t border-subtle pt-2 leading-relaxed">
            Blocked by rules gate — LLM evaluation was not reached for this candidate.
          </p>
        </div>
      )}
    </div>
  );
}

function HistoryLogRow({ row }: { row: HistoryRow }) {
  const [open, setOpen] = useState(false);
  const bucket = historyDecisionBucket(row.decision);
  const bucketCls =
    bucket === "approved" ? "text-success bg-positive/10 border-positive/30"
      : bucket === "rejected" ? "text-danger bg-negative/10 border-negative/30"
        : "text-tertiary bg-white/5 border-subtle";
  const gd = row.gate_diagnostics;
  const hasGateDiag = gd != null && typeof gd === "object" && Object.keys(gd).length > 0;
  const gRaw = hasGateDiag ? blockedRuleRawKey(gd) : "";
  const gHuman = hasGateDiag ? gateRuleHumanLabel(gRaw) : "";
  const gDetail = hasGateDiag ? gateDetail(gd) : "";
  const gMetrics = hasGateDiag ? gateBlockedMetrics(gd) : { actual: null, threshold: null, operator: null };
  const hasGMetrics = hasGateDiag && (gMetrics.actual != null || gMetrics.threshold != null || gMetrics.operator != null);
  const gMetricKey = gRaw === LEGACY_UNKNOWN_RULE ? "" : gRaw;

  const rec = row.llm_recommendation != null ? String(row.llm_recommendation).toLowerCase() : "";
  const hasLlm = rec === "yes" || rec === "no" || rec === "conditional";
  const conf = typeof row.llm_confidence === "number" && !Number.isNaN(row.llm_confidence)
    ? Math.max(0, Math.min(1, row.llm_confidence))
    : null;

  return (
    <div className="card-surface mb-2 overflow-hidden w-full">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-start gap-3 px-4 py-3 hover:bg-white/[0.02] transition-colors text-left"
      >
        {open ? <ChevronUp size={13} className="text-tertiary shrink-0 mt-0.5" /> : <ChevronDown size={13} className="text-tertiary shrink-0 mt-0.5" />}
        <div className="flex-1 min-w-0 flex flex-col gap-1.5 text-xs">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-primary font-bold text-[13px]">{row.symbol}</span>
            <span className={`text-[11px] px-2 py-0.5 rounded font-numeric border font-semibold ${bucketCls}`}>
              {row.decision}
            </span>
            <span className="inline-flex items-center gap-1.5 text-[10px] px-1.5 py-0.5 rounded font-numeric border border-subtle bg-white/5">
              <span className={`inline-block w-2 h-2 rounded-full shrink-0 ${scoreDotBg(Number(row.score))}`} />
              <span className={scoreColor(Number(row.score))}>{Number(row.score).toFixed(0)}</span>
            </span>
            <span className="text-tertiary font-numeric">{formatIsoShort(row.decided_at)}</span>
            {row.pnl != null && Number.isFinite(row.pnl) && (
              <span className={`font-numeric font-medium ${row.pnl >= 0 ? "text-success" : "text-danger"}`}>
                P/L ${row.pnl.toFixed(2)}
              </span>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-2 opacity-80">
            {hasLlm && (
              <span className="text-[9px] px-1.5 py-0.5 rounded font-numeric border border-subtle/60 bg-white/[0.03] text-tertiary">
                Model ·{" "}
                <span className={rec === "yes" ? "text-success" : rec === "no" ? "text-danger" : "text-warning"}>
                  {recLabel(rec)}
                </span>
              </span>
            )}
            {conf != null && (
              <span className="text-[9px] text-tertiary/90 font-numeric" title="LLM confidence">
                Conf {confPct(conf)}
              </span>
            )}
          </div>
          <IronCondorSpreadSummary
            strategy={row.strategy}
            long_put_strike={row.long_put_strike}
            short_put_strike={row.short_put_strike}
            short_call_strike={row.short_call_strike}
            long_call_strike={row.long_call_strike}
            expiry={row.expiry}
            qty={row.qty}
            className="text-[10px] leading-snug"
          />
        </div>
      </button>
      {open && (
        <div className="border-t border-subtle bg-zinc-950 px-4 py-2.5 space-y-2 text-xs">
          <div className="text-secondary font-numeric text-[11px] leading-snug space-y-1">
            <div className="history-meta-block">
              <span><span className="text-tertiary">Account · </span>{formatAccountDisplay(row.account_id)}</span>
              <span className="text-tertiary/20 select-none px-0.5" aria-hidden>·</span>
              <span><span className="text-tertiary">Created · </span>{formatIsoShort(row.created_at)}</span>
              <span className="text-tertiary/20 select-none px-0.5" aria-hidden>·</span>
              <span><span className="text-tertiary">Expiry · </span>{row.expiry ?? "—"}</span>
            </div>
            <div className="history-meta-block">
              <span><span className="text-tertiary">Decided · </span>{formatIsoShort(row.decided_at)}</span>
              <span className="text-tertiary/20 select-none px-0.5" aria-hidden>·</span>
              <span><span className="text-tertiary">Net credit · </span>{row.net_credit != null && row.net_credit !== "" ? String(row.net_credit) : "—"}</span>
              <span className="text-tertiary/20 select-none px-0.5" aria-hidden>·</span>
              <span><span className="text-tertiary">Closed · </span>{row.closed_at ? formatIsoShort(row.closed_at) : "—"}</span>
            </div>
          </div>
          {(row.reason != null && String(row.reason).trim()) ? (
            <div>
              <div className="text-[10px] text-tertiary tracking-widest mb-0.5 font-numeric">DECISION REASON</div>
              <div className="text-secondary whitespace-pre-wrap text-[11px] leading-snug">{row.reason}</div>
            </div>
          ) : null}
          {(row.exit_reason != null && String(row.exit_reason).trim()) ? (
            <div>
              <div className="text-[10px] text-tertiary tracking-widest mb-0.5 font-numeric">EXIT REASON</div>
              <div className="text-secondary whitespace-pre-wrap text-[11px] leading-snug">{row.exit_reason}</div>
            </div>
          ) : null}

          <div className="border-t border-subtle pt-2 space-y-1.5">
            <div className="text-[10px] text-tertiary tracking-widest font-numeric">Gates at decision</div>
            {!hasGateDiag ? (
              <p className="text-[11px] text-secondary leading-relaxed">All gates passed — no blocks recorded.</p>
            ) : (
              <>
                <div className="text-secondary text-[11px]"><span className="text-tertiary">Rule · </span>{gHuman || "—"}</div>
                {gDetail.trim() ? (
                  <div className="text-secondary whitespace-pre-wrap text-[11px]">{gDetail}</div>
                ) : null}
                {hasGMetrics && (
                  <div className="rounded border border-subtle/60 bg-white/[0.02] px-2 py-2 font-numeric text-[11px] space-y-1">
                    {gMetrics.operator ? (
                      <div className="text-tertiary">Operator <span className="text-info">{gMetrics.operator}</span></div>
                    ) : null}
                    <div className="grid grid-cols-2 gap-2">
                      <div><span className="text-tertiary">Actual · </span>{formatGateMetric(gMetricKey, "actual", gMetrics.actual)}</div>
                      <div><span className="text-tertiary">Threshold · </span>{formatGateMetric(gMetricKey, "threshold", gMetrics.threshold)}</div>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>

          <div className="border-t border-subtle pt-2 space-y-1.5">
            <div className="text-[10px] text-tertiary tracking-widest font-numeric">Model (LLM)</div>
            {!hasLlm && !(row.llm_reasoning && String(row.llm_reasoning).trim()) && !row.llm_model ? (
              <p className="text-[10px] text-tertiary leading-snug">No model output stored for this row.</p>
            ) : (
              <>
                {hasLlm && (
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-[9px] px-1.5 py-0.5 rounded font-numeric font-medium border border-subtle/70 bg-white/[0.04] text-tertiary">
                      <span className={rec === "yes" ? "text-success" : rec === "no" ? "text-danger" : "text-warning"}>
                        {recLabel(rec)}
                      </span>
                    </span>
                    {conf != null && (
                      <div className="flex-1 min-w-[120px] max-w-xs flex items-center gap-2">
                        <div className="flex-1 h-2 bg-white/10 rounded overflow-hidden">
                          <div
                            className={`h-full ${rec === "yes" ? "bg-emerald-500" : rec === "no" ? "bg-red-500" : "bg-amber-500"}`}
                            style={{ width: `${conf * 100}%` }}
                          />
                        </div>
                        <span className="text-[10px] text-tertiary font-numeric w-10">{confPct(conf)}</span>
                      </div>
                    )}
                  </div>
                )}
                {(row.llm_reasoning != null && String(row.llm_reasoning).trim()) ? (
                  <div className="text-secondary whitespace-pre-wrap leading-relaxed text-[11px]">{String(row.llm_reasoning)}</div>
                ) : null}
                <div className="text-[10px] text-tertiary font-numeric flex flex-wrap gap-x-3 gap-y-1">
                  {row.llm_model != null && String(row.llm_model).trim() ? <span>Model · {String(row.llm_model)}</span> : null}
                  {row.llm_latency != null && Number.isFinite(Number(row.llm_latency)) ? (
                    <span>Latency · {Number(row.llm_latency).toFixed(0)}ms</span>
                  ) : null}
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Position Row (Changes 5 + 6) ──────────────────────────────────────────────

const strategyBadge: Record<string, string> = {
  IRON_CONDOR:    "IC",
  SHORT_OPTION:   "SO",
  LONG_OPTION:    "LO",
  EQUITY:         "EQ",
  STRANGLE:       "STR",
  STRADDLE:       "STD",
  VERTICAL_SPREAD: "VS",
};

function formatClosePositionError(err: unknown): string {
  const e = err as { response?: { data?: { detail?: unknown } }; message?: string };
  const d = e?.response?.data?.detail;
  if (typeof d === "string") return d;
  if (Array.isArray(d))
    return d.map((x: { msg?: string }) => x?.msg ?? JSON.stringify(x)).join("; ");
  if (d != null && typeof d === "object") return JSON.stringify(d);
  return e?.message ?? "Close request failed.";
}

function PositionRow({ p, onPositionClosed }: { p: Position; onPositionClosed?: (id: number) => void }) {
  const [open, setOpen] = useState(false);
  const [closeConfirmOpen, setCloseConfirmOpen] = useState(false);
  const [exitDebitStr, setExitDebitStr] = useState("");
  const [closeLoading, setCloseLoading] = useState(false);
  const [closeErr, setCloseErr] = useState<string | null>(null);
  const [closeSuccessPnl, setCloseSuccessPnl] = useState<number | null>(null);

  const credit  = p.fill_credit ?? p.entry_credit;
  const pnl     = p.unrealized_pnl ?? 0;
  const pct     = p.profit_pct;
  const target50 = credit != null ? credit * 0.5 : null;
  const stop200  = credit != null ? credit * 2.0 : null;

  const dte = typeof p.dte === "number" && !Number.isNaN(p.dte)
    ? p.dte
    : (p.expiry ? Math.ceil((new Date(p.expiry).getTime() - Date.now()) / 86400000) : NaN);
  const stratUpper   = (p.strategy ?? "").toUpperCase();
  const isEquity     = stratUpper === "EQUITY";
  const canPaperClose =
    p.account_id === "PAPER" && (stratUpper === "IRON_CONDOR" || stratUpper === "SHORT_OPTION");
  const dteDisplay   = isEquity ? "—" : (Number.isNaN(dte) || dte === undefined ? "--d" : `${dte}d`);
  const accountBadge =
    p.account_id === "PAPER"
      ? "PAPER"
      : p.account_id === "primary"
        ? "legacy·primary"
        : `···${p.account_id?.slice(-4) ?? ""}`;
  const mark         = p.mark;

  // Correction 4: new chip logic
  const chipKey = getChip(mark, credit, Number.isNaN(dte) ? null : dte);
  const computedIcMaxRisk = computeIronCondorMaxRiskUsd(p);
  const displayMaxRisk =
    computedIcMaxRisk != null
      ? `$${computedIcMaxRisk.toFixed(2)}`
      : p.max_risk != null && Number.isFinite(Number(p.max_risk))
        ? `$${Number(p.max_risk).toFixed(2)}`
        : "—";
  const netD = typeof p.net_delta === "number" && !Number.isNaN(p.net_delta) ? p.net_delta : null;
  const absNetD = netD != null ? Math.abs(netD) : null;
  const isPaperAcct = p.account_id === "PAPER";

  useEffect(() => {
    if (closeSuccessPnl == null) return;
    const t = window.setTimeout(() => {
      onPositionClosed?.(p.id);
      setCloseSuccessPnl(null);
    }, 2200);
    return () => window.clearTimeout(t);
  }, [closeSuccessPnl, p.id, onPositionClosed]);

  return (
    // Change 6: card-surface (bg + border + shadow + gradient)
    <div
      className={`card-surface mb-3 overflow-hidden w-full ${
        isPaperAcct ? "border-l-[3px] border-l-blue-500/30 bg-blue-500/[0.03]" : ""
      }`}
    >
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-3 px-5 py-3.5 hover:bg-white/[0.02] transition-colors text-left"
      >
        {open ? <ChevronUp size={13} className="text-tertiary shrink-0" /> : <ChevronDown size={13} className="text-tertiary shrink-0" />}

        {/* Left group — symbol + IC tag + DTE + qty + credit + condor strip */}
        <div className="flex flex-col gap-1 flex-1 min-w-0">
          <div className="flex items-center gap-3 flex-wrap font-mono">
            <span className="text-primary font-bold text-base">{p.symbol}</span>
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-white/5 text-tertiary border border-subtle">
              {strategyBadge[(p.strategy ?? "IRON_CONDOR").toUpperCase()] ?? "—"}
            </span>
            <span className="text-secondary text-xs">{p.expiry} · {dteDisplay}</span>
            {p.qty != null && (
              <span className="text-tertiary text-xs">{isEquity ? `${p.qty} sh` : `${p.qty}×`}</span>
            )}
            <span className="text-secondary text-xs">
              {credit != null ? `$${credit.toFixed(2)} ${isEquity ? "avg" : "cr"}` : "—"}
            </span>
            <span className="text-secondary text-xs">
              Mark: {mark != null ? `$${mark.toFixed(2)}` : "—"}
            </span>
          </div>
          <IronCondorSpreadSummary
            strategy={p.strategy}
            long_put_strike={p.long_put_strike}
            short_put_strike={p.short_put_strike}
            short_call_strike={p.short_call_strike}
            long_call_strike={p.long_call_strike}
            expiry={p.expiry}
            qty={p.qty}
            className="text-[10px] leading-snug"
          />
        </div>

        {/* Right group — P/L + account + status chip */}
        <div className="flex items-center gap-3 font-mono text-xs shrink-0">
          <span className={pnl !== 0 ? (pnl >= 0 ? "text-success font-bold" : "text-danger font-bold") : "text-secondary"}>
            {mark != null
              ? `${pnl >= 0 ? "+" : ""}$${pnl.toFixed(0)}`
              : "—"}
            {mark != null && pct != null && (
              <span className="opacity-60 ml-1 text-[10px]">{pct >= 0 ? "+" : ""}{pct.toFixed(0)}%</span>
            )}
          </span>
          <span className="text-tertiary text-[10px]">{accountBadge}</span>
          <span className={`text-[10px] px-1.5 py-0.5 rounded font-mono font-bold ${chipStyle[chipKey]}`}>
            {chipKey}
          </span>
        </div>
      </button>

      {open && (
        <div className="border-t border-subtle bg-zinc-950">
          {closeSuccessPnl != null && (
            <div className="px-4 py-2.5 border-b border-amber-500/35 bg-amber-950/25 text-[11px] font-mono text-amber-100/95">
              Position closed · Realized P/L:{" "}
              <span className={`font-bold ${closeSuccessPnl >= 0 ? "text-success" : "text-danger"}`}>
                {closeSuccessPnl >= 0 ? "+" : ""}${closeSuccessPnl.toFixed(2)}
              </span>
              <span className="text-tertiary ml-2">(removing from list…)</span>
            </div>
          )}
          {/* Two-column: ENTRY | CURRENT */}
          <div className="grid grid-cols-2 divide-x divide-subtle">
            <div className="p-4">
              <div className="text-[10px] font-mono text-tertiary tracking-widest mb-3">ENTRY</div>
              <div className="space-y-2 text-sm font-mono">
                {([
                  [isEquity ? "Avg price"  : "Credit received", credit != null ? `$${credit.toFixed(2)}` : "—"],
                  [isEquity ? "Shares"     : "Contracts",       p.qty != null ? String(p.qty) : "—"],
                  ...(!isEquity ? [["Total credit gained", credit != null && p.qty != null ? `$${(credit * p.qty * 100).toFixed(2)}` : "—"]] as [string, string][] : []),
                  ...(!isEquity ? [["Max risk (margin)",  displayMaxRisk]] as [string, string][] : []),
                  ...(!isEquity ? [["Net delta",          netD != null ? netD.toFixed(4) : "—"]] as [string, string][] : []),
                  ["Account", accountBadge],
                ] as [string, string][]).map(([l, v]) => (
                  <div key={l} className="flex justify-between">
                    <span className="text-tertiary">{l}</span>
                    <span className="text-secondary">{v}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="p-4">
              <div className="text-[10px] font-mono text-tertiary tracking-widest mb-3">CURRENT</div>
              <div className="space-y-2 text-sm font-mono">
                {([
                  ["Mark", { val: mark != null ? `$${mark.toFixed(2)}` : "—", color: "text-secondary" }],
                  ["Gross P/L", { val: `${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}`, color: pnl >= 0 ? "text-success" : "text-danger" }],
                  [isEquity ? "% change" : "% of credit", { val: pct != null ? `${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%` : "—", color: (pct ?? 0) >= 0 ? "text-success" : "text-danger" }],
                  ...(!isEquity ? [["50% target", { val: target50 != null ? `$${target50.toFixed(2)}` : "—", color: "text-secondary" }]] as [string, { val: string; color: string }][] : []),
                  ...(!isEquity ? [["200% stop",  { val: stop200  != null ? `$${stop200.toFixed(2)}`  : "—", color: "text-danger"    }]] as [string, { val: string; color: string }][] : []),
                ] as [string, { val: string; color: string }][]).map(([l, v]) => (
                  <div key={l as string} className="flex justify-between">
                    <span className="text-tertiary">{l as string}</span>
                    <span className={v.color}>{v.val}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Leg strikes + prices + breakevens — IRON_CONDOR only */}
          {(p.strategy ?? "").toUpperCase() === "IRON_CONDOR" && (() => {
            const legPrices = getCondorLegPrices(p.legs, p.legs_json);
            const hasAnyPrice = legPrices.lp != null || legPrices.sp != null || legPrices.sc != null || legPrices.lc != null;
            const be = computeBreakevens(p.short_put_strike, p.short_call_strike, safeNumericCredit(credit));
            const hasAnyBe = be.put != null || be.call != null;
            const fmt = (v: number | null) => (v != null ? `$${v.toFixed(2)}` : "—");
            const strikeSep = <span className="text-tertiary"> | </span>;
            return (
              <div className="px-4 py-3 border-t border-subtle space-y-2">
                <div>
                  <div className="text-[10px] font-mono text-tertiary tracking-widest mb-1">LEG STRIKES</div>
                  <div className="text-xs font-mono">
                    <span className="text-tertiary">LP</span>{" "}
                    <span className="text-secondary">{formatCondorLegStrike(p.long_put_strike)}</span>
                    {strikeSep}
                    <span className="text-tertiary">SP</span>{" "}
                    <span className="text-secondary">{formatCondorLegStrike(p.short_put_strike)}</span>
                    {strikeSep}
                    <span className="text-tertiary">SC</span>{" "}
                    <span className="text-secondary">{formatCondorLegStrike(p.short_call_strike)}</span>
                    {strikeSep}
                    <span className="text-tertiary">LC</span>{" "}
                    <span className="text-secondary">{formatCondorLegStrike(p.long_call_strike)}</span>
                  </div>
                </div>
                {hasAnyPrice && (
                  <div>
                    <div className="text-[10px] font-mono text-tertiary tracking-widest mb-1">LEG PRICES</div>
                    <div className="text-xs font-mono text-secondary">
                      LP {fmt(legPrices.lp)} | SP {fmt(legPrices.sp)} | SC {fmt(legPrices.sc)} | LC {fmt(legPrices.lc)}
                    </div>
                  </div>
                )}
                {hasAnyBe && (
                  <div>
                    <div className="text-[10px] font-mono text-tertiary tracking-widest mb-1">BREAKEVENS</div>
                    <div className="text-xs font-mono text-secondary">
                      Put {be.put != null ? `$${be.put.toFixed(2)}` : "—"} | Call {be.call != null ? `$${be.call.toFixed(2)}` : "—"}
                    </div>
                  </div>
                )}
              </div>
            );
          })()}

          {/* DTE timeline, Greeks, Exit Status, Action buttons — options only */}
          {!isEquity && (
            <>
              {/* DTE timeline: cycle = max(45, dte) so bar never overflows */}
              {!Number.isNaN(dte) && (
                <div className="px-4 py-3 border-t border-subtle">
                  <div className="text-[10px] font-mono text-tertiary tracking-widest mb-2">DTE TIMELINE</div>
                  {(() => {
                    const totalDays  = Math.max(45, dte ?? 0);
                    const elapsed    = totalDays - (dte ?? 0);
                    const pctElapsed = Math.min(100, Math.max(0, (elapsed / totalDays) * 100));
                    return (
                      <>
                        <div className="relative h-1.5 bg-white/5 rounded-full overflow-hidden">
                          <div className="h-full rounded-full bg-accent/40" style={{ width: `${pctElapsed}%` }} />
                        </div>
                        <div className="flex justify-between text-[10px] font-mono text-tertiary mt-1">
                          <span>Entry</span>
                          <span className="text-secondary">{dte}d remaining</span>
                          <span>{totalDays} DTE</span>
                        </div>
                      </>
                    );
                  })()}
                </div>
              )}

              {/* Greeks — net delta from book; other greeks not persisted */}
              <div className="px-4 py-3 border-t border-subtle">
                <div className="text-[10px] font-mono text-tertiary tracking-widest mb-0.5">GREEKS · ENTRY / SNAPSHOT</div>
                <p className="text-[9px] text-tertiary font-mono mb-2 leading-snug">
                  Gamma, theta, and vega are not stored on open positions. Delta uses the last position net delta from the book.
                </p>
                <div className="grid grid-cols-4 gap-2 text-xs font-mono text-center">
                  <div>
                    <div className="text-[10px] text-tertiary mb-0.5">DELTA</div>
                    <div className={`font-numeric ${absNetD != null ? directionalDeltaTone(absNetD) : "text-secondary"}`}>
                      {netD != null ? netD.toFixed(2) : "—"}
                    </div>
                  </div>
                  {(["Gamma", "Theta", "Vega"] as const).map(g => (
                    <div key={g}>
                      <div className="text-[10px] text-tertiary mb-0.5">{g.toUpperCase()}</div>
                      <div className="text-secondary">—</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Exit status */}
              <div className="px-4 py-2 border-t border-subtle flex items-center justify-between">
                <span className="text-[10px] font-mono text-tertiary tracking-widest">EXIT STATUS</span>
                <span className={`text-[10px] font-mono font-bold ${chipStyle[chipKey]}`}>
                  {chipKey === "TARGET" ? "50% profit target reached" :
                   chipKey === "DANGER" ? "Stop loss approaching" :
                   chipKey === "WATCH"  ? "Time exit zone (<14 DTE)" :
                   "Monitoring"}
                </span>
              </div>

              {/* Action — only show implemented controls (paper IC/SO close) */}
              {canPaperClose && (
                <div className="grid grid-cols-1 gap-2 px-4 pt-3 pb-4 border-t border-subtle">
                  <button
                    type="button"
                    disabled={closeLoading || closeSuccessPnl != null}
                    title="Close this paper position"
                    onClick={() => {
                      setCloseConfirmOpen(true);
                      setCloseErr(null);
                      setExitDebitStr(
                        mark != null && Number.isFinite(Number(mark))
                          ? Number(mark).toFixed(2)
                          : "",
                      );
                    }}
                    className="py-2 text-[10px] font-mono rounded-lg border border-amber-500/45 text-amber-100/90 bg-amber-950/20 hover:bg-amber-950/35 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                  >
                    Close Position
                  </button>
                </div>
              )}

              {canPaperClose && closeConfirmOpen && closeSuccessPnl == null && (
                <div className="mx-4 mb-4 mt-2 p-3 rounded-lg border border-amber-500/30 bg-amber-950/15 space-y-2">
                  <div className="text-[10px] font-mono text-amber-200/90 tracking-wide">
                    Confirm close — enter exit debit (per contract, per share).{" "}
                    {mark != null && Number.isFinite(Number(mark))
                      ? "Default is current mark."
                      : "No mark on file — enter manually."}
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <label className="text-[10px] text-tertiary font-mono shrink-0">Exit debit</label>
                    <input
                      type="text"
                      inputMode="decimal"
                      value={exitDebitStr}
                      onChange={e => setExitDebitStr(e.target.value)}
                      placeholder="e.g. 0.90"
                      className="flex-1 min-w-[100px] bg-zinc-900 border border-subtle rounded px-2 py-1.5 text-xs font-mono text-secondary focus:outline-none focus:border-amber-500/50"
                    />
                  </div>
                  {closeErr && (
                    <div className="text-[10px] font-mono text-danger break-words">{closeErr}</div>
                  )}
                  <div className="flex flex-wrap gap-2 pt-1">
                    <button
                      type="button"
                      disabled={closeLoading}
                      onClick={async () => {
                        const v = parseFloat(exitDebitStr.trim());
                        if (!Number.isFinite(v)) {
                          setCloseErr("Enter a valid exit debit.");
                          return;
                        }
                        setCloseLoading(true);
                        setCloseErr(null);
                        try {
                          const res = await axios.post<{ success: boolean; pnl: number }>(
                            `${API}/positions/${p.id}/close`,
                            { exit_debit: v, exit_reason: "MANUAL_CLOSE" },
                          );
                          setCloseConfirmOpen(false);
                          setCloseSuccessPnl(res.data.pnl);
                        } catch (err) {
                          setCloseErr(formatClosePositionError(err));
                        } finally {
                          setCloseLoading(false);
                        }
                      }}
                      className="px-3 py-1.5 text-[10px] font-mono rounded-lg border border-amber-500/50 text-amber-100 bg-amber-950/30 hover:bg-amber-950/45 disabled:opacity-50"
                    >
                      {closeLoading ? "Closing…" : "Submit close"}
                    </button>
                    <button
                      type="button"
                      disabled={closeLoading}
                      onClick={() => {
                        setCloseConfirmOpen(false);
                        setCloseErr(null);
                      }}
                      className="px-3 py-1.5 text-[10px] font-mono rounded-lg border border-subtle text-tertiary hover:bg-white/5"
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}
            </>
          )}

        </div>
      )}
    </div>
  );
}

// ── Exit Signals Panel ────────────────────────────────────────────────────────

function ExitSignalRow({ s, onAction }: {
  s: ExitSignal;
  onAction: (id: number, action: "acknowledge" | "snooze" | "dismiss") => void;
}) {
  const [open, setOpen]     = useState(false);
  const [acting, setActing] = useState(false);

  const reasonColor   = s.reason === "profit_target" ? "text-success" : s.reason === "stop_loss" ? "text-danger" : "text-warning";
  const profitPct     = s.pnl_pct ?? 0;
  const creditReceived = s.credit_received ?? 0;
  const debitToClose  = s.debit_to_close ?? 0;

  return (
    <div className={`w-full rounded-xl border overflow-hidden mb-3 ${
      s.reason === "stop_loss" ? "border-red-500/30 bg-red-500/5" : "border-amber-500/20 bg-zinc-900/50"
    }`}>
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-5 py-4 hover:bg-zinc-800/40 transition-colors"
      >
        <div className="flex items-center gap-4">
          {open ? <ChevronUp size={14} className="text-zinc-600" /> : <ChevronDown size={14} className="text-zinc-600" />}
          <span className="text-white font-bold text-base font-mono">{s.symbol}</span>
          <span className="text-zinc-500 text-sm font-mono">{s.expiry} · {s.dte}d</span>
          <span className={`text-xs font-mono font-bold px-2 py-0.5 rounded-sm bg-zinc-950 border border-subtle ${reasonColor}`}>
            {s.reason?.toUpperCase()}
          </span>
        </div>
        <div className="flex items-center gap-4 font-mono text-sm">
          <span className={profitPct >= 0 ? "text-success font-bold" : "text-danger font-bold"}>
            {profitPct >= 0 ? "+" : ""}{profitPct.toFixed(1)}%
          </span>
          <span className={s.pnl_dollars >= 0 ? "text-success" : "text-danger"}>
            ${s.pnl_dollars?.toFixed(0)}
          </span>
        </div>
      </button>

      {open && (
        <div className="border-t border-subtle bg-zinc-950">
          <div className="grid grid-cols-2 divide-x divide-subtle">
            <div className="p-4">
              <div className="text-[10px] font-mono text-tertiary tracking-widest mb-3">SIGNAL DETAILS</div>
              <div className="space-y-2 text-sm font-mono">
                {([
                  ["Signal type",    s.reason?.toUpperCase()],
                  ["Signal age",     `${s.age_minutes.toFixed(0)}m ago`],
                  ["Triggered at DTE", s.dte],
                ] as [string, any][]).map(([l, v]) => (
                  <div key={l} className="flex justify-between">
                    <span className="text-tertiary">{l}</span>
                    <span className="text-secondary">{v}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="p-4">
              <div className="text-[10px] font-mono text-tertiary tracking-widest mb-3">CLOSING ESTIMATE</div>
              <div className="space-y-2 text-sm font-mono">
                {([
                  ["Credit received", creditReceived > 0 ? `$${creditReceived.toFixed(2)}` : "—"],
                  ["Debit to close",  debitToClose  > 0 ? `$${debitToClose.toFixed(2)}`  : "—"],
                  ["Est. P/L",   { val: `${s.pnl_dollars >= 0 ? "+" : ""}$${s.pnl_dollars?.toFixed(2)}`, color: s.pnl_dollars >= 0 ? "text-success" : "text-danger" }],
                  ["% of credit", { val: `${profitPct >= 0 ? "+" : ""}${profitPct.toFixed(1)}%`,          color: profitPct >= 0 ? "text-success" : "text-danger" }],
                ] as [string, any][]).map(([l, v]) => (
                  <div key={l} className="flex justify-between">
                    <span className="text-tertiary">{l}</span>
                    {typeof v === "object"
                      ? <span className={v.color}>{v.val}</span>
                      : <span className="text-secondary">{v}</span>}
                  </div>
                ))}
              </div>
            </div>
          </div>
          <div className="px-4 py-3 bg-zinc-900/80 border-t border-subtle">
            <div className="text-[10px] font-mono text-tertiary tracking-widest mb-2">RECOMMENDED ACTION</div>
            <p className="text-secondary text-sm font-mono italic">
              {s.reason === "profit_target"
                ? `Position has captured ${profitPct.toFixed(0)}% of max profit. Close now to lock in gains and free up capital.`
                : s.reason === "time_exit"
                  ? `At ${s.dte} DTE, gamma risk is increasing. Standard time-exit protocol — close to avoid expiration week risk.`
                  : `Stop loss triggered. Close immediately to prevent further loss.`}
            </p>
          </div>
          <div className="grid grid-cols-3 gap-3 p-4 border-t border-subtle">
            <button
              onClick={() => { setActing(true); onAction(s.id, "acknowledge"); }}
              disabled={acting}
              className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-mono font-bold text-xs transition-all disabled:opacity-50"
            >
              <CheckCircle size={13} /> CLOSE NOW
            </button>
            <button
              onClick={() => { setActing(true); onAction(s.id, "snooze"); }}
              disabled={acting}
              className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg border border-amber-500/40 text-amber-400 hover:bg-amber-900/30 font-mono font-bold text-xs transition-all disabled:opacity-50"
            >
              <BellOff size={13} /> SNOOZE 24H
            </button>
            <button
              onClick={() => { setActing(true); onAction(s.id, "dismiss"); }}
              disabled={acting}
              className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg border border-zinc-700 text-zinc-400 hover:bg-zinc-800 font-mono font-bold text-xs transition-all disabled:opacity-50"
            >
              <Trash2 size={13} /> DISMISS
            </button>
          </div>
        </div>
      )}
</div>
  );
}

// ── Main Dashboard ────────────────────────────────────────────────────────────

export default function Dashboard() {
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [positions, setPositions]   = useState<Position[]>([]);
  const [signals, setSignals]       = useState<ExitSignal[]>([]);
  const [accounts, setAccounts]     = useState<Account[]>([]);
  const [liveNav, setLiveNav]       = useState<number | null>(null);
  const [health, setHealth]         = useState<Health | null>(null);
  const [lastPoll, setLastPoll]     = useState<Date | null>(null);
  const [polling, setPolling]       = useState(false);
  const [refreshRefreshing, setRefreshRefreshing] = useState(false);
  const [refreshResult, setRefreshResult] = useState<RefreshResult | null>(null);
  const [showHiddenRows, setShowHiddenRows] = useState(false);
  const [hiddenPositions, setHiddenPositions] = useState<Position[]>([]);
  const [tab, setTab]               = useState<"candidates" | "positions" | "exits" | "shadow" | "history">("candidates");
  const [shadowPayload, setShadowPayload]   = useState<ShadowResponse>({
    blocked: [], count: 0, hours: 48, cutoff: "", gate_kill_distribution: [],
  });
  const [pipelineStats, setPipelineStats]     = useState<PipelineStats | null>(null);
  const [historyPayload, setHistoryPayload]   = useState<HistoryResponse>({ history: [], count: 0, days: 30 });
  const [shadowHours, setShadowHours]         = useState(48);
  const [historyDays, setHistoryDays]         = useState(30);
  const [shadowSearch, setShadowSearch]       = useState("");
  const [shadowRulePill, setShadowRulePill]   = useState<string>("all");
  const [shadowSort, setShadowSort]           = useState<string>("created_desc");
  const [historySearch, setHistorySearch]     = useState("");
  const [historyDecisionPill, setHistoryDecisionPill] = useState<string>("all");
  const [historySort, setHistorySort]         = useState<string>("decided_desc");
  const [filterAccount,  setFilterAccount]  = useState<string>("all");
  const [filterStrategy, setFilterStrategy] = useState<string>("all");
  const [filterStatus,   setFilterStatus]   = useState<string>("all");
  const [sortBy,         setSortBy]         = useState<string>("default");

  const tokenUi = useMemo(() => {
    const rawToken = health?.checks?.token;
    let tokenColor = "text-zinc-400";
    let tokenDot = "bg-zinc-600";
    let tokenLabel = "--";
    let tokenObj: TokenData | null = null;
    if (rawToken) {
      if (typeof rawToken === "object" && rawToken !== null) {
        tokenObj = rawToken as TokenData;
        if (rawToken.days_remaining != null) {
          tokenLabel = `${rawToken.days_remaining.toFixed(1)}d left`;
          tokenDot = rawToken.days_remaining > 1 ? "bg-green-500" : "bg-red-500 animate-pulse";
          tokenColor = rawToken.days_remaining > 1 ? "text-green-500" : "text-red-500 font-bold";
        } else {
          tokenLabel = "ACTIVE";
          tokenDot = "bg-green-500";
          tokenColor = "text-green-500";
        }
      } else if (rawToken === "no_expiry_date" || rawToken === "present") {
        tokenLabel = "ACTIVE";
        tokenDot = "bg-green-500";
        tokenColor = "text-green-500";
      } else {
        tokenLabel = "ERROR";
        tokenDot = "bg-red-500";
        tokenColor = "text-red-500";
      }
    }
    return { tokenColor, tokenDot, tokenLabel, tokenObj };
  }, [health]);

  const shellAlertBorder = useMemo(() => {
    const tokenObj = tokenUi.tokenObj;
    const dataMin = health?.checks?.data_freshness?.last_snapshot_minutes_ago;
    const stale1h = dataMin != null && dataMin > 60;
    const tokenDead = tokenObj?.valid === false;
    const brokerErr = refreshResult?.feed_status === "upstream_error";
    const dbBad = health?.checks?.database != null && health.checks.database !== "ok";
    if (tokenDead || stale1h || brokerErr || dbBad) return "border-t-2 border-red-500/40";
    const tokenWarn =
      tokenObj != null &&
      tokenObj.valid !== false &&
      tokenObj.days_remaining != null &&
      tokenObj.days_remaining < 1;
    if (tokenWarn) return "border-t-2 border-amber-500/30";
    return "";
  }, [health, refreshResult, tokenUi.tokenObj]);

  // Data age: freshest timestamp from candidates or positions, then health fallback
  const dataAgeSeconds = useMemo(() => {
    if (health?.checks?.data_freshness?.last_snapshot_minutes_ago != null) {
      return Math.floor(health.checks.data_freshness.last_snapshot_minutes_ago * 60);
    }
    return null;
  }, [health]);

  const systemLevel: FreshnessLevel = useMemo(() => {
    if (dataAgeSeconds === null) return "unknown";
    return getFreshnessLevel(dataAgeSeconds / 60);
  }, [dataAgeSeconds]);

  // ── Utility rail derived data ─────────────────────────────────────────────

  const portfolioHealth = useMemo(() => {
    const premiumPositions = positions.filter(p => {
      const strategy = (p.strategy ?? "").toUpperCase();
      const credit   = p.fill_credit ?? p.entry_credit;
      return PREMIUM_STRATEGIES.has(strategy) && credit != null && p.qty != null;
    });
    const totalCredit = premiumPositions.length > 0
      ? premiumPositions.reduce((sum, p) =>
          sum + ((p.fill_credit ?? p.entry_credit ?? 0) * (p.qty ?? 1) * 100), 0)
      : null;
    const expiryClusters = positions.reduce((acc, p) => {
      if (p.expiry) acc[p.expiry] = (acc[p.expiry] ?? 0) + 1;
      return acc;
    }, {} as Record<string, number>);
    const sortedExpiries = Object.entries(expiryClusters).sort(([a], [b]) => a.localeCompare(b));
    const maxCount = sortedExpiries.length > 0 ? Math.max(...sortedExpiries.map(([, c]) => c)) : 0;
    const expiryConcentration = positions.length > 0 && maxCount / positions.length > 0.5;
    const uniqueSymbols = [...new Set(positions.map(p => p.symbol))].sort();
    let icMaxRiskSum = 0;
    let hasIcRisk = false;
    let netDeltaSum = 0;
    let hasNetDelta = false;
    for (const p of positions) {
      const m = computeIronCondorMaxRiskUsd(p);
      if (m != null) {
        icMaxRiskSum += m;
        hasIcRisk = true;
      }
      if (typeof p.net_delta === "number" && !Number.isNaN(p.net_delta)) {
        netDeltaSum += p.net_delta;
        hasNetDelta = true;
      }
    }
    return {
      totalCredit,
      sortedExpiries,
      expiryConcentration,
      uniqueSymbols,
      icMaxRiskSum: hasIcRisk ? icMaxRiskSum : null,
      netDeltaSum: hasNetDelta ? netDeltaSum : null,
    };
  }, [positions]);

  const railAlerts = useMemo(() => {
    const list: { severity: "red" | "amber" | "green" | "gray"; msg: string }[] = [];
    if (systemLevel === "stale")
      list.push({ severity: "amber", msg: "Data stale · refresh before acting" });
    if (health != null && health.checks?.circuit_breaker?.state !== "closed")
      list.push({ severity: "red", msg: "LLM circuit breaker tripped" });
    const tokenObj = tokenUi.tokenObj;
    if (tokenObj?.valid === false)
      list.push({ severity: "red", msg: "Schwab token expired — renew now" });
    if (
      tokenObj?.valid !== false &&
      tokenObj?.days_remaining != null &&
      tokenObj.days_remaining > 0 &&
      tokenObj.days_remaining < 1
    ) {
      const hrs = Math.max(1, Math.round(tokenObj.days_remaining * 24));
      list.push({
        severity: "amber",
        msg: `Schwab token expires in ~${hrs}h — refresh token to maintain broker connectivity.`,
      });
    }
    signals.slice(0, 3).forEach(s =>
      list.push({ severity: "green", msg: `Exit signal: ${s.symbol}` })
    );
    if (candidates.length === 0 && positions.length === 0)
      list.push({ severity: "gray", msg: "No active candidates or positions" });
    const order: Record<string, number> = { red: 0, amber: 1, green: 2, gray: 3 };
    list.sort((a, b) => order[a.severity] - order[b.severity]);
    return list.slice(0, 5);
  }, [systemLevel, health, signals, candidates, positions, tokenUi.tokenObj]);

  const alertCount = useMemo(() => {
    return railAlerts.filter(a => a.severity === "red" || a.severity === "amber").length;
  }, [railAlerts]);

  const reconciled = useMemo(() => {
    const r = (health as Health)?.checks?.reconciler_log;
    if (typeof r === "string" && r.trim()) return r;
    return null;
  }, [health]);

  const hiddenOnly = useMemo(() => {
    return hiddenPositions.filter(p =>
      (p.status === "imbalanced") || ((p.strategy ?? "").toUpperCase() === "UNKNOWN")
    );
  }, [hiddenPositions]);

  const uniqueAccounts   = useMemo(() => [...new Set(positions.map(p => p.account_id))].sort(), [positions]);
  const uniqueStrategies = useMemo(() => [...new Set(positions.map(p => (p.strategy ?? "IRON_CONDOR").toUpperCase()))].sort(), [positions]);
  const filteredPositions = useMemo(() => {
    let list = positions.slice();
    if (filterAccount  !== "all") list = list.filter(p => p.account_id === filterAccount);
    if (filterStrategy !== "all") list = list.filter(p => (p.strategy ?? "IRON_CONDOR").toUpperCase() === filterStrategy);
    if (filterStatus   !== "all") list = list.filter(p => getPositionStatus(p) === filterStatus);
    if      (sortBy === "dte_asc") list.sort((a, b) => (a.dte ?? 999) - (b.dte ?? 999));
    else if (sortBy === "pnl")     list.sort((a, b) => (b.unrealized_pnl ?? 0) - (a.unrealized_pnl ?? 0));
    else if (sortBy === "credit")  list.sort((a, b) => ((b.fill_credit ?? b.entry_credit ?? 0) - (a.fill_credit ?? a.entry_credit ?? 0)));
    return list;
  }, [positions, filterAccount, filterStrategy, filterStatus, sortBy]);

  const shadowRules = useMemo(() => {
    const set = new Set<string>();
    shadowPayload.blocked.forEach(r => set.add(blockedRuleRawKey(r.blocked_reason)));
    return [...set].sort();
  }, [shadowPayload.blocked]);

  const shadowGateKillBins = useMemo(
    () => mergeGateKillDist(shadowPayload.gate_kill_distribution, shadowPayload.blocked),
    [shadowPayload.gate_kill_distribution, shadowPayload.blocked],
  );
  const shadowHeatmapClientFallback = !shadowPayload.gate_kill_distribution?.length && shadowPayload.blocked.length > 0;

  const filteredSortedShadow = useMemo(() => {
    let list = shadowPayload.blocked.slice();
    const q = shadowSearch.trim().toLowerCase();
    if (q) list = list.filter(r => (r.symbol ?? "").toLowerCase().includes(q));
    if (shadowRulePill !== "all") {
      list = list.filter(r => blockedRuleRawKey(r.blocked_reason) === shadowRulePill);
    }
    const t = (a: ShadowBlocked, b: ShadowBlocked) => {
      const da = new Date(a.created_at).getTime();
      const db = new Date(b.created_at).getTime();
      if (shadowSort === "created_asc") return (Number.isNaN(da) ? 0 : da) - (Number.isNaN(db) ? 0 : db);
      if (shadowSort === "created_desc") return (Number.isNaN(db) ? 0 : db) - (Number.isNaN(da) ? 0 : da);
      if (shadowSort === "symbol") return (a.symbol ?? "").localeCompare(b.symbol ?? "");
      if (shadowSort === "score_desc") return (b.score ?? 0) - (a.score ?? 0);
      if (shadowSort === "score_asc") return (a.score ?? 0) - (b.score ?? 0);
      if (shadowSort === "rule") return blockedRuleRawKey(a.blocked_reason).localeCompare(blockedRuleRawKey(b.blocked_reason));
      return (Number.isNaN(db) ? 0 : db) - (Number.isNaN(da) ? 0 : da);
    };
    list.sort(t);
    return list;
  }, [shadowPayload.blocked, shadowSearch, shadowRulePill, shadowSort]);

  const filteredSortedHistory = useMemo(() => {
    let list = historyPayload.history.slice();
    const q = historySearch.trim().toLowerCase();
    if (q) list = list.filter(r => (r.symbol ?? "").toLowerCase().includes(q));
    if (historyDecisionPill !== "all") {
      list = list.filter(r => historyDecisionBucket(r.decision) === historyDecisionPill);
    }
    const t = (a: HistoryRow, b: HistoryRow) => {
      const da = new Date(a.decided_at).getTime();
      const db = new Date(b.decided_at).getTime();
      const ca = new Date(a.created_at).getTime();
      const cb = new Date(b.created_at).getTime();
      if (historySort === "decided_asc") return (Number.isNaN(da) ? 0 : da) - (Number.isNaN(db) ? 0 : db);
      if (historySort === "decided_desc") return (Number.isNaN(db) ? 0 : db) - (Number.isNaN(da) ? 0 : da);
      if (historySort === "symbol") return (a.symbol ?? "").localeCompare(b.symbol ?? "");
      if (historySort === "score_desc") return (b.score ?? 0) - (a.score ?? 0);
      if (historySort === "score_asc") return (a.score ?? 0) - (b.score ?? 0);
      if (historySort === "pnl_desc") return (b.pnl ?? -1e18) - (a.pnl ?? -1e18);
      if (historySort === "pnl_asc") return (a.pnl ?? 1e18) - (b.pnl ?? 1e18);
      if (historySort === "created_desc") return (Number.isNaN(cb) ? 0 : cb) - (Number.isNaN(ca) ? 0 : ca);
      return (Number.isNaN(db) ? 0 : db) - (Number.isNaN(da) ? 0 : da);
    };
    list.sort(t);
    return list;
  }, [historyPayload.history, historySearch, historyDecisionPill, historySort]);

  const exportShadowCsv = useCallback(() => {
    const cols = [
      { key: "id", header: "id" },
      { key: "symbol", header: "symbol" },
      { key: "score", header: "score" },
      { key: "net_credit", header: "net_credit" },
      { key: "expiry", header: "expiry" },
      { key: "rule", header: "gate_rule" },
      { key: "detail", header: "gate_detail" },
      { key: "created_at", header: "created_at" },
      { key: "snapshot_id", header: "snapshot_id" },
    ];
    const rows = filteredSortedShadow.map(r => ({
      id: r.id,
      symbol: r.symbol,
      score: r.score,
      net_credit: r.net_credit ?? "",
      expiry: r.expiry ?? "",
      rule: r.gate_rule_label ?? gateRuleHumanLabel(blockedRuleRawKey(r.blocked_reason)),
      detail: gateDetail(r.blocked_reason),
      created_at: r.created_at,
      snapshot_id: r.snapshot_id ?? "",
    }));
    downloadCsv(`shadow_filtered_${shadowHours}h.csv`, cols, rows);
  }, [filteredSortedShadow, shadowHours]);

  const exportHistoryCsv = useCallback(() => {
    const cols = [
      { key: "id", header: "id" },
      { key: "symbol", header: "symbol" },
      { key: "score", header: "score" },
      { key: "account_id", header: "account_id" },
      { key: "decision", header: "decision" },
      { key: "decided_at", header: "decided_at" },
      { key: "reason", header: "reason" },
      { key: "pnl", header: "pnl" },
      { key: "exit_reason", header: "exit_reason" },
      { key: "closed_at", header: "closed_at" },
      { key: "net_credit", header: "net_credit" },
      { key: "expiry", header: "expiry" },
      { key: "created_at", header: "created_at" },
    ];
    const rows = filteredSortedHistory.map(r => ({
      id: r.id,
      symbol: r.symbol,
      score: r.score,
      account_id: r.account_id ?? "",
      decision: r.decision,
      decided_at: r.decided_at,
      reason: r.reason ?? "",
      pnl: r.pnl ?? "",
      exit_reason: r.exit_reason ?? "",
      closed_at: r.closed_at ?? "",
      net_credit: r.net_credit != null ? String(r.net_credit) : "",
      expiry: r.expiry ?? "",
      created_at: r.created_at,
    }));
    downloadCsv(`history_filtered_${historyDays}d.csv`, cols, rows);
  }, [filteredSortedHistory, historyDays]);

  const fetchAll = useCallback(async () => {
    setPolling(true);
    try {
      const [cRes, pRes, sRes, hRes, aRes, shRes, hiRes, pipeRes] = await Promise.all([
        axios.get(`${API}/candidates`).catch(() => ({ data: { candidates: [] } })),
        axios.get(`${API}/positions`).catch(() => ({ data: { positions: [] } })),
        axios.get(`${API}/exit-signals`).catch(() => ({ data: { signals: [] } })),
        axios.get(`${API}/health`).catch(() => ({ data: null })),
        axios.get(`${API}/accounts`).catch(() => ({ data: { accounts: [] } })),
        axios.get(`${API}/shadow`, { params: { hours: shadowHours } }).catch(() => ({
          data: {
            blocked: [],
            count: 0,
            hours: shadowHours,
            cutoff: "",
            gate_kill_distribution: [],
          },
        })),
        axios.get(`${API}/history`, { params: { days: historyDays } }).catch(() => ({
          data: { history: [], count: 0, days: historyDays },
        })),
        axios.get(`${API}/pipeline-stats`, { params: { hours: shadowHours } }).catch(() => ({ data: null })),
      ]);
      const accountsData: Account[] = aRes.data.accounts || [];
      setCandidates(cRes.data.candidates || []);
      setPositions(pRes.data.positions || []);
      setSignals(sRes.data.signals || []);
      setHealth(hRes.data);
      setAccounts(accountsData);
      setShadowPayload({
        blocked: shRes.data.blocked ?? [],
        count: shRes.data.count ?? 0,
        hours: shRes.data.hours ?? shadowHours,
        cutoff: shRes.data.cutoff ?? "",
        gate_kill_distribution: shRes.data.gate_kill_distribution ?? [],
      });
      const pd = pipeRes.data;
      if (pd && typeof pd.scanned === "number") {
        setPipelineStats(pd as PipelineStats);
      } else {
        setPipelineStats(null);
      }
      setHistoryPayload({
        history: hiRes.data.history ?? [],
        count: hiRes.data.count ?? 0,
        days: hiRes.data.days ?? historyDays,
      });
      const derivedLive = accountsData
        .filter(a => a.type === "LIVE")
        .reduce((sum, a) => sum + (a.nav ?? 0), 0);
      setLiveNav(derivedLive > 0 ? derivedLive : null);
      setLastPoll(new Date());
    } catch (e) {
      console.error("Poll failed:", e);
    } finally {
      setPolling(false);
    }
  }, [shadowHours, historyDays]);

  const handleRefresh = useCallback(async () => {
    setRefreshRefreshing(true);
    setRefreshResult(null);
    try {
      const res = await axios.post<RefreshResult & { snapshot_updated?: boolean; pricing_refreshed?: boolean; snapshot_id?: number; snapshot_ts?: string }>(`${API}/refresh`);
      setRefreshResult({
        ok: res.data.ok,
        message: res.data.message,
        feed_status: res.data.feed_status,
        reason: res.data.reason,
        symbols_failed: res.data.symbols_failed,
      });
      await fetchAll();
    } catch (err: any) {
      const msg = err?.response?.data?.detail ?? err?.response?.data?.message ?? "Refresh failed.";
      setRefreshResult({ ok: false, message: msg });
    } finally {
      setRefreshRefreshing(false);
      setTimeout(() => setRefreshResult(null), 5000);
    }
  }, [fetchAll]);

  useEffect(() => {
    if (showHiddenRows) {
      axios.get(`${API}/positions`, { params: { include_hidden: true } })
        .then(r => setHiddenPositions(r.data.positions || []))
        .catch(() => setHiddenPositions([]));
    } else {
      setHiddenPositions([]);
    }
  }, [showHiddenRows, lastPoll]);

  useEffect(() => {
    fetchAll();
    const id = setInterval(fetchAll, 5000);
    return () => clearInterval(id);
  }, [fetchAll]);

  const handleApprove = async (id: number) => {
    try {
      await axios.post(`${API}/candidates/${id}/approve`, { idempotency_key: Date.now().toString() });
      fetchAll();
    } catch (err: any) {
      const msg =
        err?.response?.data?.detail ??
        err?.response?.data?.execution_error ??
        "Execution failed. Check backend logs.";
      alert(msg);
      console.error(err);
    }
  };

  const handleReject = async (id: number) => {
    try {
      await axios.post(`${API}/candidates/${id}/reject`, { reason: "Manual UI Rejection" });
      fetchAll();
    } catch (err: any) {
      const msg =
        err?.response?.data?.detail ??
        err?.response?.data?.message ??
        "Reject failed. Check backend logs.";
      alert(msg);
      console.error(err);
      throw err;
    }
  };

  const handlePositionClosed = useCallback((id: number) => {
    setPositions(prev => prev.filter(x => x.id !== id));
  }, []);

  const handleSignalAction = async (id: number, action: "acknowledge" | "snooze" | "dismiss") => {
    try {
      await axios.post(`${API}/exit-signals/${id}/${action}`);
      fetchAll();
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className={`min-h-screen bg-page text-primary ${shellAlertBorder}`.trim()}>
      <header className="sticky top-0 z-10 bg-card/95 backdrop-blur">
        <OperatorBar
          dataAgeSeconds={dataAgeSeconds}
          systemLevel={systemLevel}
          openCount={positions.length}
          pendingCount={candidates.length}
          alertCount={alertCount}
          reconciled={reconciled}
          polling={polling}
          refreshRefreshing={refreshRefreshing}
          refreshResult={refreshResult}
          onRefresh={handleRefresh}
        />
      </header>

      {health?.checks?.circuit_breaker?.state === "open" && (
        <div className="flex items-center gap-3 px-6 py-3 bg-red-900/30 border-b border-red-900/50 text-danger font-mono text-sm">
          <AlertTriangle size={15} />
          <span className="font-bold">CIRCUIT BREAKER OPEN</span>
          <span className="opacity-60">— LLM layer disabled. Check backend logs.</span>
        </div>
      )}

      <div className="flex flex-col xl:flex-row gap-6 p-6 items-start">
        <div className="flex-1 min-w-0">
          {accounts.length > 0 && <NavDashboard accounts={accounts} liveNav={liveNav} />}

          <div className="flex gap-1 mb-6 border-b border-subtle pb-px flex-wrap">
            {([
              ["candidates", "Pending",      candidates.length, null as string | null],
              ["positions",  "Positions",    positions.length, null],
              ["exits",      "Exit Signals", signals.length, null],
              ["shadow",     "Shadow",       shadowPayload.count, "ban"],
              ["history",    "History Log",  historyPayload.count, "history"],
            ] as const).map(([key, label, count, icon]) => {
              const isActive   = tab === key;
              const hasUrgent  = key === "exits" && signals.length > 0 && !isActive;
              return (
                <button
                  key={key}
                  onClick={() => setTab(key)}
                  className={`tab-btn px-4 py-2.5 text-[13px] font-mono font-medium transition-all border-b-2 flex items-center gap-2 ${
                    isActive
                      ? "text-accent border-accent bg-accent/5"
                      : hasUrgent
                        ? "text-warning border-transparent"
                        : "text-secondary border-transparent"
                  }`}
                >
                  {icon === "ban" && <Ban size={14} className={isActive ? "text-accent" : "text-tertiary"} />}
                  {icon === "history" && <History size={14} className={isActive ? "text-accent" : "text-tertiary"} />}
                  {label}
                  {count > 0 && (
                    <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-mono ${
                      isActive      ? "bg-accent/20 text-accent"
                      : hasUrgent  ? "bg-warning/15 text-warning"
                      : "bg-white/5 text-tertiary"
                    }`}>
                      {count}
                    </span>
                  )}
                </button>
              );
            })}
          </div>

          {tab === "candidates" && (
            candidates.length === 0 ? (
              <div className="text-center py-20 border border-dashed border-subtle rounded-xl bg-card/30">
                <TrendingUp size={32} className="mx-auto mb-3 text-tertiary" />
                <div className="text-secondary text-lg">No pending trade candidates</div>
                <div className="text-xs mt-2 text-tertiary font-mono">Polling every 5s · Awaiting strategy engine output</div>
              </div>
            ) : (
              <div className={`grid gap-6 ${candidates.length > 1 ? "grid-cols-1 xl:grid-cols-2" : "grid-cols-1"}`}>
                {candidates.map(c => (
                  <TradeCard key={c.id} candidate={c} freshnessLevel={systemLevel} onApprove={handleApprove} onReject={handleReject} />
                ))}
              </div>
            )
          )}

          {tab === "positions" && (
            positions.length === 0 ? (
              <div className="text-center py-20 border border-dashed border-subtle rounded-xl bg-card/30 font-mono text-sm text-secondary">
                No open positions found in database.
              </div>
            ) : (
              <>
                <div className="flex flex-wrap gap-2 mb-4 font-mono text-xs">
                  <select value={filterAccount} onChange={e => setFilterAccount(e.target.value)}
                    className="bg-card border border-subtle rounded px-2 py-1 text-secondary focus:outline-none focus:border-focus">
                    <option value="all">All accounts</option>
                    {uniqueAccounts.map(a => <option key={a} value={a}>{a === "PAPER" ? "PAPER" : `···${a.slice(-4)}`}</option>)}
                  </select>
                  <select value={filterStrategy} onChange={e => setFilterStrategy(e.target.value)}
                    className="bg-card border border-subtle rounded px-2 py-1 text-secondary focus:outline-none focus:border-focus">
                    <option value="all">All strategies</option>
                    {uniqueStrategies.map(s => <option key={s} value={s}>{strategyBadge[s] ?? s}</option>)}
                  </select>
                  <select value={filterStatus} onChange={e => setFilterStatus(e.target.value)}
                    className="bg-card border border-subtle rounded px-2 py-1 text-secondary focus:outline-none focus:border-focus">
                    <option value="all">All statuses</option>
                    <option value="on_track">On track</option>
                    <option value="watch">Watch</option>
                    <option value="at_risk">At risk</option>
                    <option value="neutral">Neutral</option>
                  </select>
                  <select value={sortBy} onChange={e => setSortBy(e.target.value)}
                    className="bg-card border border-subtle rounded px-2 py-1 text-secondary focus:outline-none focus:border-focus">
                    <option value="default">Sort: default</option>
                    <option value="dte_asc">Sort: DTE ↑</option>
                    <option value="pnl">Sort: P/L</option>
                    <option value="credit">Sort: credit</option>
                  </select>
                  {filteredPositions.length !== positions.length && (
                    <span className="text-tertiary self-center">{filteredPositions.length} of {positions.length}</span>
                  )}
                  <button
                    onClick={() => setShowHiddenRows(h => !h)}
                    className={`ml-auto text-[10px] font-mono px-2 py-1 rounded border transition-colors ${
                      showHiddenRows ? "border-warning text-warning bg-warning/10" : "border-subtle text-tertiary hover:text-secondary"
                    }`}
                  >
                    {showHiddenRows ? "Hide debug" : "Debug / Hidden Rows"}
                  </button>
                </div>
                <div className="space-y-0 w-full">
                  {filteredPositions.map(p => (
                    <PositionRow key={p.id} p={p} onPositionClosed={handlePositionClosed} />
                  ))}
                </div>
                {showHiddenRows && hiddenOnly.length > 0 && (
                  <div className="mt-6 pt-6 border-t border-subtle">
                    <div className="text-[10px] font-mono text-tertiary tracking-widest mb-3">DEBUG — HIDDEN ROWS (imbalanced / UNKNOWN)</div>
                    <div className="space-y-0 w-full opacity-80">
                      {hiddenOnly.map(p => (
                        <div key={p.id} className="mb-3 p-3 rounded border border-amber-500/20 bg-amber-950/10 font-mono text-xs">
                          <div className="flex justify-between">
                            <span>{p.symbol} · {(p.strategy ?? "—").toUpperCase()}</span>
                            <span className="text-tertiary">status={p.status ?? "—"}</span>
                          </div>
                          <div className="text-tertiary mt-1">key={p.position_key}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
                {showHiddenRows && hiddenOnly.length === 0 && (
                  <div className="mt-4 text-[10px] font-mono text-tertiary">No hidden rows (imbalanced/UNKNOWN).</div>
                )}
              </>
            )
          )}

          {tab === "exits" && (
            signals.length === 0 ? (
              <div className="text-center py-20 border border-dashed border-subtle rounded-xl bg-card/30 font-mono text-sm text-secondary">
                No pending exit signals triggered.
              </div>
            ) : (
              <div className="space-y-4 w-full">{signals.map(s => <ExitSignalRow key={s.id} s={s} onAction={handleSignalAction} />)}</div>
            )
          )}

          {tab === "shadow" && (
            <div className="space-y-4 w-full">
              <PipelineFunnelBar
                stats={pipelineStats}
                windowHours={shadowHours}
                onWindowHours={setShadowHours}
                cutoffIso={shadowPayload.cutoff || undefined}
              />
              <GateKillHeatmap bins={shadowGateKillBins} usedClientFallback={shadowHeatmapClientFallback} />
              <div className="flex flex-wrap gap-2 items-center font-mono text-xs">
                <input
                  type="search"
                  placeholder="Symbol contains…"
                  value={shadowSearch}
                  onChange={e => setShadowSearch(e.target.value)}
                  className="bg-card border border-subtle rounded px-2 py-1 text-secondary placeholder:text-tertiary focus:outline-none focus:border-focus min-w-[140px]"
                />
                <span className="text-tertiary">Rule:</span>
                <button
                  type="button"
                  onClick={() => setShadowRulePill("all")}
                  className={`px-2 py-1 rounded-full text-[10px] border ${
                    shadowRulePill === "all" ? "border-accent text-accent bg-accent/10" : "border-subtle text-tertiary"
                  }`}
                >
                  All
                </button>
                {shadowRules.map(ruleKey => (
                  <button
                    key={ruleKey}
                    type="button"
                    onClick={() => setShadowRulePill(ruleKey)}
                    className={`px-2 py-1 rounded-full text-[10px] border max-w-[160px] truncate ${
                      shadowRulePill === ruleKey ? "border-warning text-warning bg-warning/10" : "border-subtle text-tertiary"
                    }`}
                    title={ruleKey}
                  >
                    {gateRuleHumanLabel(ruleKey)}
                  </button>
                ))}
                <select
                  value={shadowSort}
                  onChange={e => setShadowSort(e.target.value)}
                  className="bg-card border border-subtle rounded px-2 py-1 text-secondary focus:outline-none focus:border-focus ml-auto"
                >
                  <option value="created_desc">Sort: newest</option>
                  <option value="created_asc">Sort: oldest</option>
                  <option value="symbol">Sort: symbol</option>
                  <option value="score_desc">Sort: score ↓</option>
                  <option value="score_asc">Sort: score ↑</option>
                  <option value="rule">Sort: rule</option>
                </select>
                <button
                  type="button"
                  onClick={exportShadowCsv}
                  className="flex items-center gap-1 px-2 py-1 rounded border border-subtle text-tertiary hover:text-secondary hover:border-focus"
                  title="Export filtered rows to CSV"
                >
                  <Download size={14} />
                  CSV
                </button>
              </div>
              {filteredSortedShadow.length !== shadowPayload.blocked.length && (
                <div className="text-[10px] text-tertiary font-mono">
                  Showing {filteredSortedShadow.length} of {shadowPayload.blocked.length} (client filter)
                </div>
              )}
              {shadowPayload.blocked.length === 0 ? (
                <div className="text-center py-20 border border-dashed border-subtle rounded-xl bg-card/30 font-mono text-sm text-secondary">
                  No blocked candidates in the selected window.
                </div>
              ) : (
                <div className="space-y-0 w-full">
                  {filteredSortedShadow.map(r => (
                    <ShadowBlockedRow key={r.id} row={r} />
                  ))}
                </div>
              )}
              {shadowPayload.blocked.length > 0 && shadowPayload.blocked.length <= 3 && (
                <p className="text-[10px] text-tertiary font-mono px-1 pt-1 border-t border-subtle/40">
                  {shadowPayload.blocked.length} blocked candidate(s) in this window.
                  {dataAgeSeconds != null && (
                    <span> Last data snapshot: {formatDataAge(dataAgeSeconds)}.</span>
                  )}
                </p>
              )}
            </div>
          )}

          {tab === "history" && (
            <div className="space-y-4 w-full">
              <div className="flex flex-wrap items-center gap-2 text-xs font-mono">
                <span className="text-tertiary w-full sm:w-auto">Window (API <code className="text-tertiary">days</code>):</span>
                {([7, 30, 90] as const).map(d => (
                  <button
                    key={d}
                    type="button"
                    onClick={() => setHistoryDays(d)}
                    className={`px-2.5 py-1 rounded border text-[10px] transition-colors ${
                      historyDays === d
                        ? "border-accent text-accent bg-accent/10"
                        : "border-subtle text-secondary hover:border-focus"
                    }`}
                  >
                    {d}d
                  </button>
                ))}
                <span className="text-tertiary text-[10px] ml-auto">max 90 · API returns ≤200 rows</span>
              </div>
              <div className="flex flex-wrap gap-2 items-center font-mono text-xs">
                <input
                  type="search"
                  placeholder="Symbol contains…"
                  value={historySearch}
                  onChange={e => setHistorySearch(e.target.value)}
                  className="bg-card border border-subtle rounded px-2 py-1 text-secondary placeholder:text-tertiary focus:outline-none focus:border-focus min-w-[140px]"
                />
                <span className="text-tertiary">Decision:</span>
                {(["all", "approved", "rejected", "other"] as const).map(pill => (
                  <button
                    key={pill}
                    type="button"
                    onClick={() => setHistoryDecisionPill(pill)}
                    className={`px-2 py-1 rounded-full text-[10px] border capitalize ${
                      historyDecisionPill === pill
                        ? pill === "approved"
                          ? "border-success text-success bg-positive/10"
                          : pill === "rejected"
                            ? "border-danger text-danger bg-negative/10"
                            : "border-accent text-accent bg-accent/10"
                        : "border-subtle text-tertiary"
                    }`}
                  >
                    {pill}
                  </button>
                ))}
                <select
                  value={historySort}
                  onChange={e => setHistorySort(e.target.value)}
                  className="bg-card border border-subtle rounded px-2 py-1 text-secondary focus:outline-none focus:border-focus ml-auto"
                >
                  <option value="decided_desc">Sort: decided ↓</option>
                  <option value="decided_asc">Sort: decided ↑</option>
                  <option value="created_desc">Sort: created ↓</option>
                  <option value="symbol">Sort: symbol</option>
                  <option value="score_desc">Sort: score ↓</option>
                  <option value="score_asc">Sort: score ↑</option>
                  <option value="pnl_desc">Sort: P/L ↓</option>
                  <option value="pnl_asc">Sort: P/L ↑</option>
                </select>
                <button
                  type="button"
                  onClick={exportHistoryCsv}
                  className="flex items-center gap-1 px-2 py-1 rounded border border-subtle text-tertiary hover:text-secondary hover:border-focus"
                  title="Export filtered rows to CSV"
                >
                  <Download size={14} />
                  CSV
                </button>
              </div>
              {filteredSortedHistory.length !== historyPayload.history.length && (
                <div className="text-[10px] text-tertiary font-mono">
                  Showing {filteredSortedHistory.length} of {historyPayload.history.length} (client filter)
                </div>
              )}
              {historyPayload.history.length === 0 ? (
                <div className="text-center py-20 border border-dashed border-subtle rounded-xl bg-card/30 font-mono text-sm text-secondary">
                  No trade decisions in the selected window.
                </div>
              ) : (
                <div className="space-y-0 w-full">
                  {filteredSortedHistory.map(r => (
                    <HistoryLogRow key={`${r.id}-${r.decided_at}`} row={r} />
                  ))}
                </div>
              )}
            </div>
          )}
        </div>

        <div className="w-full xl:w-80 xl:shrink-0 space-y-4">
          <div className="card-surface p-4 font-mono">
            <div className="text-[10px] text-tertiary tracking-widest mb-3">🛡 PORTFOLIO HEALTH</div>
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div className="flex flex-col gap-0.5 rounded border border-subtle/60 bg-white/[0.02] px-2 py-2">
                  <span className="text-[10px] text-tertiary">Open positions</span>
                  <span className="font-numeric text-secondary">{positions.length > 0 ? String(positions.length) : "—"}</span>
                </div>
                <div className="flex flex-col gap-0.5 rounded border border-subtle/60 bg-white/[0.02] px-2 py-2">
                  <span className="text-[10px] text-tertiary">Total credit</span>
                  <span className="font-numeric text-secondary">
                    {portfolioHealth.totalCredit != null ? `$${portfolioHealth.totalCredit.toFixed(2)}` : "—"}
                  </span>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div className="flex flex-col gap-0.5 rounded border border-subtle/60 bg-white/[0.02] px-2 py-2">
                  <span className="text-[10px] text-tertiary">Net delta</span>
                  <span
                    className={`font-numeric ${
                      portfolioHealth.netDeltaSum != null
                        ? directionalDeltaTone(Math.abs(portfolioHealth.netDeltaSum))
                        : "text-secondary"
                    }`}
                  >
                    {portfolioHealth.netDeltaSum != null ? portfolioHealth.netDeltaSum.toFixed(2) : "—"}
                  </span>
                </div>
                <div className="flex flex-col gap-0.5 rounded border border-subtle/60 bg-white/[0.02] px-2 py-2">
                  <span className="text-[10px] text-tertiary">Theta / day</span>
                  <span className="font-numeric text-secondary">—</span>
                </div>
                <div className="flex flex-col gap-0.5 rounded border border-subtle/60 bg-white/[0.02] px-2 py-2">
                  <span className="text-[10px] text-tertiary">Vega</span>
                  <span className="font-numeric text-secondary">—</span>
                </div>
                <div className="flex flex-col gap-0.5 rounded border border-subtle/60 bg-white/[0.02] px-2 py-2">
                  <span className="text-[10px] text-tertiary">IC max risk</span>
                  <span className="font-numeric text-secondary">
                    {portfolioHealth.icMaxRiskSum != null ? `$${portfolioHealth.icMaxRiskSum.toFixed(2)}` : "—"}
                  </span>
                </div>
              </div>
              {portfolioHealth.sortedExpiries.length > 0 && (
                <div className="pt-2 border-t border-subtle">
                  <div className="text-[10px] text-tertiary mb-1.5">EXPIRY CLUSTERS</div>
                  <div className="text-xs text-secondary leading-relaxed">
                    {portfolioHealth.sortedExpiries.map(([exp, count], i) => (
                      <span key={exp}>{i > 0 ? " · " : ""}{formatExpiry(exp)}: {count}</span>
                    ))}
                  </div>
                  {portfolioHealth.expiryConcentration && (
                    <div className="text-[10px] text-warning mt-1">⚠ Expiry concentration</div>
                  )}
                </div>
              )}
              {portfolioHealth.uniqueSymbols.length > 0 && (
                <div className="pt-2 border-t border-subtle">
                  <div className="text-[10px] text-tertiary mb-1.5">SYMBOLS</div>
                  <div className="flex flex-wrap gap-1">
                    {portfolioHealth.uniqueSymbols.map(sym => (
                      <span key={sym} className="text-[10px] px-1.5 py-0.5 rounded bg-white/5 text-tertiary border border-subtle">{sym}</span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="card-surface p-4 font-mono">
            <div className="text-[10px] text-tertiary tracking-widest mb-3">⚡ SYSTEM STATUS</div>
            <div className="space-y-2 mb-4">
              <div className="flex justify-between items-center text-xs">
                <span className="text-tertiary">LLM</span>
                <span className={`flex items-center gap-1 ${health?.checks?.circuit_breaker?.state === "closed" ? "text-success" : "text-danger"}`}>
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-current" />
                  {health?.checks?.circuit_breaker?.state === "closed" ? "Online" : "Tripped"}
                </span>
              </div>
              <div className="flex justify-between items-center text-xs">
                <span className="text-tertiary">Data feed</span>
                <span className={`flex items-center gap-1 ${
                  refreshResult?.feed_status === "market_closed" ? "text-secondary" :
                  refreshResult?.feed_status === "upstream_error" ? "text-danger" :
                  systemLevel === "live" ? "text-success" :
                  systemLevel === "delayed" ? "text-warning" : "text-danger"
                }`}>
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-current" />
                  {refreshResult?.feed_status === "market_closed" ? "CLOSED" :
                   refreshResult?.feed_status === "upstream_error" ? "ERROR" :
                   systemLevel.toUpperCase()}
                  <span className="text-tertiary font-normal">
                    {" "}· {formatDataAge(dataAgeSeconds)}
                  </span>
                </span>
              </div>
              {refreshResult && refreshResult.feed_status !== "fresh" && (
                <div className={`text-[10px] font-mono ${
                  refreshResult.feed_status === "market_closed" ? "text-secondary" :
                  refreshResult.feed_status === "upstream_error" ? "text-danger" : "text-warning"
                }`}>
                  {refreshResult.message}
                  {refreshResult.symbols_failed?.length ? ` (${refreshResult.symbols_failed.join(", ")} failed)` : ""}
                </div>
              )}
              <div className="flex justify-between items-center text-xs py-1">
                <span className="text-tertiary uppercase tracking-tight">Schwab Token</span>
                <span className={`${tokenUi.tokenColor} flex items-center gap-1.5`}>
                  <span className={`inline-block w-1.5 h-1.5 rounded-full ${tokenUi.tokenDot}`} />
                  {tokenUi.tokenLabel}
                </span>
              </div>
              <div className="flex justify-between items-center text-xs py-1">
                <span className="text-tertiary uppercase tracking-tight">Agent Loop</span>
                <span className="text-success flex items-center gap-1.5">
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-green-500" />
                  Running
                </span>
              </div>
              {([
                ["Last reconcile", reconciled ?? "Not Initialized"],
                ["Next reconcile", "Auto-scheduled"],
              ] as [string, string][]).map(([l, v]) => (
                <div key={l} className="flex justify-between text-xs">
                  <span className="text-tertiary">{l}</span>
                  <span className="text-tertiary italic">{v}</span>
                </div>
              ))}
            </div>
            <div className="space-y-2 border-t border-subtle pt-3">
              {refreshResult && (
                <div className={`text-xs font-mono ${
                  refreshResult.feed_status === "fresh" ? "text-positive" :
                  refreshResult.ok ? "text-warning" : "text-danger"
                }`}>
                  {refreshResult.message}
                </div>
              )}
              <button
                onClick={handleRefresh}
                disabled={refreshRefreshing}
                className="w-full py-2 text-xs font-mono rounded-lg border border-subtle text-secondary hover:text-primary hover:border-focus transition-colors flex items-center justify-center gap-1.5 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {refreshRefreshing ? "Refreshing…" : "↻ Refresh Prices"}
              </button>
            </div>
          </div>

          <div className="card-surface p-4 font-mono">
            <div className="text-[10px] text-tertiary tracking-widest mb-3">⚡ ALERTS</div>
            {railAlerts.length === 0 ? (
              <div className="text-xs text-tertiary">✓ All systems nominal</div>
            ) : (
              <div className="space-y-2">
                {railAlerts.map((a, i) => (
                  <div key={i} className={`flex items-start gap-2 text-xs ${
                    a.severity === "red"   ? "text-danger"  :
                    a.severity === "amber" ? "text-warning" :
                    a.severity === "green" ? "text-success" : "text-tertiary"
                  }`}>
                    <span className="mt-px shrink-0">{a.severity === "gray" ? "○" : "●"}</span>
                    <span className="flex-1">{a.msg}</span>
                    <span className="text-tertiary shrink-0">--</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}