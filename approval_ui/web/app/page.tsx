"use client";

import { useEffect, useState, useCallback, useMemo } from "react";
import axios from "axios";
import {
  AlertTriangle, CheckCircle, XCircle, Clock, TrendingUp,
  RefreshCw, ChevronDown, ChevronUp,
  Activity, BellOff, Trash2
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
}

interface Candidate {
  id: number; symbol: string; score: number; account_id: string;
  created_at: string; age_minutes: number; is_stale: boolean;
  candidate_json: CandidateJson; llm_card: LLMCard;
}

interface Position {
  id: number; account_id: string; symbol: string; strategy: string;
  expiry: string; dte: number; fill_credit?: number; entry_credit?: number;
  unrealized_pnl: number; profit_pct: number | null;
  net_delta: number; max_risk: number; opened_at: string;
  legs: any; meta: any; position_key: string;
  qty?: number;
  mark?: number | null;
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

interface TokenData { valid: boolean; days_remaining: number }
interface Health {
  circuit_breaker: { state: string; failures: number; attempts: number };
  data_freshness: { last_snapshot_minutes_ago: number | null; is_stale: boolean };
  // Backend returns object when expiry is known; string sentinel ("present"|"missing"|"error:…") otherwise
  token?: TokenData | string | null;
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

// Extract first sentence — skips decimal points (e.g. "1.06"), capped at maxLen
function firstSentence(text: string | undefined, maxLen = 140): string {
  if (!text) return "";
  // Find the first period that is NOT between two digits (skips "1.06", "0.50", etc.)
  const sentenceEnd = text.search(/(?<!\d)\.(?!\d)/);
  if (sentenceEnd > 0 && sentenceEnd <= maxLen) return text.slice(0, sentenceEnd + 1);
  // Fallback: any period after position 10 (avoids "Mr." etc. at start)
  const laterPeriod = text.indexOf(".", 10);
  if (laterPeriod > 0 && laterPeriod <= maxLen) return text.slice(0, laterPeriod + 1);
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

function OperatorBar({ dataAgeSeconds, systemLevel, openCount, pendingCount, polling, onRefresh }: {
  dataAgeSeconds: number | null;
  systemLevel: FreshnessLevel;
  openCount: number;
  pendingCount: number;
  polling: boolean;
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
        <span className="text-tertiary px-4">0 Alerts</span>
        <span className="text-tertiary px-4">Reconciled: --</span>
        <button
          onClick={onRefresh}
          className="flex items-center gap-1 pl-4 text-secondary hover:text-primary transition-colors"
        >
          <RefreshCw size={10} className={polling ? "animate-spin" : ""} />
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
          <div className={dailyPnl != null ? (dailyPnl >= 0 ? "text-success font-bold" : "text-danger font-bold") : "text-tertiary"}>
            {dailyPnl != null ? `${dailyPnl >= 0 ? "+" : ""}$${dailyPnl.toFixed(2)}` : "—"}
          </div>
        </div>
        <div>
          <div className="text-[10px] text-tertiary mb-0.5">Unrealized P/L</div>
          <div className={unrealizedPnl == null ? "text-tertiary" : unrealizedPnl >= 0 ? "text-success font-bold" : "text-danger font-bold"}>
            {unrealizedPnl == null ? "—" : `${unrealizedPnl >= 0 ? "+" : ""}$${Math.abs(unrealizedPnl).toFixed(2)}`}
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
      <span className={`text-sm font-mono font-bold tabular-nums w-12 text-right ${scoreColor(score)}`}>
        {score?.toFixed(1)}
      </span>
    </div>
  );
}

// 5-dot score indicator: ●●●○○
function ScoreDots({ score }: { score: number }) {
  const filled = Math.min(5, Math.round(score / 20));
  const color  = score >= 70 ? "text-emerald-400" : score >= 50 ? "text-amber-400" : "text-red-400";
  return (
    <span className={`text-[10px] font-mono tracking-tighter ${color}`} title={`Score: ${score}`}>
      {"●".repeat(filled)}{"○".repeat(5 - filled)}
    </span>
  );
}

// ── Trade Card (Changes 3 + 6) ────────────────────────────────────────────────

function TradeCard({ candidate, freshnessLevel, onApprove, onReject }: {
  candidate: Candidate;
  freshnessLevel: FreshnessLevel;
  onApprove: (id: number) => void;
  onReject: (id: number) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const [acting, setActing]     = useState(false);

  const c    = typeof candidate.candidate_json === "string" ? JSON.parse(candidate.candidate_json) : candidate.candidate_json;
  const card = typeof candidate.llm_card        === "string" ? JSON.parse(candidate.llm_card)        : candidate.llm_card;

  const rec    = card?.recommendation ?? "no";
  const ivRank = c?.iv_rank;

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (acting) return;
      // 'A' only fires when data is live or delayed — not stale
      if (e.key.toLowerCase() === "a" && freshnessLevel !== "stale" && !candidate.is_stale) {
        setActing(true); onApprove(candidate.id);
      }
      if (e.key.toLowerCase() === "r") { setActing(true); onReject(candidate.id); }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [candidate.id, candidate.is_stale, freshnessLevel, acting, onApprove, onReject]);

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
            <span className="text-[10px] font-mono px-1.5 py-0.5 rounded-full flex items-center gap-1 text-warning bg-warning/10 border border-warning/20">
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
          <span className={`text-3xl font-black font-mono tabular-nums ${scoreColor(candidate.score)}`}>
            {candidate.score?.toFixed(0)}
          </span>
          <ScoreDots score={candidate.score} />
        </div>
      </div>

      {/* Row 2: Expiry · DTE · price · IV rank */}
      <div className="px-3 py-1.5 text-xs font-mono text-zinc-400 border-b border-zinc-800/60">
        {c?.expiry} · {c?.dte} DTE · ${c?.underlying_price?.toFixed(2)}
        {ivRank != null && <span className="ml-2 text-purple-400 font-bold">IVR {ivRank.toFixed(0)}</span>}
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

      {/* Row 5: Why now — first sentence of LLM thesis */}
      {card?.summary && (
        <p className="text-zinc-400 text-xs px-3 py-1.5 border-b border-zinc-800/60">
          {firstSentence(card.summary)}
        </p>
      )}

      {/* Context panel (market events — keeps own fetch) */}
      {c?.symbol && <ContextPanel symbol={c.symbol} />}

      {/* Row 6: Action buttons — 3 states based on freshnessLevel */}
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
            onClick={() => { setActing(true); onApprove(candidate.id); }}
            disabled={acting}
            title="Data is delayed — verify before approving"
            className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg border border-amber-500/60 text-amber-400 hover:bg-amber-900/30 font-mono font-bold text-sm transition-all disabled:opacity-50"
          >
            <CheckCircle size={13} /> Approve (Delayed)
          </button>
        ) : (
          <button
            onClick={() => { setActing(true); onApprove(candidate.id); }}
            disabled={acting}
            className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-mono font-bold text-sm transition-all disabled:opacity-50"
          >
            <CheckCircle size={13} /> APPROVE (A)
          </button>
        )}
        <button
          onClick={() => { setActing(true); onReject(candidate.id); }}
          disabled={acting}
          className="flex items-center justify-center gap-1.5 py-2.5 rounded-lg border border-red-500/40 text-red-400 hover:bg-red-900/50 font-mono font-bold text-sm transition-all disabled:opacity-50"
        >
          <XCircle size={13} /> REJECT (R)
        </button>
      </div>

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

function PositionRow({ p }: { p: Position }) {
  const [open, setOpen] = useState(false);
  const credit  = p.fill_credit ?? p.entry_credit;
  const pnl     = p.unrealized_pnl ?? 0;
  const pct     = p.profit_pct;
  const target50 = credit != null ? credit * 0.5 : null;
  const stop200  = credit != null ? credit * 2.0 : null;

  const dte = typeof p.dte === "number" && !Number.isNaN(p.dte)
    ? p.dte
    : (p.expiry ? Math.ceil((new Date(p.expiry).getTime() - Date.now()) / 86400000) : NaN);
  const isEquity     = (p.strategy ?? "").toUpperCase() === "EQUITY";
  const dteDisplay   = isEquity ? "—" : (Number.isNaN(dte) || dte === undefined ? "--d" : `${dte}d`);
  const accountBadge = p.account_id === "PAPER" ? "PAPER" : `···${p.account_id?.slice(-4) ?? ""}`;
  const mark         = p.mark;

  // Correction 4: new chip logic
  const chipKey = getChip(mark, credit, Number.isNaN(dte) ? null : dte);

  return (
    // Change 6: card-surface (bg + border + shadow + gradient)
    <div className="card-surface mb-3 overflow-hidden w-full">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-3 px-5 py-3.5 hover:bg-white/[0.02] transition-colors text-left"
      >
        {open ? <ChevronUp size={13} className="text-tertiary shrink-0" /> : <ChevronDown size={13} className="text-tertiary shrink-0" />}

        {/* Left group — symbol + IC tag + DTE + qty + credit */}
        <div className="flex items-center gap-3 flex-1 min-w-0 font-mono">
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
          {/* Two-column: ENTRY | CURRENT */}
          <div className="grid grid-cols-2 divide-x divide-subtle">
            <div className="p-4">
              <div className="text-[10px] font-mono text-tertiary tracking-widest mb-3">ENTRY</div>
              <div className="space-y-2 text-sm font-mono">
                {([
                  [isEquity ? "Avg price"  : "Credit received", credit != null ? `$${credit.toFixed(2)}` : "—"],
                  [isEquity ? "Shares"     : "Contracts",       p.qty != null ? String(p.qty) : "—"],
                  ...(!isEquity ? [["Total credit gained", credit != null && p.qty != null ? `$${(credit * p.qty * 100).toFixed(2)}` : "—"]] as [string, string][] : []),
                  ...(!isEquity ? [["Max risk (margin)",  p.max_risk != null ? `$${p.max_risk?.toFixed(2)}` : "—"]] as [string, string][] : []),
                  ...(!isEquity ? [["Net delta",          p.net_delta?.toFixed(4) ?? "—"]] as [string, string][] : []),
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

              {/* Greeks placeholder */}
              <div className="px-4 py-3 border-t border-subtle">
                <div className="text-[10px] font-mono text-tertiary tracking-widest mb-2">GREEKS</div>
                <div className="grid grid-cols-4 gap-2 text-xs font-mono text-center">
                  {(["Delta", "Gamma", "Theta", "Vega"] as const).map(g => (
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

              {/* Action buttons */}
              <div className="grid grid-cols-3 gap-2 px-4 pb-4 border-t border-subtle pt-3">
                {(["Close Position", "Roll Position", "Add Note"] as const).map(label => (
                  <button key={label} disabled title="Coming soon"
                    className="py-2 text-[10px] font-mono rounded-lg border border-subtle text-tertiary opacity-30 cursor-not-allowed">
                    {label}
                  </button>
                ))}
              </div>
            </>
          )}

          {/* Legs */}
          {p.legs && Object.keys(p.legs).length > 0 && (
            <div className="px-4 pb-3 border-t border-subtle">
              <div className="text-[10px] font-mono text-tertiary tracking-widest mt-3 mb-2">LEGS</div>
              <div className="text-xs font-mono text-secondary bg-card rounded p-2 overflow-x-auto">
                {JSON.stringify(p.legs, null, 2)}
              </div>
            </div>
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
  const [tab, setTab]               = useState<"candidates" | "positions" | "exits">("candidates");
  const [filterAccount,  setFilterAccount]  = useState<string>("all");
  const [filterStrategy, setFilterStrategy] = useState<string>("all");
  const [filterStatus,   setFilterStatus]   = useState<string>("all");
  const [sortBy,         setSortBy]         = useState<string>("default");

  // Data age: freshest timestamp from candidates or positions, then health fallback
  const dataAgeSeconds = useMemo(() => {
    const timestamps: number[] = [];
    candidates.forEach(c => { if (c.created_at) timestamps.push(new Date(c.created_at).getTime()); });
    positions.forEach(p  => { if (p.opened_at)  timestamps.push(new Date(p.opened_at).getTime());  });
    if (timestamps.length > 0) {
      return Math.floor((Date.now() - Math.max(...timestamps)) / 1000);
    }
    if (health?.data_freshness?.last_snapshot_minutes_ago != null) {
      return health.data_freshness.last_snapshot_minutes_ago * 60;
    }
    return null;
  }, [candidates, positions, health]);

  const systemLevel: FreshnessLevel = useMemo(() => {
    if (dataAgeSeconds === null) return "unknown";
    return getFreshnessLevel(dataAgeSeconds / 60);
  }, [dataAgeSeconds]);

  // ── Utility rail derived data ─────────────────────────────────────────────

  const portfolioHealth = useMemo(() => {
    // Only premium-selling strategies with BOTH credit and qty populated.
    // If either is missing the position is skipped — "$0.00" is worse than "—".
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
    return { totalCredit, sortedExpiries, expiryConcentration, uniqueSymbols };
  }, [positions]);

  const railAlerts = useMemo(() => {
    const list: { severity: "red" | "amber" | "green" | "gray"; msg: string }[] = [];
    if (systemLevel === "stale")
      list.push({ severity: "amber", msg: "Data stale · refresh before acting" });
    if (health != null && health.circuit_breaker?.state !== "closed")
      list.push({ severity: "red", msg: "LLM circuit breaker tripped" });
    const tokenObj = typeof health?.token === "object" && health.token !== null
      ? (health.token as TokenData) : null;
    if (tokenObj?.valid === false)
      list.push({ severity: "red", msg: "Schwab token expired — renew now" });
    signals.slice(0, 3).forEach(s =>
      list.push({ severity: "green", msg: `Exit signal: ${s.symbol}` })
    );
    if (candidates.length === 0 && positions.length === 0)
      list.push({ severity: "gray", msg: "No active candidates or positions" });
    const order: Record<string, number> = { red: 0, amber: 1, green: 2, gray: 3 };
    list.sort((a, b) => order[a.severity] - order[b.severity]);
    return list.slice(0, 5);
  }, [systemLevel, health, signals, candidates, positions]);

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

  const fetchAll = useCallback(async () => {
    setPolling(true);
    try {
      const [cRes, pRes, sRes, hRes, aRes] = await Promise.all([
        axios.get(`${API}/candidates`).catch(() => ({ data: { candidates: [] } })),
        axios.get(`${API}/positions`).catch(() => ({ data: { positions: [] } })),
        axios.get(`${API}/exit-signals`).catch(() => ({ data: { signals: [] } })),
        axios.get(`${API}/health`).catch(() => ({ data: null })),
        axios.get(`${API}/accounts`).catch(() => ({ data: { accounts: [] } })),
      ]);
      const accountsData: Account[] = aRes.data.accounts || [];
      setCandidates(cRes.data.candidates || []);
      setPositions(pRes.data.positions || []);
      setSignals(sRes.data.signals || []);
      setHealth(hRes.data);
      setAccounts(accountsData);
      // Derive liveNav from LIVE accounts in /accounts — /nav endpoint not needed
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
  }, []);

  useEffect(() => {
    fetchAll();
    const id = setInterval(fetchAll, 5000);
    return () => clearInterval(id);
  }, [fetchAll]);

  const handleApprove = async (id: number) => {
    try {
      await axios.post(`${API}/candidates/${id}/approve`, { idempotency_key: Date.now().toString() });
      fetchAll();
    } catch {
      alert("Execution failed. Check backend logs.");
    }
  };

  const handleReject = async (id: number) => {
    try {
      await axios.post(`${API}/candidates/${id}/reject`, { reason: "Manual UI Rejection" });
      fetchAll();
    } catch (err) {
      console.error(err);
    }
  };

  const handleSignalAction = async (id: number, action: "acknowledge" | "snooze" | "dismiss") => {
    try {
      await axios.post(`${API}/exit-signals/${id}/${action}`);
      fetchAll();
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="min-h-screen bg-page text-primary">

      {/* Change 2: Operator Bar — single sticky header strip */}
      <header className="sticky top-0 z-10 bg-card/95 backdrop-blur">
        <OperatorBar
          dataAgeSeconds={dataAgeSeconds}
          systemLevel={systemLevel}
          openCount={positions.length}
          pendingCount={candidates.length}
          polling={polling}
          onRefresh={fetchAll}
        />
      </header>

      {/* Circuit breaker alert banner */}
      {health?.circuit_breaker?.state === "open" && (
        <div className="flex items-center gap-3 px-6 py-3 bg-red-900/30 border-b border-red-900/50 text-danger font-mono text-sm">
          <AlertTriangle size={15} />
          <span className="font-bold">CIRCUIT BREAKER OPEN</span>
          <span className="opacity-60">— LLM layer disabled. Check backend logs.</span>
        </div>
      )}

      {/* Two-column layout: main content + utility rail */}
      <div className="flex flex-col xl:flex-row gap-6 p-6 items-start">

        {/* ── Left column — main content ──────────────────────────────────── */}
        <div className="flex-1 min-w-0">

          {/* NAV Dashboard */}
          {accounts.length > 0 && <NavDashboard accounts={accounts} liveNav={liveNav} />}

          {/* Tab bar */}
          <div className="flex gap-1 mb-6 border-b border-subtle pb-px">
            {([
              ["candidates", "Pending",      candidates.length],
              ["positions",  "Positions",    positions.length],
              ["exits",      "Exit Signals", signals.length],
            ] as const).map(([key, label, count]) => {
              const isActive   = tab === key;
              const hasUrgent  = key === "exits" && signals.length > 0 && !isActive;
              return (
                <button
                  key={key}
                  onClick={() => setTab(key)}
                  className={`tab-btn px-5 py-2.5 text-[13px] font-mono font-medium transition-all border-b-2 flex items-center gap-2 ${
                    isActive
                      ? "text-accent border-accent bg-accent/5"
                      : hasUrgent
                        ? "text-warning border-transparent"
                        : "text-secondary border-transparent"
                  }`}
                >
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

          {/* Tab content */}
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
                {/* Filter / sort controls */}
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
                </div>
                <div className="space-y-0 w-full">{filteredPositions.map(p => <PositionRow key={p.id} p={p} />)}</div>
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
        </div>

        {/* ── Right column — utility rail ──────────────────────────────────── */}
        <div className="w-full xl:w-80 xl:shrink-0 space-y-4">

          {/* Panel 1 — Portfolio Health */}
          <div className="card-surface p-4 font-mono">
            <div className="text-[10px] text-tertiary tracking-widest mb-3">🛡 PORTFOLIO HEALTH</div>
            <div className="space-y-2">
              {([
                ["Open positions",   positions.length > 0 ? String(positions.length) : "—"],
                ["Total credit",     portfolioHealth.totalCredit != null ? `$${portfolioHealth.totalCredit.toFixed(2)}` : "—"],
                ["Net delta",        "—"],
                ["Theta / day",      "—"],
                ["Vega",             "—"],
              ] as [string, string][]).map(([l, v]) => (
                <div key={l} className="flex justify-between text-xs">
                  <span className="text-tertiary">{l}</span>
                  <span className="text-secondary">{v}</span>
                </div>
              ))}
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

          {/* Panel 2 — System Status */}
          <div className="card-surface p-4 font-mono">
            <div className="text-[10px] text-tertiary tracking-widest mb-3">⚡ SYSTEM STATUS</div>
            <div className="space-y-2 mb-4">
              <div className="flex justify-between items-center text-xs">
                <span className="text-tertiary">LLM</span>
                <span className={`flex items-center gap-1 ${health?.circuit_breaker?.state === "closed" ? "text-success" : "text-danger"}`}>
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-current" />
                  {health?.circuit_breaker?.state === "closed" ? "Online" : "Tripped"}
                </span>
              </div>
              <div className="flex justify-between items-center text-xs">
                <span className="text-tertiary">Data feed</span>
                <span className={`flex items-center gap-1 ${
                  systemLevel === "live" ? "text-success" :
                  systemLevel === "delayed" ? "text-warning" : "text-danger"
                }`}>
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-current" />
                  {systemLevel.toUpperCase()}
                </span>
              </div>
              <div className="flex justify-between items-center text-xs">
                <span className="text-tertiary">Token</span>
                {(() => {
                  const raw = health?.token;
                  if (raw == null) return <span className="text-tertiary">—</span>;
                  // Structured object: valid + days_remaining
                  if (typeof raw === "object") {
                    const tok = raw as TokenData;
                    if (tok.valid === false)
                      return <span className="text-danger">EXPIRED</span>;
                    if (tok.days_remaining != null)
                      return <span className={tok.days_remaining <= 3 ? "text-warning" : "text-success"}>{tok.days_remaining}d remaining</span>;
                    return <span className="text-tertiary">—</span>;
                  }
                  // String sentinel from backend — show it directly so operator can see why
                  const label: Record<string, string> = {
                    missing:        "No token file",
                    no_expiry_date: "Present (no expiry)",
                  };
                  const display = label[raw] ?? raw;
                  return <span className="text-tertiary text-[10px]">{display}</span>;
                })()}
              </div>
              <div className="flex justify-between items-center text-xs">
                <span className="text-tertiary">Agent loop</span>
                <span className="text-success flex items-center gap-1">
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-current" />
                  Running
                </span>
              </div>
              {([
                ["Last reconcile", "Not Initialized"],
                ["Next reconcile", "Waiting for Loop"],
              ] as [string, string][]).map(([l, v]) => (
                <div key={l} className="flex justify-between text-xs">
                  <span className="text-tertiary">{l}</span>
                  <span className="text-tertiary italic">{v}</span>
                </div>
              ))}
            </div>
            <div className="space-y-2 border-t border-subtle pt-3">
              <button
                onClick={fetchAll}
                className="w-full py-2 text-xs font-mono rounded-lg border border-subtle text-secondary hover:text-primary hover:border-focus transition-colors flex items-center justify-center gap-1.5"
              >
                ↻ Refresh Prices
              </button>
              <button
                disabled
                title="Coming soon"
                className="w-full py-2 text-xs font-mono rounded-lg border border-subtle text-tertiary opacity-30 cursor-not-allowed"
              >
                ⟳ Force Reconcile
              </button>
              <button
                disabled
                title="Coming soon"
                className="w-full py-2 text-xs font-mono rounded-lg border border-subtle text-tertiary opacity-30 cursor-not-allowed"
              >
                ⊞ Rescan Candidates
              </button>
            </div>
          </div>

          {/* Panel 3 — Alerts Feed */}
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
