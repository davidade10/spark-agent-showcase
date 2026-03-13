"use client";

import { useEffect, useState, useCallback } from "react";
import axios from "axios";
import {
  AlertTriangle, CheckCircle, XCircle, Clock, TrendingUp,
  Shield, Zap, RefreshCw, ChevronDown, ChevronUp,
  Activity, BarChart2, BellOff, Trash2, DollarSign
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
  expiry: string; dte: number; entry_credit: number;
  unrealized_pnl: number; profit_pct: number | null;
  net_delta: number; max_risk: number; opened_at: string;
  legs: any; meta: any; position_key: string;
}

interface ExitSignal {
  id: number; symbol: string; expiry: string; dte: number;
  reason: string; credit_received: number; debit_to_close: number;
  pnl_dollars: number; pnl_pct: number; status: string; age_minutes: number;
}

interface Account {
  account_id: string; open_positions: number;
  total_credit: number; total_margin: number; total_pnl: number;
  nav: number | null; buying_power: number | null;
  type?: string;
  daily_pnl?: number;
}

interface Event {
  symbol: string; event_type: string; event_ts: string; days_away: number;
}

interface Health {
  circuit_breaker: { state: string; failures: number; attempts: number };
  data_freshness: { last_snapshot_minutes_ago: number | null; is_stale: boolean };
  token?: { valid: boolean; days_remaining: number };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const recLabel = (rec: string) => rec.toUpperCase();
const recTextColor = (rec: string) => rec === "yes" ? "text-emerald-400" : rec === "no" ? "text-red-400" : "text-amber-400";
const recBorderBg = (rec: string) => rec === "yes"
  ? "border-emerald-500/30 bg-emerald-500/5"
  : rec === "no"
    ? "border-red-500/30 bg-red-500/5"
    : "border-amber-500/30 bg-amber-500/5";
const recBadgeBg = (rec: string) => rec === "yes"
  ? "bg-emerald-400 text-black"
  : rec === "no"
    ? "bg-red-500 text-white"
    : "bg-amber-400 text-black";

const scoreColor = (s: number) => s >= 70 ? "text-emerald-400" : s >= 50 ? "text-amber-400" : "text-red-400";
const scoreBarColor = (s: number) => s >= 70 ? "bg-emerald-400" : s >= 50 ? "bg-amber-400" : "bg-red-400";
const confPct = (c: number) => `${Math.round((c ?? 0) * 100)}%`;

function pop(putDelta: number, callDelta: number): string {
  const p = Math.max(0, Math.min(100, (1 - Math.abs(putDelta) - Math.abs(callDelta)) * 100));
  return `${p.toFixed(0)}%`;
}
function rr(credit: number, maxLoss: number): string {
  if (!maxLoss || maxLoss === 0) return "—";
  return `1:${(maxLoss / credit).toFixed(2)}`;
}

// ── Status Bar ────────────────────────────────────────────────────────────────

function StatusBar({ health }: { health: Health | null }) {
  if (!health) return null;
  const cb = health.circuit_breaker;
  const fresh = health.data_freshness;
  const token = health.token;
  return (
    <div className="flex items-center gap-6 px-6 py-2 bg-zinc-900/80 border-b border-zinc-800 text-xs font-mono">
      <span className={`flex items-center gap-1.5 ${cb?.state === "closed" ? "text-emerald-400" : "text-red-400"}`}>
        <Zap size={10} />
        LLM {cb?.state === "closed" ? "ONLINE" : "TRIPPED"}
        {cb?.attempts > 0 && <span className="text-zinc-600 ml-1">{cb?.failures}/{cb?.attempts} fail</span>}
      </span>
      <span className={`flex items-center gap-1.5 ${fresh?.is_stale ? "text-amber-400" : "text-emerald-400"}`}>
        <Activity size={10} />
        {fresh?.last_snapshot_minutes_ago !== null
          ? `DATA ${fresh?.is_stale ? "STALE" : "LIVE"} · ${fresh?.last_snapshot_minutes_ago}m ago`
          : "NO SNAPSHOTS"}
      </span>
      {token && (
        <span className={`flex items-center gap-1.5 ${token.valid && token.days_remaining > 1 ? "text-zinc-500" : "text-red-400"}`}>
          <Shield size={10} />
          TOKEN {token.valid ? `${token.days_remaining}d` : "EXPIRED"}
        </span>
      )}
    </div>
  );
}

// ── NAV Dashboard ─────────────────────────────────────────────────────────────

function NavDashboard({ accounts }: { accounts: Account[] }) {
  const combined = accounts.reduce((acc, a) => ({
    open_positions: acc.open_positions + (a.open_positions || 0),
    total_credit: acc.total_credit + (a.total_credit || 0),
    total_margin: acc.total_margin + (a.total_margin || 0),
    total_pnl: acc.total_pnl + (a.total_pnl || 0),
  }), { open_positions: 0, total_credit: 0, total_margin: 0, total_pnl: 0 });

  const fmt = (n: number | null | undefined, prefix = "$") =>
    n == null ? <span className="text-zinc-600">—</span> : `${prefix}${Number(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

  return (
    <div className="rounded-xl border border-zinc-800 overflow-hidden mb-6 shadow-lg">
      <div className="flex items-center justify-between px-4 py-2.5 bg-zinc-900 border-b border-zinc-800">
        <span className="text-xs font-mono font-semibold text-zinc-500 tracking-widest">ACCOUNT SUMMARY</span>
      </div>
      <div className="grid divide-x divide-zinc-800" style={{ gridTemplateColumns: `repeat(${accounts.length + 1}, 1fr)` }}>
        {accounts.map((a) => (
          <div key={a.account_id} className={`p-5 ${a.type === 'LIVE' ? 'bg-blue-950/10' : 'bg-zinc-950'}`}>
            <div className="flex justify-between items-start mb-1">
              <div className="text-xs font-mono text-zinc-600">{a.type || 'ACCOUNT'}</div>
              {a.daily_pnl !== undefined && (
                <div className={`text-[10px] px-1.5 py-0.5 rounded font-mono ${a.daily_pnl >= 0 ? 'bg-[#166534] text-[#22c55e]' : 'bg-[#991b1b] text-[#ef4444]'}`}>
                  {a.daily_pnl >= 0 ? '+' : ''}${a.daily_pnl.toFixed(2)}
                </div>
              )}
            </div>
            <div className={`text-xs font-mono mb-3 ${a.type === 'LIVE' ? 'text-blue-400' : 'text-zinc-400'}`}>
              ···{a.account_id.slice(-4)}
            </div>
            <div className="text-2xl font-black font-mono text-zinc-300 mb-1">{fmt(a.nav)}</div>
            <div className="text-xs text-zinc-600 mb-4 font-mono">NAV</div>
            <div className="space-y-1.5 text-xs font-mono">
              <div className="flex justify-between">
                <span className="text-zinc-600">Open positions</span>
                <span className="text-zinc-400">{a.open_positions}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-zinc-600">Total credit</span>
                <span className="text-emerald-400">{fmt(a.total_credit)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-zinc-600">Margin used</span>
                <span className="text-zinc-400">{fmt(a.total_margin)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-zinc-600">Unrealized P/L</span>
                <span className={a.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"}>
                  {a.total_pnl >= 0 ? "+" : ""}{fmt(a.total_pnl)}
                </span>
              </div>
            </div>
          </div>
        ))}
        {/* Combined */}
        <div className="p-5 bg-zinc-900/50">
          <div className="text-xs font-mono text-zinc-600 mb-1">COMBINED</div>
          <div className="text-xs font-mono text-zinc-600 mb-3">&nbsp;</div>
          <div className="text-2xl font-black font-mono text-zinc-300 mb-1">{fmt(null)}</div>
          <div className="text-xs text-zinc-600 mb-4 font-mono">NAV — pending</div>
          <div className="space-y-1.5 text-xs font-mono">
            <div className="flex justify-between">
              <span className="text-zinc-600">Open positions</span>
              <span className="text-zinc-400">{combined.open_positions}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-zinc-600">Total credit</span>
              <span className="text-emerald-400">{fmt(combined.total_credit)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-zinc-600">Margin used</span>
              <span className="text-zinc-400">{fmt(combined.total_margin)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-zinc-600">Unrealized P/L</span>
              <span className={combined.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"}>
                {combined.total_pnl >= 0 ? "+" : ""}{fmt(combined.total_pnl)}
              </span>
            </div>
          </div>
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
    <div className="mx-5 mb-4 px-4 py-3 bg-zinc-900 rounded-lg border border-zinc-800">
      <div className="text-xs font-mono text-zinc-600 mb-2 tracking-widest">MARKET CONTEXT</div>
      <div className="flex flex-wrap gap-4">
        {events.map((e, i) => {
          const isEarnings = e.event_type?.toLowerCase().includes("earnings");
          const isFomc = e.event_type?.toLowerCase().includes("fomc");
          const urgent = e.days_away <= 7;
          const color = urgent ? "text-red-400" : isEarnings ? "text-amber-400" : "text-zinc-400";
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
        <div className={`h-full rounded-full transition-all duration-700 ${scoreBarColor(score)}`}
          style={{ width: `${score}%` }} />
      </div>
      <span className={`text-sm font-mono font-bold tabular-nums w-12 text-right ${scoreColor(score)}`}>
        {score?.toFixed(1)}
      </span>
    </div>
  );
}

// ── Trade Card ────────────────────────────────────────────────────────────────

function TradeCard({ candidate, onApprove, onReject }: {
  candidate: Candidate;
  onApprove: (id: number) => void;
  onReject: (id: number) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const [acting, setActing] = useState(false);
  
  // Safely parse JSON strings if necessary
  const c = typeof candidate.candidate_json === 'string' ? JSON.parse(candidate.candidate_json) : candidate.candidate_json;
  const card = typeof candidate.llm_card === 'string' ? JSON.parse(candidate.llm_card) : candidate.llm_card;
  
  const rec = card?.recommendation ?? "no";
  const ivRank = c?.iv_rank;

  // Keyboard Shortcuts for individual cards (only when visible/first in list)
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (candidate.is_stale || acting) return; 
      if (e.key.toLowerCase() === 'a') {
        setActing(true);
        onApprove(candidate.id);
      }
      if (e.key.toLowerCase() === 'r') {
        setActing(true);
        onReject(candidate.id);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [candidate.id, candidate.is_stale, acting, onApprove, onReject]);

  return (
    <div className={`border rounded-xl overflow-hidden shadow-lg ${recBorderBg(rec)}`}>

      {candidate.is_stale && (
        <div className="flex items-center gap-2 px-4 py-2 bg-amber-500/10 border-b border-amber-500/20 text-amber-400 text-xs font-mono">
          <AlertTriangle size={11} />
          DATA IS {candidate.age_minutes.toFixed(0)} MIN OLD — MARKET MAY HAVE MOVED. REFRESH BEFORE APPROVING.
        </div>
      )}

      {/* Header */}
      <div className="flex items-start justify-between p-5 border-b border-zinc-800/60">
        <div>
          <div className="flex items-center gap-3 mb-1">
            <span className="text-3xl font-black font-mono tracking-tight text-white">{candidate.symbol}</span>
            <span className={`text-xs font-mono font-bold px-2.5 py-1 rounded ${recBadgeBg(rec)}`}>
              {recLabel(rec)}
            </span>
            <span className="text-xs text-zinc-500 font-mono">{confPct(card?.confidence)} conf</span>
            {ivRank != null && (
              <span className="flex items-center gap-1.5 px-2.5 py-1 bg-purple-500/10 border border-purple-500/20 rounded text-xs font-mono">
                <span className="text-zinc-500">IV RANK</span>
                <span className="text-purple-400 font-bold">{ivRank.toFixed(0)}</span>
              </span>
            )}
          </div>
          <div className="text-zinc-400 text-sm font-mono">
            {c?.expiry} · {c?.dte} DTE · ${c?.underlying_price?.toFixed(2)}
          </div>
        </div>
        <div className="text-right">
          <div className="text-xs text-zinc-600 font-mono mb-1">SCORE</div>
          <div className={`text-4xl font-black font-mono tabular-nums ${scoreColor(candidate.score)}`}>
            {candidate.score?.toFixed(0)}
          </div>
        </div>
      </div>

      {/* Score bar */}
      <div className="px-5 py-3 border-b border-zinc-800/60">
        <ScoreBar score={candidate.score} />
      </div>

      {/* Strikes */}
      {c && (
        <div className="grid grid-cols-4 gap-px bg-zinc-800/40 mx-5 my-4 rounded-lg overflow-hidden">
          {[
            { label: "LONG PUT", val: c.long_put_strike, delta: null },
            { label: "SHORT PUT", val: c.short_put_strike, delta: c.short_put_delta },
            { label: "SHORT CALL", val: c.short_call_strike, delta: c.short_call_delta },
            { label: "LONG CALL", val: c.long_call_strike, delta: null },
          ].map(({ label, val, delta }) => (
            <div key={label} className="bg-zinc-900 px-3 py-3 text-center">
              <div className="text-[10px] font-mono text-zinc-600 mb-1.5">{label}</div>
              <div className="text-lg font-bold font-mono text-white">${val}</div>
              {delta != null && (
                <div className="text-[11px] font-mono text-zinc-500 mt-1">Δ {delta?.toFixed(3)}</div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Economics */}
      {c && (
        <div className="flex flex-wrap gap-5 px-5 pb-4 text-sm font-mono border-b border-zinc-800/60">
          <div>
            <div className="text-[10px] text-zinc-600 mb-0.5">CREDIT</div>
            <span className="text-emerald-400 font-bold">${c.net_credit?.toFixed(2)}</span>
            <span className="text-zinc-600 text-xs ml-1">(${(c.net_credit * 100).toFixed(0)}/contract)</span>
          </div>
          <div>
            <div className="text-[10px] text-zinc-600 mb-0.5">MAX LOSS</div>
            <span className="text-red-400 font-bold">${c.max_loss?.toFixed(2)}</span>
            <span className="text-zinc-600 text-xs ml-1">(${(c.max_loss * 100).toFixed(0)}/contract)</span>
          </div>
          <div>
            <div className="text-[10px] text-zinc-600 mb-0.5">WIDTH</div>
            <span className="text-zinc-300 font-bold">${c.spread_width}</span>
          </div>
          <div>
            <div className="text-[10px] text-zinc-600 mb-0.5">R:R</div>
            <span className="text-zinc-300 font-bold">{rr(c.net_credit, c.max_loss)}</span>
          </div>
          <div>
            <div className="text-[10px] text-zinc-600 mb-0.5">POP</div>
            <span className="text-zinc-300 font-bold">{pop(c.short_put_delta, c.short_call_delta)}</span>
          </div>
        </div>
      )}

      {/* Context panel */}
      {c?.symbol && <ContextPanel symbol={c.symbol} />}

      {/* LLM Summary */}
      {card?.summary && (
        <p className="text-zinc-300 text-sm px-5 pb-3 leading-relaxed">{card.summary}</p>
      )}

      {/* Expand toggle */}
      <button
        onClick={() => setExpanded(e => !e)}
        className="flex items-center gap-1.5 text-xs text-zinc-500 hover:text-zinc-300 transition-colors font-mono px-5 pb-4"
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
                  .map((c: string, i: number) => (
                    <li key={i} className="flex gap-2 text-amber-300 leading-relaxed">
                      <span className="shrink-0 mt-0.5">›</span>{c}
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

      {/* Actions */}
      <div className="grid grid-cols-2 gap-3 p-5 border-t border-zinc-800/60 bg-zinc-950">
        <button
          onClick={() => { setActing(true); onApprove(candidate.id); }}
          disabled={acting || candidate.is_stale}
          className="flex items-center justify-center gap-2 py-3 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-mono font-bold text-sm transition-all disabled:opacity-50 disabled:bg-zinc-800"
        >
          <CheckCircle size={15} /> {candidate.is_stale ? 'STALE DATA' : 'APPROVE (A)'}
        </button>
        <button
          onClick={() => { setActing(true); onReject(candidate.id); }}
          disabled={acting}
          className="flex items-center justify-center gap-2 py-3 rounded-lg border border-red-500/40 text-red-400 hover:bg-red-900/50 font-mono font-bold text-sm transition-all disabled:opacity-50"
        >
          <XCircle size={15} /> REJECT (R)
        </button>
      </div>
    </div>
  );
}

// ── Positions Panel ───────────────────────────────────────────────────────────

function PositionRow({ p }: { p: Position }) {
  const [open, setOpen] = useState(false);
  const pnl = p.unrealized_pnl ?? 0;
  const pct = p.profit_pct;
  const target50  = p.entry_credit ? p.entry_credit * 0.5 : null;
  const stop200   = p.entry_credit ? p.entry_credit * 2.0 : null;

  return (
    <div className="rounded-xl border border-zinc-800 overflow-hidden mb-3 bg-zinc-900/50 shadow-sm">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-5 py-4 hover:bg-zinc-800/60 transition-colors"
      >
        <div className="flex items-center gap-4">
          {open ? <ChevronUp size={14} className="text-zinc-600" /> : <ChevronDown size={14} className="text-zinc-600" />}
          <span className="text-white font-bold text-base font-mono">{p.symbol}</span>
          <span className="text-zinc-500 text-sm font-mono">{p.expiry} · {p.dte}d</span>
          <span className="text-zinc-600 text-xs font-mono">···{p.account_id?.slice(-4)}</span>
        </div>
        <div className="flex items-center gap-6 font-mono text-sm">
          <span className="text-zinc-400">${p.entry_credit?.toFixed(2)} cr</span>
          <span className={pnl >= 0 ? "text-emerald-400 font-bold" : "text-red-400 font-bold"}>
            {pnl >= 0 ? "+" : ""}${pnl.toFixed(0)}
            {pct != null && <span className="text-xs ml-1 opacity-70">{pct >= 0 ? "+" : ""}{pct.toFixed(0)}%</span>}
          </span>
        </div>
      </button>

      {open && (
        <div className="border-t border-zinc-800 bg-zinc-950">
          <div className="grid grid-cols-2 divide-x divide-zinc-800">
            {/* Entry details */}
            <div className="p-4">
              <div className="text-[10px] font-mono text-zinc-600 tracking-widest mb-3">ENTRY DETAILS</div>
              <div className="space-y-2 text-sm font-mono">
                {[
                  ["Credit received", `$${p.entry_credit?.toFixed(2)}`],
                  ["Max risk (margin)", p.max_risk != null ? `$${p.max_risk?.toFixed(2)}` : "—"],
                  ["Net delta", p.net_delta?.toFixed(4) ?? "—"],
                  ["Account", `···${p.account_id?.slice(-4)}`],
                ].map(([l, v]) => (
                  <div key={l} className="flex justify-between">
                    <span className="text-zinc-600">{l}</span>
                    <span className="text-zinc-300">{v}</span>
                  </div>
                ))}
              </div>
            </div>
            {/* P&L */}
            <div className="p-4">
              <div className="text-[10px] font-mono text-zinc-600 tracking-widest mb-3">UNREALIZED P/L</div>
              <div className="space-y-2 text-sm font-mono">
                {[
                  ["Gross P/L", { val: `${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}`, color: pnl >= 0 ? "text-emerald-400" : "text-red-400" }],
                  ["% of credit", { val: pct != null ? `${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%` : "—", color: (pct ?? 0) >= 0 ? "text-emerald-400" : "text-red-400" }],
                  ["50% target", { val: target50 != null ? `$${target50.toFixed(2)}` : "—", color: "text-zinc-300" }],
                  ["200% stop", { val: stop200 != null ? `$${stop200.toFixed(2)}` : "—", color: "text-red-400" }],
                ].map(([l, v]: any) => (
                  <div key={l} className="flex justify-between">
                    <span className="text-zinc-600">{l}</span>
                    <span className={v.color}>{v.val}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
          {/* Legs */}
          {p.legs && Object.keys(p.legs).length > 0 && (
            <div className="px-4 pb-3 border-t border-zinc-800">
              <div className="text-[10px] font-mono text-zinc-600 tracking-widest mt-3 mb-2">LEGS</div>
              <div className="text-xs font-mono text-zinc-500 bg-zinc-900 rounded p-2 overflow-x-auto">
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
  const [open, setOpen] = useState(false);
  const [acting, setActing] = useState(false);

  const reasonColor = s.reason === "profit_target" ? "text-emerald-400"
    : s.reason === "stop_loss" ? "text-red-400" : "text-amber-400";

  const profitPct = s.pnl_pct ?? 0;
  const creditReceived = s.credit_received ?? 0;
  const debitToClose = s.debit_to_close ?? 0;

  return (
    <div className={`rounded-xl border overflow-hidden mb-3 ${
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
          <span className={`text-xs font-mono font-bold px-2 py-0.5 rounded-sm bg-zinc-950 border border-zinc-800 ${reasonColor}`}>
            {s.reason?.toUpperCase()}
          </span>
        </div>
        <div className="flex items-center gap-4 font-mono text-sm">
          <span className={profitPct >= 0 ? "text-emerald-400 font-bold" : "text-red-400 font-bold"}>
            {profitPct >= 0 ? "+" : ""}{profitPct.toFixed(1)}%
          </span>
          <span className={s.pnl_dollars >= 0 ? "text-emerald-400" : "text-red-400"}>
            ${s.pnl_dollars?.toFixed(0)}
          </span>
        </div>
      </button>

      {open && (
        <div className="border-t border-zinc-800 bg-zinc-950">
          <div className="grid grid-cols-2 divide-x divide-zinc-800">
            <div className="p-4">
              <div className="text-[10px] font-mono text-zinc-600 tracking-widest mb-3">SIGNAL DETAILS</div>
              <div className="space-y-2 text-sm font-mono">
                {[
                  ["Signal type", s.reason?.toUpperCase()],
                  ["Signal age", `${s.age_minutes.toFixed(0)}m ago`],
                  ["Triggered at DTE", s.dte],
                ].map(([l, v]: any) => (
                  <div key={l} className="flex justify-between">
                    <span className="text-zinc-600">{l}</span>
                    <span className="text-zinc-300">{v}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="p-4">
              <div className="text-[10px] font-mono text-zinc-600 tracking-widest mb-3">CLOSING ESTIMATE</div>
              <div className="space-y-2 text-sm font-mono">
                {[
                  ["Credit received", creditReceived > 0 ? `$${creditReceived.toFixed(2)}` : "—"],
                  ["Debit to close", debitToClose > 0 ? `$${debitToClose.toFixed(2)}` : "—"],
                  ["Est. P/L", { val: `${s.pnl_dollars >= 0 ? "+" : ""}$${s.pnl_dollars?.toFixed(2)}`, color: s.pnl_dollars >= 0 ? "text-emerald-400" : "text-red-400" }],
                  ["% of credit", { val: `${profitPct >= 0 ? "+" : ""}${profitPct.toFixed(1)}%`, color: profitPct >= 0 ? "text-emerald-400" : "text-red-400" }],
                ].map(([l, v]: any) => (
                  <div key={l} className="flex justify-between">
                    <span className="text-zinc-600">{l}</span>
                    {typeof v === "object"
                      ? <span className={v.color}>{v.val}</span>
                      : <span className="text-zinc-300">{v}</span>}
                  </div>
                ))}
              </div>
            </div>
          </div>
          <div className="px-4 py-3 bg-zinc-900/80 border-t border-zinc-800">
            <div className="text-[10px] font-mono text-zinc-600 tracking-widest mb-2">RECOMMENDED ACTION</div>
            <p className="text-zinc-400 text-sm font-mono italic">
              {s.reason === "profit_target"
                ? `Position has captured ${profitPct.toFixed(0)}% of max profit. Close now to lock in gains and free up capital.`
                : s.reason === "time_exit"
                  ? `At ${s.dte} DTE, gamma risk is increasing. Standard time-exit protocol — close to avoid expiration week risk.`
                  : `Stop loss triggered. Close immediately to prevent further loss.`}
            </p>
          </div>
          <div className="grid grid-cols-3 gap-3 p-4 border-t border-zinc-800">
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
  const [health, setHealth]         = useState<Health | null>(null);
  const [lastPoll, setLastPoll]     = useState<Date | null>(null);
  const [polling, setPolling]       = useState(false);
  const [tab, setTab]               = useState<"candidates" | "positions" | "exits">("candidates");

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
      setCandidates(cRes.data.candidates || []);
      setPositions(pRes.data.positions || []);
      setSignals(sRes.data.signals || []);
      setHealth(hRes.data);
      setAccounts(aRes.data.accounts || []);
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
      await axios.post(`${API}/approve/${id}`, { idempotency_key: Date.now().toString() });
      fetchAll();
    } catch (err) {
      alert("Execution failed. Check backend logs.");
    }
  };

  const handleReject = async (id: number) => {
    try {
      await axios.post(`${API}/reject/${id}`, { reason: "Manual UI Rejection" });
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
    <div className="min-h-screen bg-[#0d0d0d] text-white">
      {/* Header */}
      <header className="border-b border-zinc-800 sticky top-0 z-10 bg-[#0d0d0d]/95 backdrop-blur">
        <div className="flex items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <Activity className="text-emerald-500" size={20} />
            <span className="font-mono font-black tracking-widest text-lg text-white">SPARK COMMAND CENTER</span>
            <span className="text-zinc-600 font-mono text-xs hidden sm:inline">/ Iron Condor Desk</span>
          </div>
          <div className="flex items-center gap-4">
            <span className="text-xs font-mono text-zinc-600">
              {lastPoll ? lastPoll.toLocaleTimeString() : "—"}
            </span>
            <button
              onClick={fetchAll}
              className="flex items-center gap-1.5 text-xs font-mono text-zinc-500 hover:text-zinc-300 transition-colors"
            >
              <RefreshCw size={12} className={polling ? "animate-spin" : ""} />
              REFRESH
            </button>
          </div>
        </div>
        <StatusBar health={health} />
      </header>

      {/* Circuit breaker banner */}
      {health?.circuit_breaker?.state === "open" && (
        <div className="flex items-center gap-3 px-6 py-3 bg-red-900/30 border-b border-red-900/50 text-red-400 font-mono text-sm">
          <AlertTriangle size={15} />
          <span className="font-bold">CIRCUIT BREAKER OPEN</span>
          <span className="text-red-500/60">— LLM layer disabled. Check backend logs.</span>
        </div>
      )}

      <div className="max-w-6xl mx-auto px-6 py-6">

        {/* NAV Dashboard */}
        {accounts.length > 0 && <NavDashboard accounts={accounts} />}

        {/* Tabs */}
        <div className="flex gap-2 mb-6 border-b border-zinc-800 pb-px">
          {([
            ["candidates", `Pending (${candidates.length})`],
            ["positions", `Positions (${positions.length})`],
            ["exits", `Exit Signals (${signals.length})`],
          ] as const).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setTab(key)}
              className={`px-6 py-3 text-sm font-mono font-medium transition-all border-b-2 ${
                tab === key
                  ? "text-emerald-400 border-emerald-400 bg-emerald-500/5"
                  : "text-zinc-500 border-transparent hover:text-zinc-300 hover:border-zinc-700"
              }${key === "exits" && signals.length > 0 && tab !== "exits" ? " !text-amber-400" : ""}`}
            >
              {label}
            </button>
          ))}
        </div>

        {/* Content */}
        {tab === "candidates" && (
          candidates.length === 0 ? (
            <div className="text-center py-20 border border-dashed border-zinc-800 rounded-xl bg-zinc-900/30">
              <TrendingUp size={32} className="mx-auto mb-3 text-zinc-700" />
              <div className="text-zinc-400 text-lg">No pending trade candidates</div>
              <div className="text-xs mt-2 text-zinc-600 font-mono">Polling every 5s · Awaiting strategy engine output</div>
            </div>
          ) : (
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              {candidates.map(c => (
                <TradeCard key={c.id} candidate={c} onApprove={handleApprove} onReject={handleReject} />
              ))}
            </div>
          )
        )}

        {tab === "positions" && (
          positions.length === 0 ? (
            <div className="text-center py-20 border border-dashed border-zinc-800 rounded-xl bg-zinc-900/30 font-mono text-sm text-zinc-500">
              No open positions found in database.
            </div>
          ) : (
            <div className="space-y-4 max-w-4xl">{positions.map(p => <PositionRow key={p.id} p={p} />)}</div>
          )
        )}

        {tab === "exits" && (
          signals.length === 0 ? (
            <div className="text-center py-20 border border-dashed border-zinc-800 rounded-xl bg-zinc-900/30 font-mono text-sm text-zinc-500">
              No pending exit signals triggered.
            </div>
          ) : (
            <div className="space-y-4 max-w-4xl">{signals.map(s => <ExitSignalRow key={s.id} s={s} onAction={handleSignalAction} />)}</div>
          )
        )}
      </div>
    </div>
  );
}