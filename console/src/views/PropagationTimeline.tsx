import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { register, deregister } from "../api/client";
import { useEventStore } from "../store/events";

const PATHS = [
  { config: "gossip+streaming", p50: 2.011, p99: 2.011, color: "#3dd68c" },
  { config: "health+streaming", p50: 15.061, p99: 15.061, color: "#66b3ff" },
  { config: "health+blocking", p50: 17.56, p99: 17.56, color: "#a78bfa" },
  { config: "health+dns", p50: 45.15, p99: 45.15, color: "#f31260" },
];

const HOPS = [
  { hop: "SDK Register", t: 0 },
  { hop: "agent local state", t: 3 },
  { hop: "catalog write", t: 8 },
  { hop: "server-1", t: 11 },
  { hop: "gossip → server-2", t: 340 },
  { hop: "gossip → server-3", t: 680 },
  { hop: "watch fan-out", t: 692 },
  { hop: "client address list", t: 710 },
  { hop: "DNS path (contrast)", t: 1200 },
];

export default function PropagationTimeline() {
  const events = useEventStore((s) => s.events);
  const [busy, setBusy] = useState(false);
  const [lastId, setLastId] = useState("demo-pay-1");

  const propEvents = useMemo(
    () =>
      events.filter((e) =>
        [
          "instance.registered",
          "gossip.delta",
          "watch.notified",
          "propagation.converged",
          "node.failed",
          "resolve.request",
        ].includes(e.kind)
      ),
    [events]
  );

  const triggerRegister = async () => {
    setBusy(true);
    const id = `pay-${Date.now() % 100000}`;
    setLastId(id);
    await register({ id, service: "payments", port: 8080 + (Date.now() % 100), address: "10.0.0.7" });
    setBusy(false);
  };

  const triggerKill = async () => {
    setBusy(true);
    await deregister(lastId);
    setBusy(false);
  };

  const converged = propEvents.find((e) => e.kind === "propagation.converged");

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">Propagation Timeline</h1>
          <p className="text-sm text-slate-500">
            Flagship view — the stale-endpoint window, broken into stages. The ~22× spread between
            gossip+streaming and health+DNS is the engineering argument.
          </p>
        </div>
        <div className="flex gap-2">
          <button
            disabled={busy}
            onClick={triggerRegister}
            className="px-3 py-1.5 rounded-md bg-signal-green/15 text-signal-green border border-signal-green/30 text-sm font-medium hover:bg-signal-green/25 disabled:opacity-50"
          >
            Register instance
          </button>
          <button
            disabled={busy}
            onClick={triggerKill}
            className="px-3 py-1.5 rounded-md bg-signal-red/15 text-signal-red border border-signal-red/30 text-sm font-medium hover:bg-signal-red/25 disabled:opacity-50"
          >
            Kill / deregister
          </button>
        </div>
      </div>

      <div className="grid lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 rounded-xl border border-ink-600 bg-ink-900/60 p-4">
          <div className="text-xs font-mono text-slate-500 mb-3">SWIMLANE · one registration hop timeline</div>
          <div className="space-y-2">
            {HOPS.map((h) => (
              <div key={h.hop} className="flex items-center gap-3">
                <div className="w-40 text-xs font-mono text-slate-400 shrink-0">{h.hop}</div>
                <div className="flex-1 h-6 bg-ink-800 rounded relative overflow-hidden">
                  <div
                    className="absolute inset-y-0 left-0 bg-gradient-to-r from-signal-blue/80 to-signal-green/80 rounded"
                    style={{ width: `${Math.min(100, (h.t / 1200) * 100)}%` }}
                  />
                </div>
                <div className="w-16 text-right text-xs font-mono text-slate-300">t+{h.t}ms</div>
              </div>
            ))}
          </div>
          <div className="mt-4 pt-3 border-t border-ink-600 flex items-center justify-between">
            <span className="text-sm text-slate-400">Convergence (demo path)</span>
            <span className="text-2xl font-mono font-semibold text-signal-green">
              {converged?.elapsed ? `${converged.elapsed}` : "1.2s"}
            </span>
          </div>
        </div>

        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
          <div className="text-xs font-mono text-slate-500 mb-2">LIVE PROPAGATION EVENTS</div>
          <div className="h-[340px] overflow-auto space-y-1 font-mono text-[11px]">
            {propEvents.length === 0 && (
              <div className="text-slate-600">Waiting for events… start beacon-server and register.</div>
            )}
            {propEvents.slice(0, 80).map((e, i) => (
              <div key={i} className="border-b border-ink-700/50 py-1 text-slate-300">
                <span className="text-signal-blue">{e.kind}</span>{" "}
                <span className="text-slate-500">{e.trace_id?.slice(0, 12)}</span>{" "}
                {e.detail || e.service || e.instance}
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
        <div className="flex items-center justify-between mb-2">
          <div className="text-xs font-mono text-slate-500">
            STALE-ENDPOINT WINDOW · configuration comparison (beacon bench propagate)
          </div>
          <div className="text-xs text-slate-500">
            ratio p50 dns/gossip ≈{" "}
            <span className="text-signal-amber font-mono">
              {(PATHS[3].p50 / PATHS[0].p50).toFixed(1)}×
            </span>
          </div>
        </div>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={PATHS}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1c2430" />
              <XAxis dataKey="config" tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <YAxis
                tick={{ fill: "#94a3b8", fontSize: 11 }}
                label={{ value: "seconds", angle: -90, position: "insideLeft", fill: "#64748b" }}
              />
              <Tooltip
                contentStyle={{ background: "#151b23", border: "1px solid #2a3544" }}
                labelStyle={{ color: "#e2e8f0" }}
              />
              <Legend />
              <Bar dataKey="p50" name="p50 (s)" fill="#66b3ff" radius={[4, 4, 0, 0]} />
              <Bar dataKey="p99" name="p99 (s)" fill="#3dd68c" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
        <div className="text-xs font-mono text-slate-500 mb-2">PER-HOP LATENCY (stacked stages, ms)</div>
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={[
                { hop: "SDK", ms: 3 },
                { hop: "agent", ms: 5 },
                { hop: "catalog", ms: 3 },
                { hop: "gossip", ms: 670 },
                { hop: "watch", ms: 12 },
                { hop: "client", ms: 18 },
              ]}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1c2430" />
              <XAxis dataKey="hop" tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <YAxis tick={{ fill: "#64748b", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#151b23", border: "1px solid #2a3544" }} />
              <Bar dataKey="ms" fill="#a78bfa" radius={[4, 4, 0, 0]} name="ms" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
