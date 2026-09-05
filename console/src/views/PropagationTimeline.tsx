import { useEffect, useMemo, useState } from "react";
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
import {
  ApiError,
  deregister,
  fetchGossipContrast,
  register,
  type GossipContrast,
} from "../api/client";
import { useEventStore } from "../store/events";

function durationSeconds(value: number) {
  return value / 1_000_000_000;
}

function formatElapsed(value?: number) {
  if (value == null) return "—";
  const ms = value / 1_000_000;
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(1)}ms`;
}

export default function PropagationTimeline() {
  const events = useEventStore((s) => s.events);
  const [busy, setBusy] = useState(false);
  const [lastId, setLastId] = useState<string | null>(null);
  const [actionError, setActionError] = useState<ApiError | null>(null);
  const [contrast, setContrast] = useState<GossipContrast | null>(null);
  const [contrastLoading, setContrastLoading] = useState(true);
  const [contrastError, setContrastError] = useState<ApiError | null>(null);

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

  const latestTrace = [...propEvents].reverse().find((event) => event.trace_id)?.trace_id;
  const stageEvents = useMemo(() => {
    const traceEvents = latestTrace
      ? propEvents.filter((event) => event.trace_id === latestTrace)
      : propEvents;
    return traceEvents.slice(-20);
  }, [latestTrace, propEvents]);
  const stageStart = stageEvents[0] ? new Date(stageEvents[0].timestamp).getTime() : 0;

  const loadContrast = async () => {
    setContrastLoading(true);
    try {
      setContrast(await fetchGossipContrast());
      setContrastError(null);
    } catch (error) {
      setContrastError(
        error instanceof ApiError
          ? error
          : new ApiError("Unable to load gossip contrast", "/v1/bench/gossip-contrast", null)
      );
    } finally {
      setContrastLoading(false);
    }
  };

  useEffect(() => {
    loadContrast();
  }, []);

  const triggerRegister = async () => {
    setBusy(true);
    setActionError(null);
    const id = `pay-${Date.now() % 100000}`;
    try {
      await register({ id, service: "payments", port: 8080 + (Date.now() % 100), address: "10.0.0.7" });
      setLastId(id);
    } catch (error) {
      setActionError(
        error instanceof ApiError
          ? error
          : new ApiError("Unable to register instance", "/v1/agent/service/register", null)
      );
    } finally {
      setBusy(false);
    }
  };

  const triggerKill = async () => {
    if (!lastId) return;
    setBusy(true);
    setActionError(null);
    try {
      await deregister(lastId);
      setLastId(null);
    } catch (error) {
      setActionError(
        error instanceof ApiError
          ? error
          : new ApiError("Unable to deregister instance", "/v1/agent/service/deregister", null)
      );
    } finally {
      setBusy(false);
    }
  };

  const converged = stageEvents.find((e) => e.kind === "propagation.converged");
  const contrastData = contrast
    ? [
        { config: "gossip-on", p50: durationSeconds(contrast.gossip_on_p50), p99: durationSeconds(contrast.gossip_on_p99) },
        { config: "gossip-off", p50: durationSeconds(contrast.gossip_off_p50), p99: durationSeconds(contrast.gossip_off_p99) },
      ]
    : [];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">Propagation Timeline</h1>
          <p className="text-sm text-slate-500">
            Live SSE events show the observed propagation sequence. The contrast panel reads the
            server-backed gossip benchmark when it is available.
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
            disabled={busy || !lastId}
            onClick={triggerKill}
            className="px-3 py-1.5 rounded-md bg-signal-red/15 text-signal-red border border-signal-red/30 text-sm font-medium hover:bg-signal-red/25 disabled:opacity-50"
          >
            Kill / deregister
          </button>
        </div>
      </div>
      {actionError && (
        <div role="alert" className="rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-2 text-sm text-signal-red">
          Live action failed: {actionError.message}
        </div>
      )}

      <div className="grid lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 rounded-xl border border-ink-600 bg-ink-900/60 p-4">
          <div className="text-xs font-mono text-slate-500 mb-3">SWIMLANE · one registration hop timeline</div>
          <div className="space-y-2">
             {stageEvents.map((event, index) => {
               const timestamp = new Date(event.timestamp).getTime();
               const elapsed = Number.isNaN(timestamp) || !stageStart ? 0 : Math.max(0, timestamp - stageStart);
               return (
              <div key={`${event.timestamp}-${index}`} className="flex items-center gap-3">
                <div className="w-40 text-xs font-mono text-slate-400 shrink-0">{event.kind}</div>
                <div className="flex-1 h-6 bg-ink-800 rounded relative overflow-hidden">
                  <div
                    className="absolute inset-y-0 left-0 bg-gradient-to-r from-signal-blue/80 to-signal-green/80 rounded"
                    style={{ width: `${Math.min(100, (elapsed / 1200) * 100)}%` }}
                  />
                </div>
                <div className="w-16 text-right text-xs font-mono text-slate-300">t+{elapsed}ms</div>
              </div>
               );
             })}
             {stageEvents.length === 0 && <div className="text-sm text-slate-600">Waiting for live propagation events…</div>}
          </div>
          <div className="mt-4 pt-3 border-t border-ink-600 flex items-center justify-between">
             <span className="text-sm text-slate-400">Live convergence event</span>
             <span className="text-2xl font-mono font-semibold text-signal-green">
               {formatElapsed(converged?.elapsed)}
             </span>
          </div>
        </div>

        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
           <div className="text-xs font-mono text-slate-500 mb-2">LIVE PROPAGATION EVENTS</div>
          <div className="h-[340px] overflow-auto space-y-1 font-mono text-[11px]">
            {propEvents.length === 0 && (
              <div className="text-slate-600">Waiting for events… start beacon-server and register.</div>
            )}
             {propEvents.slice(-80).map((e, i) => (
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
             GOSSIP CONTRAST · live benchmark endpoint
          </div>
          <div className="flex items-center gap-3 text-xs text-slate-500">
            {contrast && <span>p50 slowdown <span className="text-signal-amber font-mono">{contrast.slowdown_p50.toFixed(1)}×</span></span>}
            <button onClick={loadContrast} disabled={contrastLoading} className="border border-ink-600 rounded px-2 py-1 hover:text-slate-200 disabled:opacity-50">
              {contrastLoading ? "Loading…" : "Refresh"}
            </button>
          </div>
        </div>
        {contrastError && <div role="alert" className="mb-3 text-sm text-signal-red">Benchmark unavailable: {contrastError.message}</div>}
        {!contrast && contrastLoading && <div role="status" className="h-64 flex items-center justify-center text-sm text-slate-600">Loading live benchmark…</div>}
        <div className="h-64">
           <ResponsiveContainer width="100%" height="100%">
             <BarChart data={contrastData}>
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

    </div>
  );
}
