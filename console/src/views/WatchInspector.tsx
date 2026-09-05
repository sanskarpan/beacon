import { useEffect, useMemo, useState } from "react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { ApiError, fetchWatchStats, type WatchStats } from "../api/client";
import { useEventStore } from "../store/events";

export default function WatchInspector() {
  const events = useEventStore((s) => s.events);
  const watchEvents = events.filter((e) => e.kind.startsWith("watch"));
  const [stats, setStats] = useState<WatchStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ApiError | null>(null);

  useEffect(() => {
    let alive = true;
    const fetchStats = async () => {
      try {
        const next = await fetchWatchStats();
        if (!alive) return;
        setStats(next);
        setError(null);
      } catch (cause) {
        if (alive) {
          setError(
            cause instanceof ApiError
              ? cause
              : new ApiError("Unable to load watcher stats", "/v1/watch/stats", null)
          );
        }
      } finally {
        if (alive) setLoading(false);
      }
    };
    fetchStats();
    const id = setInterval(fetchStats, 2000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const histogram = useMemo(() => {
    const buckets: Record<number, number> = {};
    for (const e of watchEvents.filter((x) => x.kind === "watch.notified")) {
      const t = new Date(e.timestamp).getTime();
      if (Number.isNaN(t)) continue;
      const b = Math.floor(t / 10) * 10;
      buckets[b] = (buckets[b] || 0) + 1;
    }
    return Object.entries(buckets)
      .sort(([a], [b]) => Number(a) - Number(b))
      .slice(-40)
      .map(([t, count]) => ({ t: new Date(Number(t)).toISOString().slice(11, 23), count }));
  }, [watchEvents]);

  const herd = events.some((e) => e.kind === "watch.herd");

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold">Watch Stream Inspector</h1>
        <p className="text-sm text-slate-500">
          Notification timestamps bucketed by 10ms. A spike = thundering herd. Staggered fan-out
          should flatten the histogram.
        </p>
      </div>

      {herd && (
        <div className="rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-2 text-signal-red text-sm">
          Herd detected — many watchers notified in the same 10ms bucket.
        </div>
      )}

      {error && (
        <div role="alert" className="rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-2 text-sm text-signal-red">
          Watcher stats unavailable: {error.message}
        </div>
      )}

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4 h-72">
        <div className="text-xs font-mono text-slate-500 mb-2">HERD HISTOGRAM</div>
        <ResponsiveContainer width="100%" height="90%">
          <BarChart data={histogram.length ? histogram : [{ t: "—", count: 0 }]}>
            <XAxis dataKey="t" hide />
            <YAxis tick={{ fill: "#64748b", fontSize: 10 }} />
            <Tooltip contentStyle={{ background: "#151b23", border: "1px solid #2a3544" }} />
            <Bar dataKey="count" fill={herd ? "#f31260" : "#66b3ff"} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 overflow-hidden">
        <div className="px-4 py-2 border-b border-ink-600 flex items-center justify-between">
          <div className="text-xs font-mono text-slate-500">
            FULL WATCHER TABLE — {loading ? "loading" : `${stats?.total_watchers ?? 0} open`}
          </div>
          {stats?.cache && stats.cache.size != null && (
            <div className="text-[10px] font-mono text-slate-600">
              cache oldest={stats.cache.oldest} newest={stats.cache.newest} size={stats.cache.size}
            </div>
          )}
        </div>
        <div className="max-h-64 overflow-auto">
          <table className="w-full text-xs font-mono">
            <thead className="text-slate-500">
              <tr>
                <th className="text-left px-4 py-2">service</th>
                <th className="text-left px-4 py-2">watcher id</th>
                <th className="text-left px-4 py-2">last index</th>
              </tr>
            </thead>
            <tbody>
              {!loading && stats?.watchers.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-4 py-6 text-slate-600 text-center">
                    No watchers — open via `beacon watch &lt;service&gt;` or SDK resolver
                  </td>
                </tr>
              )}
              {loading && (
                <tr>
                  <td colSpan={3} className="px-4 py-6 text-slate-600 text-center">Loading live watchers…</td>
                </tr>
              )}
              {stats?.watchers.slice(0, 100).map((w) => (
                <tr key={`${w.service}-${w.id}`} className="border-t border-ink-700/40 text-slate-300">
                  <td className="px-4 py-1.5">{w.service}</td>
                  <td className="px-4 py-1.5">{w.id}</td>
                  <td className="px-4 py-1.5">{w.index}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
        <div className="text-xs font-mono text-slate-500 mb-2">WATCH EVENTS</div>
        <div className="max-h-64 overflow-auto font-mono text-[11px] space-y-1">
          {watchEvents.slice(0, 100).map((e, i) => (
            <div key={i} className="text-slate-300">
              <span className="text-signal-violet">{e.kind}</span> {e.service} idx={e.index}{" "}
              {e.detail}
            </div>
          ))}
          {watchEvents.length === 0 && <div className="text-slate-600">No watch events yet.</div>}
        </div>
      </div>
    </div>
  );
}
