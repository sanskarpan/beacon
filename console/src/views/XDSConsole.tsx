import { useEffect, useState } from "react";
import { ApiError, fetchXDSStatus, type XDSStatus } from "../api/client";
import { useEventStore } from "../store/events";

const ORDER = ["CDS", "EDS", "LDS", "RDS"];

export default function XDSConsole() {
  const events = useEventStore((s) => s.events);
  const xds = events.filter((e) => e.kind.startsWith("xds"));
  const nacks = xds.filter((e) => e.kind === "xds.nack");
  const [status, setStatus] = useState<XDSStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ApiError | null>(null);

  useEffect(() => {
    let alive = true;
    const refresh = async () => {
      try {
        const next = await fetchXDSStatus();
        if (!alive) return;
        setStatus(next);
        setError(null);
      } catch (cause) {
        if (alive) {
          setError(
            cause instanceof ApiError
              ? cause
              : new ApiError("Unable to load xDS status", "/v1/xds/status", null)
          );
        }
      } finally {
        if (alive) setLoading(false);
      }
    };
    refresh();
    const id = setInterval(refresh, 2000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold">xDS Console</h1>
        <p className="text-sm text-slate-500">
          ADS make-before-break order: CDS → EDS → LDS → RDS. A NACK must never trigger a resend of
          the same config.
        </p>
      </div>

      {nacks.length > 0 && (
        <div className="rounded-lg border border-signal-red/50 bg-signal-red/10 p-4">
          <div className="text-signal-red font-semibold text-sm mb-1">NACK surfaced</div>
          {nacks.slice(0, 5).map((n, i) => (
            <div key={i} className="font-mono text-xs text-signal-red/90">
              {n.node} — {n.detail}
            </div>
          ))}
        </div>
      )}

      {error && (
        <div role="alert" className="rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-2 text-sm text-signal-red">
          xDS status unavailable: {error.message}
        </div>
      )}

      <div className="grid md:grid-cols-2 gap-4">
        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
          <div className="text-xs font-mono text-slate-500 mb-3">ADS ADD ORDER</div>
          <div className="flex gap-2">
            {ORDER.map((t, i) => (
              <div key={t} className="flex items-center gap-2">
                <div className="px-3 py-2 rounded-lg bg-ink-800 border border-signal-blue/30 text-signal-blue font-mono text-sm">
                  {t}
                </div>
                {i < ORDER.length - 1 && <span className="text-slate-600">→</span>}
              </div>
            ))}
          </div>
          <p className="mt-3 text-xs text-slate-500">
            Reverse on remove: LDS → RDS → EDS → CDS. Getting this backwards produces the 503 spike
            during deploys that teams blame on the app.
          </p>
        </div>

        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4 min-h-64">
          <div className="text-xs font-mono text-slate-500 mb-3">LIVE CONTROL PLANE STATUS</div>
          {loading && <div role="status" className="text-sm text-slate-600">Loading live xDS status…</div>}
          {!loading && status && (
            <div className="space-y-3 text-sm">
              <div className={status.configured ? "text-signal-green" : "text-slate-500"}>
                {status.configured ? "Configured" : "Control plane not attached"}
              </div>
              {status.detail && <div className="text-xs text-slate-500">{status.detail}</div>}
              <div className="font-mono text-xs text-slate-400">snapshots: {status.count ?? status.nodes.length}</div>
              {status.nodes.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {status.nodes.map((node) => <span key={node} className="rounded bg-ink-800 px-2 py-1 font-mono text-xs text-slate-300">{node}</span>)}
                </div>
              ) : (
                <div className="text-xs text-slate-600">No live xDS node snapshots.</div>
              )}
            </div>
          )}
        </div>
      </div>

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
        <div className="text-xs font-mono text-slate-500 mb-2">xDS EVENTS</div>
        <div className="max-h-48 overflow-auto font-mono text-[11px] space-y-1">
          {xds.slice(0, 50).map((e, i) => (
            <div key={i}>
              <span className="text-signal-violet">{e.kind}</span> {e.node} {e.detail}
            </div>
          ))}
          {xds.length === 0 && <div className="text-slate-600">No xDS events yet.</div>}
        </div>
      </div>
    </div>
  );
}
