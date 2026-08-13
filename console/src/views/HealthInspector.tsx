import { useEventStore } from "../store/events";

const STATES = ["passing", "fail×1", "fail×2", "fail×3 → critical", "pass×1", "pass×2 → passing"];

export default function HealthInspector() {
  const { instances, events } = useEventStore();
  const all = Object.values(instances).flat();
  const healthEvents = events.filter((e) =>
    e.kind.startsWith("health") || e.kind.startsWith("check") || e.kind.startsWith("outlier")
  );
  const flaps = events.filter((e) => e.kind === "health.flapping");
  const active = healthEvents.filter((e) => e.kind.startsWith("check") || e.kind === "health.changed");
  const passive = healthEvents.filter((e) => e.kind.startsWith("outlier"));

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold">Health Check Inspector</h1>
        <p className="text-sm text-slate-500">
          Hysteresis state machine: an instance must fail N times before ejection. Flapping produces
          zero catalog writes. Active (probes) vs passive (outlier) shown side by side.
        </p>
      </div>

      {flaps.length > 0 && (
        <div className="rounded-lg border border-signal-amber/40 bg-signal-amber/10 px-4 py-2 text-signal-amber text-sm font-mono">
          Flapping detected on {flaps.length} event(s) — hysteresis is suppressing catalog churn.
        </div>
      )}

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
        <div className="text-xs font-mono text-slate-500 mb-3">HYSTERESIS (defaults: 3 fails / 2 passes)</div>
        <div className="flex flex-wrap gap-2">
          {STATES.map((s, i) => (
            <div
              key={s}
              className={`px-3 py-2 rounded-lg border text-xs font-mono ${
                s.includes("critical")
                  ? "border-signal-red/40 text-signal-red bg-signal-red/10"
                  : s.includes("passing") && i === 0
                    ? "border-signal-green/40 text-signal-green bg-signal-green/10"
                    : "border-ink-600 text-slate-400 bg-ink-800"
              }`}
            >
              {i > 0 && <span className="text-slate-600 mr-1">→</span>}
              {s}
            </div>
          ))}
        </div>
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <div className="rounded-xl border border-ink-600 bg-ink-900/60 overflow-hidden">
          <div className="px-4 py-2 border-b border-ink-600 text-xs font-mono text-slate-500">
            INSTANCES
          </div>
          <table className="w-full text-sm">
            <thead className="text-xs text-slate-500 font-mono">
              <tr>
                <th className="text-left px-4 py-2">id</th>
                <th className="text-left px-4 py-2">service</th>
                <th className="text-left px-4 py-2">health</th>
                <th className="text-left px-4 py-2">addr</th>
              </tr>
            </thead>
            <tbody>
              {all.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-6 text-slate-600 text-center">
                    No instances — register via CLI or Propagation view
                  </td>
                </tr>
              )}
              {all.map((i) => (
                <tr key={i.id} className="border-t border-ink-700/50 font-mono text-xs">
                  <td className="px-4 py-2">{i.id}</td>
                  <td className="px-4 py-2">{i.service}</td>
                  <td className="px-4 py-2">
                    <span
                      className={
                        i.health === "passing"
                          ? "text-signal-green"
                          : i.health === "warning"
                            ? "text-signal-amber"
                            : "text-signal-red"
                      }
                    >
                      {i.health}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-slate-400">
                    {i.address}:{i.port}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4 space-y-3">
          <div>
            <div className="text-xs font-mono text-slate-500 mb-1">ACTIVE (probes)</div>
            <div className="h-28 overflow-auto space-y-1 font-mono text-[11px]">
              {active.slice(0, 40).map((e, idx) => (
                <div key={idx} className="text-slate-300 border-b border-ink-700/40 py-0.5">
                  <span className="text-signal-green">{e.kind}</span> {e.instance} {e.detail}
                </div>
              ))}
              {active.length === 0 && <div className="text-slate-600">No active check events.</div>}
            </div>
          </div>
          <div>
            <div className="text-xs font-mono text-slate-500 mb-1">PASSIVE (outlier)</div>
            <div className="h-28 overflow-auto space-y-1 font-mono text-[11px]">
              {passive.slice(0, 40).map((e, idx) => (
                <div key={idx} className="text-slate-300 border-b border-ink-700/40 py-0.5">
                  <span className="text-signal-amber">{e.kind}</span> {e.detail}
                </div>
              ))}
              {passive.length === 0 && <div className="text-slate-600">No outlier events.</div>}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
