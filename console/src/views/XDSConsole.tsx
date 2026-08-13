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
import { useEventStore } from "../store/events";

const ORDER = ["CDS", "EDS", "LDS", "RDS"];

export default function XDSConsole() {
  const events = useEventStore((s) => s.events);
  const xds = events.filter((e) => e.kind.startsWith("xds"));
  const nacks = xds.filter((e) => e.kind === "xds.nack");

  const bytes = [
    { mode: "SotW (1 change / 5k eps)", bytes: 5_000_000 },
    { mode: "Delta (1 change / 5k eps)", bytes: 1_000 },
  ];

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

        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4 h-64">
          <div className="text-xs font-mono text-slate-500 mb-2">SotW vs Delta bytes</div>
          <ResponsiveContainer width="100%" height="85%">
            <BarChart data={bytes} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#1c2430" />
              <XAxis type="number" tick={{ fill: "#64748b", fontSize: 10 }} />
              <YAxis type="category" dataKey="mode" width={160} tick={{ fill: "#94a3b8", fontSize: 10 }} />
              <Tooltip contentStyle={{ background: "#151b23", border: "1px solid #2a3544" }} />
              <Legend />
              <Bar dataKey="bytes" fill="#a78bfa" name="bytes pushed" />
            </BarChart>
          </ResponsiveContainer>
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
