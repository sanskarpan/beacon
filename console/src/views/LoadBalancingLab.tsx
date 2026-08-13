import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type Policy = "round_robin" | "p2c" | "weighted_round_robin";

function simulate(policy: Policy, slowIdx: number, n = 1000) {
  const eps = [
    { name: "a", weight: 1, inflight: 0 },
    { name: "b", weight: 1, inflight: 0 },
    { name: "c", weight: 3, inflight: 0 },
    { name: "d", weight: 1, inflight: 0 },
  ];
  const counts = Object.fromEntries(eps.map((e) => [e.name, 0]));
  let rr = 0;
  const current = eps.map(() => 0);
  const totalW = eps.reduce((s, e) => s + e.weight, 0);

  for (let i = 0; i < n; i++) {
    let pick = 0;
    if (policy === "round_robin") {
      pick = rr++ % eps.length;
    } else if (policy === "weighted_round_robin") {
      let best = 0;
      for (let j = 0; j < eps.length; j++) {
        current[j] += eps[j].weight;
        if (current[j] > current[best]) best = j;
      }
      current[best] -= totalW;
      pick = best;
    } else {
      // p2c
      let a = Math.floor(Math.random() * eps.length);
      let b = Math.floor(Math.random() * (eps.length - 1));
      if (b >= a) b++;
      const load = (j: number) => {
        const base = eps[j].inflight / eps[j].weight;
        return j === slowIdx ? base + 50 : base;
      };
      pick = load(a) <= load(b) ? a : b;
    }
    counts[eps[pick].name]++;
    eps[pick].inflight++;
    // complete quickly except slow
    if (pick !== slowIdx) eps[pick].inflight = Math.max(0, eps[pick].inflight - 1);
  }
  return eps.map((e) => ({ name: e.name, hits: counts[e.name], slow: e.name === eps[slowIdx].name }));
}

export default function LoadBalancingLab() {
  const [policy, setPolicy] = useState<Policy>("p2c");
  const [slow, setSlow] = useState(1);
  const data = useMemo(() => simulate(policy, slow), [policy, slow]);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold">Load Balancing Lab</h1>
        <p className="text-sm text-slate-500">
          Inject a slow instance and watch P2C route around it while round-robin does not. “Pick the
          least loaded” herds; P2C’s randomness breaks the synchronisation.
        </p>
      </div>

      <div className="flex flex-wrap gap-2">
        {(["round_robin", "p2c", "weighted_round_robin"] as Policy[]).map((p) => (
          <button
            key={p}
            onClick={() => setPolicy(p)}
            className={`px-3 py-1.5 rounded-md text-sm font-mono border ${
              policy === p
                ? "border-signal-blue text-signal-blue bg-signal-blue/10"
                : "border-ink-600 text-slate-400"
            }`}
          >
            {p}
          </button>
        ))}
        <label className="ml-auto text-sm text-slate-400 flex items-center gap-2">
          slow instance
          <select
            value={slow}
            onChange={(e) => setSlow(Number(e.target.value))}
            className="bg-ink-800 border border-ink-600 rounded px-2 py-1 font-mono text-slate-200"
          >
            <option value={0}>a</option>
            <option value={1}>b</option>
            <option value={2}>c</option>
            <option value={3}>d</option>
          </select>
        </label>
      </div>

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4 h-80">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1c2430" />
            <XAxis dataKey="name" tick={{ fill: "#94a3b8" }} />
            <YAxis tick={{ fill: "#64748b" }} />
            <Tooltip contentStyle={{ background: "#151b23", border: "1px solid #2a3544" }} />
            <Bar dataKey="hits" fill="#66b3ff" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <p className="text-xs text-slate-500 font-mono">
        Red bar = injected slow instance. Under P2C it should receive fewer hits than under
        round-robin.
      </p>
    </div>
  );
}
