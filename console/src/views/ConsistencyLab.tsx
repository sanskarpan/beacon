import { useCallback, useEffect, useState } from "react";
import { fetchConsistency, labAction, type ConsistencyStatus } from "../api/client";

export default function ConsistencyLab() {
  const [live, setLive] = useState<ConsistencyStatus | null>(null);
  const [liveOk, setLiveOk] = useState(false);
  const [simulatedPartitioned, setSimulatedPartitioned] = useState(false);

  const refresh = useCallback(async () => {
    const st = await fetchConsistency();
    if (st) {
      setLive(st);
      setLiveOk(true);
    } else {
      setLive(null);
      setLiveOk(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);

  const toggle = async () => {
    const next = live?.partitioned ? "heal" : "partition";
    const st = await labAction(next);
    if (st) {
      setLive(st);
      setLiveOk(true);
    } else {
      setSimulatedPartitioned((p) => !p);
      setLiveOk(false);
    }
  };

  // Live backend when /v1/lab/consistency is configured; otherwise fall back
  // to the simulated overlay (lab/demo mode without a wired ConsistencyLab).
  const partitioned = live?.partitioned ?? simulatedPartitioned;
  const apA = live?.ap_a_instances ?? (partitioned ? 5 : 4);
  const apB = live?.ap_b_instances ?? (partitioned ? 3 : 4);
  const divergence = live?.divergence ?? Math.abs(apA - apB);
  const cpMinorityWrite =
    live != null
      ? live.cp_minority_ok
        ? "OK"
        : `REJECTED (${live.cp_minority_msg || "no quorum"})`
      : partitioned
        ? "REJECTED (no quorum)"
        : "OK";
  const apWrite = live?.ap_write_note ?? "ACCEPTED both sides";

  return (
    <div className="space-y-4">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-xl font-semibold">Consistency Lab</h1>
          <p className="text-sm text-slate-500">
            AP risks a stale endpoint (one failed request). CP risks being unable to register at all
            (an outage). Toggle a partition and watch the trade.
          </p>
        </div>
        <button
          onClick={toggle}
          className={`px-4 py-2 rounded-md border text-sm font-medium ${
            partitioned
              ? "border-signal-red/50 bg-signal-red/15 text-signal-red"
              : "border-signal-green/40 bg-signal-green/10 text-signal-green"
          }`}
        >
          {partitioned ? "Partition ON — click to heal" : "Partition OFF — click to split"}
        </button>
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
          <div className="text-signal-green font-mono text-xs mb-2">AP · gossip-replicated</div>
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg bg-ink-800 p-3">
              <div className="text-xs text-slate-500">Side A instances</div>
              <div className="text-3xl font-mono text-slate-100">{apA}</div>
            </div>
            <div className="rounded-lg bg-ink-800 p-3">
              <div className="text-xs text-slate-500">Side B instances</div>
              <div className="text-3xl font-mono text-slate-100">{apB}</div>
            </div>
          </div>
          <div className="mt-3 text-sm">
            Writes during partition:{" "}
            <span className="text-signal-green font-mono">{apWrite}</span>
          </div>
        </div>

        <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-4">
          <div className="text-signal-blue font-mono text-xs mb-2">CP · Raft-replicated</div>
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg bg-ink-800 p-3">
              <div className="text-xs text-slate-500">Majority writes</div>
              <div className="text-xl font-mono text-signal-green">OK</div>
            </div>
            <div className="rounded-lg bg-ink-800 p-3">
              <div className="text-xs text-slate-500">Minority writes</div>
              <div
                className={`text-xl font-mono ${
                  partitioned ? "text-signal-red" : "text-signal-green"
                }`}
              >
                {cpMinorityWrite}
              </div>
            </div>
          </div>
          <div className="mt-3 text-sm text-slate-400">
            Linearizable reads require the leader. Stale reads work anywhere with{" "}
            <code className="text-signal-blue">X-Beacon-Last-Contact</code>.
          </div>
        </div>
      </div>

      <div className="rounded-xl border border-ink-600 bg-ink-900/60 p-6 text-center">
        <div className="text-xs font-mono text-slate-500 mb-1">LIVE DIVERGENCE COUNTER</div>
        <div
          className={`text-5xl font-mono font-semibold ${
            divergence > 0 ? "text-signal-amber" : "text-signal-green"
          }`}
        >
          {divergence}
        </div>
        <div className="text-sm text-slate-500 mt-1">
          instances one side sees that the other does not
        </div>
      </div>
    </div>
  );
}
