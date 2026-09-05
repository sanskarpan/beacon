import { useCallback, useEffect, useState } from "react";
import { ApiError, fetchConsistency, labAction, type ConsistencyStatus } from "../api/client";

export default function ConsistencyLab() {
  const [live, setLive] = useState<ConsistencyStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ApiError | null>(null);

  const refresh = useCallback(async () => {
    try {
      const st = await fetchConsistency();
      setLive(st);
      setError(null);
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause
          : new ApiError("Unable to load consistency status", "/v1/lab/consistency", null)
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);

  const toggle = async () => {
    if (!live) return;
    setBusy(true);
    try {
      const st = await labAction(live.partitioned ? "heal" : "partition");
      setLive(st);
      setError(null);
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause
          : new ApiError("Unable to change consistency partition", "/v1/lab/consistency", null)
      );
    } finally {
      setBusy(false);
    }
  };

  const partitioned = live?.partitioned ?? false;
  const apA = live?.ap_a_instances ?? 0;
  const apB = live?.ap_b_instances ?? 0;
  const divergence = live?.divergence ?? 0;
  const cpMinorityWrite =
    live != null
      ? live.cp_minority_ok
        ? "OK"
        : `REJECTED (${live.cp_minority_msg || "no quorum"})`
      : "—";
  const cpMajorityWrite = live?.cp_majority_ok ? "OK" : live ? "REJECTED" : "—";
  const apWrite = live?.ap_write_note ?? "—";

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
          disabled={!live || busy || loading}
          className={`px-4 py-2 rounded-md border text-sm font-medium ${
            partitioned
              ? "border-signal-red/50 bg-signal-red/15 text-signal-red"
              : "border-signal-green/40 bg-signal-green/10 text-signal-green"
          }`}
        >
          {loading ? "Loading live state…" : busy ? "Updating…" : live ? partitioned ? "Partition ON — click to heal" : "Partition OFF — click to split" : "Partition unavailable"}
        </button>
      </div>

      {error && (
        <div role="alert" className="rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-2 text-sm text-signal-red">
          Consistency API unavailable: {error.message}
        </div>
      )}
      {!live && !loading && (
        <div role="status" className="rounded-xl border border-ink-600 bg-ink-900/60 p-6 text-center text-sm text-slate-500">
          No live consistency snapshot is available. Enable the consistency lab on the beacon server.
        </div>
      )}

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
              <div className={`text-xl font-mono ${cpMajorityWrite === "OK" ? "text-signal-green" : "text-signal-red"}`}>{cpMajorityWrite}</div>
            </div>
            <div className="rounded-lg bg-ink-800 p-3">
              <div className="text-xs text-slate-500">Minority writes</div>
              <div
                className={`text-xl font-mono ${
                  cpMinorityWrite.startsWith("REJECTED") ? "text-signal-red" : "text-signal-green"
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
