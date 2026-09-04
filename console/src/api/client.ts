import type { Instance } from "../store/events";

const BASE = "";

export async function listServices(): Promise<Record<string, string[]>> {
  const r = await fetch(`${BASE}/v1/catalog/services`);
  if (!r.ok) return {};
  return r.json();
}

export async function listInstances(name: string, passing = false): Promise<Instance[]> {
  const q = passing ? "?passing=true" : "";
  const r = await fetch(`${BASE}/v1/health/service/${encodeURIComponent(name)}${q}`);
  if (!r.ok) return [];
  return r.json();
}

export async function register(inst: Partial<Instance> & { service: string; port: number }) {
  const r = await fetch(`${BASE}/v1/agent/service/register`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      id: inst.id || `${inst.service}-${inst.port}`,
      service: inst.service,
      address: inst.address || "127.0.0.1",
      port: inst.port,
      health: "passing",
      weight: inst.weight || 1,
      node: inst.node || "console",
      tags: inst.tags || [],
    }),
  });
  return r.ok;
}

export async function deregister(id: string) {
  const r = await fetch(`${BASE}/v1/agent/service/deregister/${encodeURIComponent(id)}`, {
    method: "PUT",
  });
  return r.ok;
}

export function connectSSE(onEvent: (ev: unknown) => void, onStatus: (ok: boolean) => void) {
  let es: EventSource | null = null;
  let stopped = false;
  let attempt = 0;

  const connect = () => {
    if (stopped) return;
    es = new EventSource(`${BASE}/v1/events`);
    es.onopen = () => {
      attempt = 0;
      onStatus(true);
    };
    es.onmessage = (msg) => {
      try {
        onEvent(JSON.parse(msg.data));
      } catch {
        /* ignore */
      }
    };
    es.onerror = () => {
      onStatus(false);
      es?.close();
      const delay = Math.min(30_000, 500 * 2 ** attempt) * (0.5 + Math.random());
      attempt++;
      setTimeout(connect, delay);
    };
  };

  connect();
  return () => {
    stopped = true;
    es?.close();
  };
}

// --- Issue #154: shared API types + fetch functions ---

export type CallEdge = {
  source: string;
  target: string;
  rps: number;
  error_rate: number;
  successes: number;
  failures: number;
  window_sec: number;
};

export type WatcherInfo = { service: string; id: number; index: number };

export type WatchStats = {
  total_watchers: number;
  watchers: WatcherInfo[];
  cache: { oldest: number; newest: number; size: number } | null;
};

export type ConsistencyStatus = {
  partitioned: boolean;
  ap_a_instances: number;
  ap_b_instances: number;
  divergence: number;
  ap_write_note: string;
  cp_majority_ok: boolean;
  cp_minority_ok: boolean;
  cp_minority_msg: string;
  cp_leader: string;
  cp_index_leader: number;
  cp_index_minority: number;
};

async function getJSON<T>(path: string): Promise<T | null> {
  try {
    const r = await fetch(`${BASE}${path}`);
    if (!r.ok) return null;
    return (await r.json()) as T;
  } catch {
    return null;
  }
}

export function fetchCallEdges(): Promise<CallEdge[] | null> {
  return getJSON<CallEdge[]>("/v1/telemetry/calls");
}

export function fetchWatchStats(): Promise<WatchStats | null> {
  return getJSON<WatchStats>("/v1/watch/stats");
}

export function fetchConsistency(): Promise<ConsistencyStatus | null> {
  return getJSON<ConsistencyStatus>("/v1/lab/consistency");
}

export async function labAction(action: "partition" | "heal"): Promise<ConsistencyStatus | null> {
  try {
    const r = await fetch(`${BASE}/v1/lab/consistency/${action}`, { method: "POST" });
    if (!r.ok) return null;
    return (await r.json()) as ConsistencyStatus;
  } catch {
    return null;
  }
}
