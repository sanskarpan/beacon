import type { Instance } from "../store/events";

const BASE = "";

export class ApiError extends Error {
  readonly status: number | null;
  readonly path: string;
  readonly code?: string;

  constructor(message: string, path: string, status: number | null, code?: string) {
    super(message);
    this.name = "ApiError";
    this.path = path;
    this.status = status;
    this.code = code;
  }
}

type ErrorPayload = { code?: string; message?: string };

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${BASE}${path}`, init);
  } catch {
    throw new ApiError("Unable to reach the beacon API", path, null);
  }

  if (!response.ok) {
    let payload: ErrorPayload = {};
    try {
      payload = (await response.json()) as ErrorPayload;
    } catch {
      // Keep the HTTP status useful when the server returns a non-JSON error.
    }
    throw new ApiError(
      payload.message || `Beacon API returned HTTP ${response.status}`,
      path,
      response.status,
      payload.code
    );
  }

  try {
    return (await response.json()) as T;
  } catch {
    throw new ApiError("Beacon API returned invalid JSON", path, response.status);
  }
}

async function requestOk(path: string, init?: RequestInit): Promise<void> {
  let response: Response;
  try {
    response = await fetch(`${BASE}${path}`, init);
  } catch {
    throw new ApiError("Unable to reach the beacon API", path, null);
  }
  if (!response.ok) {
    let payload: ErrorPayload = {};
    try {
      payload = (await response.json()) as ErrorPayload;
    } catch {
      // Keep the HTTP status useful when the server returns a non-JSON error.
    }
    throw new ApiError(
      payload.message || `Beacon API returned HTTP ${response.status}`,
      path,
      response.status,
      payload.code
    );
  }
}

export async function listServices(): Promise<Record<string, string[]>> {
  return request<Record<string, string[]>>("/v1/catalog/services");
}

export async function listInstances(name: string, passing = false): Promise<Instance[]> {
  const q = passing ? "?passing=true" : "";
  return request<Instance[]>(`/v1/health/service/${encodeURIComponent(name)}${q}`);
}

export type RegisterResponse = { id: string; index: number };

export async function register(
  inst: Partial<Instance> & { service: string; port: number }
): Promise<RegisterResponse> {
  return request<RegisterResponse>("/v1/agent/service/register", {
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
}

export async function deregister(id: string): Promise<void> {
  return requestOk(`/v1/agent/service/deregister/${encodeURIComponent(id)}`, { method: "PUT" });
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

export function fetchCallEdges(): Promise<CallEdge[]> {
  return request<CallEdge[]>("/v1/telemetry/calls");
}

export function fetchWatchStats(): Promise<WatchStats> {
  return request<WatchStats>("/v1/watch/stats");
}

export function fetchConsistency(): Promise<ConsistencyStatus> {
  return request<ConsistencyStatus>("/v1/lab/consistency");
}

export type GossipContrast = {
  gossip_on_p50: number;
  gossip_on_p99: number;
  gossip_off_p50: number;
  gossip_off_p99: number;
  slowdown_p50: number;
  slowdown_p99: number;
  samples: number;
  nodes: number;
  ae_interval: number;
  note: string;
};

export type XDSStatus = {
  configured: boolean;
  nodes: string[];
  count?: number;
  detail?: string;
  node?: string;
  found?: boolean;
  version?: string;
  resources?: Record<string, unknown[]>;
};

export function fetchGossipContrast(): Promise<GossipContrast> {
  return request<GossipContrast>("/v1/bench/gossip-contrast");
}

export function fetchXDSStatus(): Promise<XDSStatus> {
  return request<XDSStatus>("/v1/xds/status");
}

export function labAction(action: "partition" | "heal"): Promise<ConsistencyStatus> {
  return request<ConsistencyStatus>(`/v1/lab/consistency/${action}`, { method: "POST" });
}
