import { useEffect, useState } from "react";
import { Activity, GitBranch, HeartPulse, Network, Radio, Scale, Shield } from "lucide-react";
import { ApiError, connectSSE, listInstances, listServices } from "./api/client";
import { useEventStore, type BeaconEvent } from "./store/events";
import MeshTopology from "./views/MeshTopology";
import PropagationTimeline from "./views/PropagationTimeline";
import HealthInspector from "./views/HealthInspector";
import WatchInspector from "./views/WatchInspector";
import XDSConsole from "./views/XDSConsole";
import ConsistencyLab from "./views/ConsistencyLab";
import LoadBalancingLab from "./views/LoadBalancingLab";

const views = [
  { id: "mesh", label: "Mesh", icon: Network },
  { id: "prop", label: "Propagation", icon: Radio },
  { id: "health", label: "Health", icon: HeartPulse },
  { id: "watch", label: "Watch", icon: Activity },
  { id: "xds", label: "xDS", icon: GitBranch },
  { id: "consist", label: "Consistency", icon: Shield },
  { id: "lb", label: "Load Balance", icon: Scale },
] as const;

type ViewId = (typeof views)[number]["id"];

export default function App() {
  const [view, setView] = useState<ViewId>("prop");
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [catalogError, setCatalogError] = useState<ApiError | null>(null);
  const { live, setLive, connected, setConnected, push, setServices, setAllInstances, events } =
    useEventStore();

  useEffect(() => {
    const stop = connectSSE(
      (ev) => push(ev as BeaconEvent),
      (ok) => setConnected(ok)
    );
    return stop;
  }, [push, setConnected]);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      setCatalogLoading(true);
      try {
        const svcs = await listServices();
        if (cancelled) return;
        setServices(svcs);
        const results = await Promise.all(
          Object.keys(svcs).map(async (name) => [name, await listInstances(name)] as const)
        );
        if (cancelled) return;
        setAllInstances(Object.fromEntries(results));
        setCatalogError(null);
      } catch (error) {
        if (!cancelled) {
          setCatalogError(
            error instanceof ApiError
              ? error
              : new ApiError("Unable to load catalog data", "/v1/catalog/services", null)
          );
        }
      } finally {
        if (!cancelled) setCatalogLoading(false);
      }
    };
    tick();
    const id = setInterval(tick, 3000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [setServices, setAllInstances]);

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-ink-600/80 bg-ink-900/80 backdrop-blur sticky top-0 z-20">
        <div className="max-w-[1600px] mx-auto px-4 py-3 flex items-center gap-4">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-signal-blue to-signal-green flex items-center justify-center font-mono font-bold text-ink-950 text-sm">
              b
            </div>
            <div>
              <div className="font-semibold tracking-tight text-slate-100">beacon</div>
              <div className="text-[11px] text-slate-500 font-mono -mt-0.5">
                propagation observatory
              </div>
            </div>
          </div>

          <nav className="flex flex-wrap gap-1 ml-4">
            {views.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                onClick={() => setView(id)}
                className={`px-3 py-1.5 rounded-md text-sm flex items-center gap-1.5 transition ${
                  view === id
                    ? "bg-ink-700 text-signal-blue"
                    : "text-slate-400 hover:text-slate-200 hover:bg-ink-800"
                }`}
              >
                <Icon size={14} />
                {label}
              </button>
            ))}
          </nav>

          <div className="ml-auto flex items-center gap-3 text-xs font-mono">
            <span
              className={`inline-flex items-center gap-1.5 ${
                connected ? "text-signal-green" : "text-signal-red"
              }`}
            >
              <span
                className={`h-1.5 w-1.5 rounded-full ${
                  connected ? "bg-signal-green animate-pulse" : "bg-signal-red"
                }`}
              />
              {connected ? "SSE live" : "SSE offline"}
            </span>
            <button
              onClick={() => setLive(!live)}
              className={`px-2 py-1 rounded border ${
                live
                  ? "border-signal-green/40 text-signal-green"
                  : "border-signal-amber/40 text-signal-amber"
              }`}
            >
              {live ? "LIVE" : "PAUSED"}
            </button>
            <span className="text-slate-500">{events.length} events</span>
          </div>
        </div>
      </header>

      <main className="flex-1 max-w-[1600px] w-full mx-auto px-4 py-4">
        {catalogError && (
          <div role="alert" className="mb-4 rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-2 text-sm text-signal-red">
            Catalog unavailable: {catalogError.message}. Retrying automatically.
          </div>
        )}
        {catalogLoading && Object.keys(useEventStore.getState().services).length === 0 && (
          <div role="status" className="mb-4 text-xs font-mono text-slate-500">
            Loading live catalog…
          </div>
        )}
        {view === "mesh" && <MeshTopology />}
        {view === "prop" && <PropagationTimeline />}
        {view === "health" && <HealthInspector />}
        {view === "watch" && <WatchInspector />}
        {view === "xds" && <XDSConsole />}
        {view === "consist" && <ConsistencyLab />}
        {view === "lb" && <LoadBalancingLab />}
      </main>
    </div>
  );
}
