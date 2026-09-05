import { useEffect, useMemo, useRef, useState } from "react";
import * as d3 from "d3";
import { ApiError, fetchCallEdges, type CallEdge } from "../api/client";
import { useEventStore } from "../store/events";

type Node = d3.SimulationNodeDatum & {
  id: string;
  kind: "service" | "instance";
  health?: string;
  weight?: number;
  zone?: string;
  label: string;
};

type Link = d3.SimulationLinkDatum<Node> & {
  source: string | Node;
  target: string | Node;
  kind: "membership" | "call";
  rps?: number;
  errorRate?: number;
};

const healthColor = (h?: string) => {
  switch (h) {
    case "passing":
      return "#3dd68c";
    case "warning":
      return "#f5a524";
    case "critical":
      return "#f31260";
    case "maintenance":
      return "#66b3ff";
    default:
      return "#64748b";
  }
};

export default function MeshTopology() {
  const ref = useRef<SVGSVGElement>(null);
  const { services, instances } = useEventStore();
  const [callEdges, setCallEdges] = useState<CallEdge[]>([]);
  const [callLoading, setCallLoading] = useState(true);
  const [callError, setCallError] = useState<ApiError | null>(null);

  useEffect(() => {
    let alive = true;
    const refresh = async () => {
      try {
        const edges = await fetchCallEdges();
        if (!alive) return;
        setCallEdges(edges);
        setCallError(null);
      } catch (error) {
        if (alive) {
          setCallError(
            error instanceof ApiError
              ? error
              : new ApiError("Unable to load call graph", "/v1/telemetry/calls", null)
          );
        }
      } finally {
        if (alive) setCallLoading(false);
      }
    };
    refresh();
    const id = setInterval(refresh, 2000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const graph = useMemo(() => {
    const nodes: Node[] = [];
    const links: Link[] = [];
    const nodeIds = new Set<string>();
    const addService = (name: string) => {
      const id = `svc:${name}`;
      if (!nodeIds.has(id)) {
        nodeIds.add(id);
        nodes.push({ id, kind: "service", label: name });
      }
      return id;
    };
    for (const name of Object.keys(services)) {
      addService(name);
      const list = instances[name] || [];
      for (const inst of list) {
        const id = `inst:${inst.id}`;
        nodes.push({
          id,
          kind: "instance",
          label: inst.id,
          health: inst.health,
          weight: inst.weight,
          zone: inst.locality?.zone,
        });
        links.push({ kind: "membership", source: `svc:${name}`, target: id });
      }
    }
    for (const edge of callEdges) {
      if (!edge.source || !edge.target) continue;
      links.push({
        kind: "call",
        source: addService(edge.source),
        target: addService(edge.target),
        rps: edge.rps,
        errorRate: edge.error_rate,
      });
    }
    return { nodes, links };
  }, [services, instances, callEdges]);

  useEffect(() => {
    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    const width = ref.current?.clientWidth || 900;
    const height = 520;

    svg.attr("viewBox", `0 0 ${width} ${height}`);

    const sim = d3
      .forceSimulation(graph.nodes)
      .force(
        "link",
        d3
          .forceLink<Node, Link>(graph.links)
          .id((d) => d.id)
           .distance((d) => (d.kind === "call" ? 140 : 70))
      )
      .force("charge", d3.forceManyBody().strength(-180))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide(24));

    // zone regions (soft)
    const zones = Array.from(new Set(graph.nodes.map((n) => n.zone).filter(Boolean))) as string[];
    const zoneG = svg.append("g").attr("class", "zones");

    const link = svg
      .append("g")
      .selectAll("line")
      .data(graph.links)
      .join("line")
      .attr("stroke", (d) => {
        if (d.kind === "membership") return "#2a3544";
        return (d.errorRate || 0) > 0 ? "#f31260" : "#66b3ff";
      })
      .attr("stroke-width", (d) =>
        d.kind === "call" ? Math.max(1.5, Math.min(8, Math.log2((d.rps || 0) + 1) + 1)) : 1.2
      );

    const node = svg
      .append("g")
      .selectAll("g")
      .data(graph.nodes)
      .join("g")
      .call(
        d3
          .drag<SVGGElement, Node>()
          .on("start", (event, d) => {
            if (!event.active) sim.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
          })
          .on("drag", (event, d) => {
            d.fx = event.x;
            d.fy = event.y;
          })
          .on("end", (event, d) => {
            if (!event.active) sim.alphaTarget(0);
            d.fx = null;
            d.fy = null;
          }) as never
      );

    node
      .append("circle")
      .attr("r", (d) => (d.kind === "service" ? 18 : 8 + (d.weight || 1)))
      .attr("fill", (d) => (d.kind === "service" ? "#1c2430" : healthColor(d.health)))
      .attr("stroke", (d) => (d.kind === "service" ? "#66b3ff" : "#0a0e14"))
      .attr("stroke-width", (d) => (d.kind === "service" ? 2 : 1));

    node
      .append("text")
      .text((d) => d.label)
      .attr("x", 0)
      .attr("y", (d) => (d.kind === "service" ? 32 : 18))
      .attr("text-anchor", "middle")
      .attr("fill", "#94a3b8")
      .attr("font-size", 10)
      .attr("font-family", "IBM Plex Mono, monospace");

    sim.on("tick", () => {
      link
        .attr("x1", (d) => (d.source as Node).x!)
        .attr("y1", (d) => (d.source as Node).y!)
        .attr("x2", (d) => (d.target as Node).x!)
        .attr("y2", (d) => (d.target as Node).y!);
      node.attr("transform", (d) => `translate(${d.x},${d.y})`);

      // rough zone hulls
      zoneG.selectAll("*").remove();
      for (const z of zones) {
        const pts = graph.nodes.filter((n) => n.zone === z && n.x != null) as Node[];
        if (pts.length < 2) continue;
        const hull = d3.polygonHull(pts.map((p) => [p.x!, p.y!] as [number, number]));
        if (!hull) continue;
        zoneG
          .append("path")
          .attr("d", `M${hull.join("L")}Z`)
          .attr("fill", z.endsWith("a") ? "rgba(102,179,255,0.06)" : "rgba(167,139,250,0.06)")
          .attr("stroke", z.endsWith("a") ? "rgba(102,179,255,0.25)" : "rgba(167,139,250,0.25)")
          .attr("stroke-dasharray", "4 3");
      }
    });

    return () => {
      sim.stop();
    };
  }, [graph]);

  return (
    <div className="space-y-3">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-xl font-semibold">Mesh Topology</h1>
          <p className="text-sm text-slate-500">
            Services as hubs, instances as satellites. Color = health, size = weight. Dashed regions
            = zones.
          </p>
        </div>
        <div className="flex gap-3 text-xs font-mono text-slate-400">
          <span className="flex items-center gap-1">
            <i className="h-2 w-2 rounded-full bg-signal-green" /> passing
          </span>
          <span className="flex items-center gap-1">
            <i className="h-2 w-2 rounded-full bg-signal-amber" /> warning
          </span>
          <span className="flex items-center gap-1">
            <i className="h-2 w-2 rounded-full bg-signal-red" /> critical
          </span>
        </div>
      </div>
      {callError && (
        <div role="alert" className="rounded-lg border border-signal-red/40 bg-signal-red/10 px-4 py-2 text-sm text-signal-red">
          Call graph unavailable: {callError.message}
        </div>
      )}
      {callLoading && <div role="status" className="text-xs font-mono text-slate-500">Loading live call graph…</div>}
      <div className="rounded-xl border border-ink-600 bg-ink-900/60 overflow-hidden">
        <svg ref={ref} className="w-full h-[520px]" />
      </div>
      {!callLoading && !callError && graph.nodes.length === 0 && (
        <div className="text-xs font-mono text-slate-600">No live services or call edges reported.</div>
      )}
      {callEdges.length > 0 && (
        <div className="text-xs font-mono text-slate-500">
          Call edges: {callEdges.length} · line width = RPS · red = non-zero error rate
        </div>
      )}
    </div>
  );
}
