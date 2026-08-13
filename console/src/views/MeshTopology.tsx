import { useEffect, useMemo, useRef } from "react";
import * as d3 from "d3";
import { useEventStore } from "../store/events";

type Node = d3.SimulationNodeDatum & {
  id: string;
  kind: "service" | "instance";
  health?: string;
  weight?: number;
  zone?: string;
  label: string;
};

type Link = d3.SimulationLinkDatum<Node> & { source: string | Node; target: string | Node };

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

  const graph = useMemo(() => {
    const nodes: Node[] = [];
    const links: Link[] = [];
    for (const name of Object.keys(services)) {
      nodes.push({ id: `svc:${name}`, kind: "service", label: name });
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
        links.push({ source: `svc:${name}`, target: id });
      }
    }
    // demo seed when empty
    if (nodes.length === 0) {
      ["payments", "web", "db"].forEach((s, i) => {
        nodes.push({ id: `svc:${s}`, kind: "service", label: s });
        for (let j = 0; j < 3; j++) {
          const id = `inst:${s}-${j}`;
          nodes.push({
            id,
            kind: "instance",
            label: `${s}-${j}`,
            health: j === 2 && i === 0 ? "critical" : "passing",
            weight: j === 0 ? 3 : 1,
            zone: j % 2 === 0 ? "us-east-1a" : "us-east-1b",
          });
          links.push({ source: `svc:${s}`, target: id });
        }
      });
    }
    return { nodes, links };
  }, [services, instances]);

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
          .distance((d) => ((d.source as Node).kind === "service" ? 70 : 40))
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
      .attr("stroke", "#2a3544")
      .attr("stroke-width", 1.2);

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
      <div className="rounded-xl border border-ink-600 bg-ink-900/60 overflow-hidden">
        <svg ref={ref} className="w-full h-[520px]" />
      </div>
    </div>
  );
}
