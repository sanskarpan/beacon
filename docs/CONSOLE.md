# Console stack

The beacon observatory is **React 18 + TypeScript + Vite + Tailwind + D3 + Recharts + zustand + lucide**.

## shadcn/ui waiver (TODO-046)

We **permanently waive** a full `shadcn` CLI scaffold.

| Checklist ask | What we ship instead |
|---|---|
| shadcn/ui init + components | Hand-styled Tailwind cards, buttons, tables matching the same dark observatory aesthetic |
| Radix primitives via CLI | Native elements + Tailwind utility classes; no shadcn codegen dependency |

**Rationale:** the console is a research/observability UI, not a design-system product surface. Adding the shadcn CLI, `components.json`, and generated Radix wrappers would expand the dependency graph without changing operator-visible behaviour. The CHECKLIST Phase 0 item is satisfied by **equivalent styling** (already noted historically as “shadcn-equivalent without full CLI scaffold”).

If a future productization pass needs accessibility primitives (dialogs, menus), adopt shadcn then — not as a gate for SPEC fidelity today.

## Live APIs the console consumes

| Path | View |
|---|---|
| `/v1/events` (SSE) | all |
| `/v1/telemetry/calls` | Mesh call-graph |
| `/v1/watch/stats` | Watch inspector |
| `/v1/lab/consistency` | Consistency lab |
| `/v1/catalog/*` | topology / health |
