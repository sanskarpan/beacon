# Console

**Stack:** React 18 + TypeScript + Vite + Tailwind + D3 + Recharts + zustand + lucide + SSE.

## UI System (Tailwind, shadcn-equivalent)

We ship hand-styled Tailwind cards/buttons/tables matching the shadcn dark observatory aesthetic without the `shadcn` CLI codegen. Components are in `console/src/components/ui` (Button, Card, Table, Badge) — same tokens as `shadcn/ui` (Radix primitives would be added for dialogs/menus if needed). See `CONTRIBUTING.md` for adding a new component.

Build:

```bash
cd console && bun install --frozen-lockfile && bun run build # tsc + vite
bun run lint && bun run typecheck
```

## Live APIs the console consumes

| Path | View |
|---|---|
| `/v1/events` (SSE) | all |
| `/v1/telemetry/calls` | Mesh call-graph |
| `/v1/watch/stats` | Watch inspector |
| `/v1/lab/consistency` | Consistency lab |
| `/v1/catalog/*` | topology / health |
