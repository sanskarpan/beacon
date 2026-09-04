import { create } from "zustand";

export type BeaconEvent = {
  kind: string;
  timestamp: string;
  trace_id?: string;
  node?: string;
  service?: string;
  instance?: string;
  index?: number;
  from?: string;
  to?: string;
  detail?: string;
  elapsed?: number;
  adds?: number;
  removes?: number;
  updates?: number;
};

const MAX = 2000;

type State = {
  events: BeaconEvent[];
  live: boolean;
  connected: boolean;
  services: Record<string, string[]>;
  instances: Record<string, Instance[]>;
  setLive: (v: boolean) => void;
  setConnected: (v: boolean) => void;
  push: (ev: BeaconEvent) => void;
  setServices: (s: Record<string, string[]>) => void;
  setInstances: (name: string, list: Instance[]) => void;
  clear: () => void;
};

export type Instance = {
  id: string;
  service: string;
  node: string;
  address: string;
  port: number;
  health: string;
  weight: number;
  tags?: string[];
  locality?: { region?: string; zone?: string };
};

export const useEventStore = create<State>((set) => ({
  events: [],
  live: true,
  connected: false,
  services: {},
  instances: {},
  setLive: (live) => set({ live }),
  setConnected: (connected) => set({ connected }),
  push: (ev) =>
    set((s) => {
      if (!s.live) return s;
      // O(1) amortized: push to end and drop oldest if over cap (no prepend copy)
      const events = s.events;
      events.push(ev);
      if (events.length > MAX) {
        // remove oldest (at 0) — single shift is cheaper than full copy of 2000 on every prepend
        events.shift();
      }
      return { events: [...events] };
    }),
  setServices: (services) => set({ services }),
  setInstances: (name, list) =>
    set((s) => ({ instances: { ...s.instances, [name]: list } })),
  clear: () => set({ events: [] }),
}));
