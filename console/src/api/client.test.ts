import { afterEach, describe, expect, it } from "bun:test";
import {
  ApiError,
  fetchWatchStats,
  listServices,
} from "./client";

const realFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("beacon API contracts", () => {
  it("decodes watcher stats from the live endpoint", async () => {
    let requested = "";
    globalThis.fetch = (async (input) => {
      requested = String(input);
      return new Response(
        JSON.stringify({
          total_watchers: 2,
          watchers: [{ service: "payments", id: 7, index: 12 }],
          cache: { oldest: 10, newest: 12, size: 3 },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }) as typeof fetch;

    const stats = await fetchWatchStats();

    expect(requested).toBe("/v1/watch/stats");
    expect(stats.total_watchers).toBe(2);
    expect(stats.watchers[0]).toEqual({ service: "payments", id: 7, index: 12 });
    expect(stats.cache?.size).toBe(3);
  });

  it("preserves structured API errors instead of returning empty data", async () => {
    globalThis.fetch = (async () =>
      new Response(JSON.stringify({ code: "catalog", message: "catalog unavailable" }), {
        status: 503,
        headers: { "Content-Type": "application/json" },
      })) as typeof fetch;

    let caught: unknown;
    try {
      await listServices();
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ApiError);
    if (caught instanceof ApiError) {
      expect(caught.status).toBe(503);
      expect(caught.code).toBe("catalog");
      expect(caught.message).toBe("catalog unavailable");
      expect(caught.path).toBe("/v1/catalog/services");
    }
  });
});
