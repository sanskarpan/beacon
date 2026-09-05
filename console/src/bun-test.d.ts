declare module "bun:test" {
  type TestFn = () => void | Promise<void>;
  type Matchers<T> = {
    toBe(expected: unknown): void;
    toEqual(expected: unknown): void;
    toBeInstanceOf(expected: new (...args: never[]) => unknown): void;
  };

  export function afterEach(fn: TestFn): void;
  export function describe(name: string, fn: () => void): void;
  export function it(name: string, fn: TestFn): void;
  export function expect<T>(actual: T): Matchers<T>;
}
