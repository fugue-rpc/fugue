import { useCallback, useEffect, useRef, useState } from "react";
import type { UnaryCall } from "@grpcws/transport";
import { GrpcStatusError } from "@grpcws/transport";

export type UnaryState<Res> =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "success"; data: Res }
  | { status: "error"; error: GrpcStatusError | Error };

export interface UseUnaryResult<Req, Res> {
  state: UnaryState<Res>;
  execute(req: Req): void;
  reset(): void;
}

/**
 * Manages a single unary RPC call. Call `execute(req)` to fire it; the hook
 * transitions idle → loading → success | error.
 *
 * @param call - Factory that opens a unary stream given a request.
 */
export function useUnary<Req, Res>(
  call: (req: Req) => UnaryCall<Res>,
): UseUnaryResult<Req, Res> {
  const [state, setState] = useState<UnaryState<Res>>({ status: "idle" });
  const cancelRef = useRef<(() => void) | null>(null);

  const execute = useCallback(
    (req: Req) => {
      cancelRef.current?.();
      setState({ status: "loading" });

      let cancelled = false;
      const unary = call(req);
      cancelRef.current = () => {
        cancelled = true;
        unary.cancel();
      };

      Promise.resolve(unary).then(
        (data) => {
          if (!cancelled) setState({ status: "success", data });
        },
        (err: unknown) => {
          if (!cancelled) {
            setState({
              status: "error",
              error: err instanceof Error ? err : new Error(String(err)),
            });
          }
        },
      );
    },
    [call],
  );

  const reset = useCallback(() => {
    cancelRef.current?.();
    cancelRef.current = null;
    setState({ status: "idle" });
  }, []);

  useEffect(() => {
    return () => {
      cancelRef.current?.();
    };
  }, []);

  return { state, execute, reset };
}
