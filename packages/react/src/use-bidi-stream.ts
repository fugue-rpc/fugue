import { useCallback, useEffect, useRef, useState } from "react";
import type { BidiStream } from "@grpcws/transport";
import { GrpcStatusError } from "@grpcws/transport";

export type BidiStreamState<Res> =
  | { status: "idle" }
  | { status: "open"; messages: Res[] }
  | { status: "done"; messages: Res[] }
  | { status: "error"; messages: Res[]; error: GrpcStatusError | Error };

export interface UseBidiStreamResult<Req, Res> {
  state: BidiStreamState<Res>;
  open(): void;
  send(req: Req): void;
  halfClose(): void;
  cancel(): void;
  reset(): void;
}

/**
 * Manages a bidirectional-streaming RPC.
 *
 * @param call - Factory that opens a bidi stream (no request argument).
 */
export function useBidiStream<Req, Res>(
  call: () => BidiStream<Req, Res>,
): UseBidiStreamResult<Req, Res> {
  const [state, setState] = useState<BidiStreamState<Res>>({ status: "idle" });
  const streamRef = useRef<BidiStream<Req, Res> | null>(null);
  const cancelledRef = useRef(false);

  const cancel = useCallback(() => {
    if (streamRef.current) {
      streamRef.current.cancel();
      streamRef.current = null;
    }
    cancelledRef.current = true;
  }, []);

  const open = useCallback(() => {
    cancel();
    cancelledRef.current = false;
    const stream = call();
    streamRef.current = stream;
    setState({ status: "open", messages: [] });

    (async () => {
      const msgs: Res[] = [];
      try {
        for await (const msg of stream) {
          if (cancelledRef.current) return;
          msgs.push(msg);
          setState({ status: "open", messages: [...msgs] });
        }
        if (!cancelledRef.current) {
          setState({ status: "done", messages: msgs });
        }
      } catch (err: unknown) {
        if (!cancelledRef.current) {
          setState({
            status: "error",
            messages: msgs,
            error: err instanceof Error ? err : new Error(String(err)),
          });
        }
      }
    })();
  }, [call, cancel]);

  const send = useCallback((req: Req) => {
    streamRef.current?.send(req);
  }, []);

  const halfClose = useCallback(() => {
    streamRef.current?.halfClose();
  }, []);

  const reset = useCallback(() => {
    cancel();
    setState({ status: "idle" });
  }, [cancel]);

  useEffect(() => {
    return () => {
      cancel();
    };
  }, [cancel]);

  return { state, open, send, halfClose, cancel, reset };
}
