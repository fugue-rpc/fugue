import { DestroyRef, Injectable, inject, signal } from "@angular/core";
import { toObservable } from "@angular/core/rxjs-interop";
import type { Signal } from "@angular/core";
import type { Observable } from "rxjs";
import { GrpcStatusError } from "@fugue-rpc/transport";
import type { ServerStream } from "@fugue-rpc/transport";

export type ServerStreamState<Res> =
  | { status: "idle" }
  | { status: "streaming"; messages: Res[] }
  | { status: "done"; messages: Res[] }
  | { status: "error"; messages: Res[]; error: GrpcStatusError | Error };

@Injectable()
export class FugueServerStreamService<Req = unknown, Res = unknown> {
  private readonly _state = signal<ServerStreamState<Res>>({ status: "idle" });
  private _cancelled = false;
  private _stream: ServerStream<Res> | null = null;

  readonly state: Signal<ServerStreamState<Res>> = this._state.asReadonly();
  readonly state$: Observable<ServerStreamState<Res>>;

  constructor() {
    this.state$ = toObservable(this._state);
    inject(DestroyRef).onDestroy(() => this._cancelStream());
  }

  start(factory: (req: Req) => ServerStream<Res>, req: Req): void {
    this._cancelStream();
    this._cancelled = false;
    this._stream = factory(req);
    this._state.set({ status: "streaming", messages: [] });
    this._consume(this._stream);
  }

  cancel(): void {
    this._cancelStream();
  }

  reset(): void {
    this._cancelStream();
    this._state.set({ status: "idle" });
  }

  private async _consume(stream: ServerStream<Res>): Promise<void> {
    const msgs: Res[] = [];
    try {
      for await (const msg of stream) {
        if (this._cancelled) return;
        msgs.push(msg);
        this._state.set({ status: "streaming", messages: [...msgs] });
      }
      if (!this._cancelled) {
        this._state.set({ status: "done", messages: msgs });
      }
    } catch (err) {
      if (!this._cancelled) {
        this._state.set({
          status: "error",
          messages: msgs,
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    }
  }

  private _cancelStream(): void {
    this._cancelled = true;
    this._stream?.cancel();
    this._stream = null;
  }
}
