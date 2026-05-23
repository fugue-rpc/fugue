import { DestroyRef, Injectable, inject, signal } from "@angular/core";
import { toObservable } from "@angular/core/rxjs-interop";
import type { Signal } from "@angular/core";
import type { Observable } from "rxjs";
import { GrpcStatusError } from "@fugue-rpc/transport";
import type { BidiStream } from "@fugue-rpc/transport";

export type BidiStreamState<Res> =
  | { status: "idle" }
  | { status: "open"; messages: Res[] }
  | { status: "done"; messages: Res[] }
  | { status: "error"; messages: Res[]; error: GrpcStatusError | Error };

@Injectable()
export class FugueBidiStreamService<Req = unknown, Res = unknown> {
  private readonly _state = signal<BidiStreamState<Res>>({ status: "idle" });
  private _cancelled = false;
  private _stream: BidiStream<Req, Res> | null = null;

  readonly state: Signal<BidiStreamState<Res>> = this._state.asReadonly();
  readonly state$: Observable<BidiStreamState<Res>>;

  constructor() {
    this.state$ = toObservable(this._state);
    inject(DestroyRef).onDestroy(() => this._cancelStream());
  }

  open(factory: () => BidiStream<Req, Res>, initialRequest?: Req): void {
    if (this._stream !== null) return;
    this._cancelled = false;
    this._stream = factory();
    if (initialRequest !== undefined) {
      this._stream.send(initialRequest);
    }
    this._state.set({ status: "open", messages: [] });
    this._consume(this._stream);
  }

  send(req: Req): void {
    this._stream?.send(req);
  }

  halfClose(): void {
    this._stream?.halfClose();
  }

  cancel(): void {
    this._cancelStream();
  }

  reset(): void {
    this._cancelStream();
    this._state.set({ status: "idle" });
  }

  private async _consume(stream: BidiStream<Req, Res>): Promise<void> {
    const msgs: Res[] = [];
    try {
      for await (const msg of stream) {
        if (this._cancelled) return;
        msgs.push(msg);
        this._state.set({ status: "open", messages: [...msgs] });
      }
      if (!this._cancelled) {
        this._stream = null;
        this._state.set({ status: "done", messages: msgs });
      }
    } catch (err) {
      if (!this._cancelled) {
        this._stream = null;
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
