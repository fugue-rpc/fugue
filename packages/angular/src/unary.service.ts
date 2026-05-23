import { DestroyRef, Injectable, inject, signal } from "@angular/core";
import { toObservable } from "@angular/core/rxjs-interop";
import type { Signal } from "@angular/core";
import type { Observable } from "rxjs";
import { GrpcStatusError } from "@fugue-rpc/transport";
import type { UnaryCall } from "@fugue-rpc/transport";

export type UnaryState<Res> =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "success"; data: Res }
  | { status: "error"; error: GrpcStatusError | Error };

@Injectable()
export class FugueUnaryService<Req = unknown, Res = unknown> {
  private readonly _state = signal<UnaryState<Res>>({ status: "idle" });
  private _cancelCurrent: (() => void) | null = null;

  readonly state: Signal<UnaryState<Res>> = this._state.asReadonly();
  readonly state$: Observable<UnaryState<Res>>;

  constructor() {
    this.state$ = toObservable(this._state);
    inject(DestroyRef).onDestroy(() => this._cancel());
  }

  execute(factory: (req: Req) => UnaryCall<Res>, req: Req): void {
    this._cancel();
    this._state.set({ status: "loading" });

    let cancelled = false;
    const call = factory(req);
    this._cancelCurrent = () => { cancelled = true; call.cancel(); };

    Promise.resolve(call).then(
      (data) => { if (!cancelled) this._state.set({ status: "success", data }); },
      (err: unknown) => {
        if (!cancelled) {
          this._state.set({
            status: "error",
            error: err instanceof Error ? err : new Error(String(err)),
          });
        }
      },
    );
  }

  reset(): void {
    this._cancel();
    this._state.set({ status: "idle" });
  }

  private _cancel(): void {
    this._cancelCurrent?.();
    this._cancelCurrent = null;
  }
}
