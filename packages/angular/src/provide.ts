import { InjectionToken, makeEnvironmentProviders } from "@angular/core";
import type { EnvironmentProviders } from "@angular/core";
import type { FugueTransport } from "@fugue-rpc/transport";

export const FUGUE_TRANSPORT = new InjectionToken<FugueTransport>("FugueTransport");

export function provideFugue(transport: FugueTransport): EnvironmentProviders {
  return makeEnvironmentProviders([
    { provide: FUGUE_TRANSPORT, useValue: transport },
  ]);
}
