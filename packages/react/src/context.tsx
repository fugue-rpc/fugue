import { createContext, useContext, type ReactNode } from "react";
import { FugueTransport } from "@fugue-rpc/transport";

const TransportContext = createContext<FugueTransport | null>(null);

export interface FugueProviderProps {
  transport: FugueTransport;
  children: ReactNode;
}

export function FugueProvider({ transport, children }: FugueProviderProps) {
  return (
    <TransportContext.Provider value={transport}>
      {children}
    </TransportContext.Provider>
  );
}

export function useTransport(): FugueTransport {
  const t = useContext(TransportContext);
  if (!t) throw new Error("useTransport must be used inside <FugueProvider>");
  return t;
}
