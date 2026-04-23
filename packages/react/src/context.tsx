import { createContext, useContext, type ReactNode } from "react";
import { WsGrpcTransport } from "@grpcws/transport";

const TransportContext = createContext<WsGrpcTransport | null>(null);

export interface WsGrpcProviderProps {
  transport: WsGrpcTransport;
  children: ReactNode;
}

export function WsGrpcProvider({ transport, children }: WsGrpcProviderProps) {
  return (
    <TransportContext.Provider value={transport}>
      {children}
    </TransportContext.Provider>
  );
}

export function useTransport(): WsGrpcTransport {
  const t = useContext(TransportContext);
  if (!t) throw new Error("useTransport must be used inside <WsGrpcProvider>");
  return t;
}
