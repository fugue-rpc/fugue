// FugueServer — HTTP upgrade handler for fugue connections.

import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { type WebSocket, WebSocketServer } from "ws";

// Structural interface so attach() accepts both http.Server and https.Server.
interface UpgradeableServer {
  on(event: "upgrade", listener: (req: IncomingMessage, socket: Duplex, head: Buffer) => void): this;
}

import {
  ServiceRegistry,
  type ServiceDefinition,
  type ServiceImplementation,
} from "./service.js";
import { FugueConn, type ConnOptions } from "./conn.js";

export interface ServerOptions extends ConnOptions {
  /**
   * Allowed WebSocket origin(s). Pass `"*"` to permit all origins (dev only).
   * When absent, requests that carry an `Origin` header (browser clients) are
   * rejected with 403; requests without `Origin` (non-browser clients) pass.
   * When set to a list, non-browser clients (no `Origin`) always pass, and
   * browser clients are checked against the list.
   */
  origins?: string | string[];
}

export class FugueServer {
  private readonly _registry = new ServiceRegistry();
  private readonly _wss: WebSocketServer;
  private readonly _origins: string[] | null;
  private readonly _connOptions: ConnOptions;
  private readonly _connections = new Set<WebSocket>();
  private _closed = false;

  constructor(options?: ServerOptions) {
    const { origins, ...connOptions } = options ?? {};
    this._connOptions = connOptions;
    this._origins = origins != null
      ? (Array.isArray(origins) ? origins : [origins])
      : null;
    this._wss = new WebSocketServer({ noServer: true });
  }

  addService(
    definition: ServiceDefinition,
    implementation: ServiceImplementation,
  ): this {
    this._registry.addService(definition, implementation);
    return this;
  }

  /**
   * Attach to an existing Node.js HTTP/HTTPS server.
   * Registers an `upgrade` event handler that handles WebSocket upgrades.
   * Pass `path` to restrict handling to a specific URL prefix (e.g. `/wsgrpc/`).
   */
  attach(server: UpgradeableServer, path?: string): this {
    server.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
      if (this._closed) { socket.destroy(); return; }
      if (path && !req.url?.startsWith(path)) { socket.destroy(); return; }
      if (!this._checkOrigin(req)) {
        socket.write("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n403 Forbidden: Origin not allowed");
        socket.destroy();
        return;
      }
      this._wss.handleUpgrade(req, socket, head, (ws) => {
        this._connections.add(ws);
        ws.once("close", () => this._connections.delete(ws));
        new FugueConn(ws, (p) => this._registry.lookup(p), this._connOptions).serve();
      });
    });
    return this;
  }

  /**
   * Close all active connections and stop accepting new ones.
   *
   * Sends WebSocket close(1001) to every open connection and resolves when
   * all have closed. Connections that have not closed within `timeoutMs`
   * (default 5 000 ms) are force-terminated.
   */
  close(timeoutMs = 5_000): Promise<void> {
    this._closed = true;
    if (this._connections.size === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      let remaining = this._connections.size;
      const onClose = () => { if (--remaining === 0) resolve(); };
      for (const ws of this._connections) {
        ws.once("close", onClose);
        ws.close(1001, "server shutting down");
      }
      const timer = setTimeout(() => {
        for (const ws of this._connections) ws.terminate();
      }, timeoutMs);
      // Don't keep the event loop alive just for the timeout.
      if (typeof timer.unref === "function") timer.unref();
    });
  }

  private _checkOrigin(req: IncomingMessage): boolean {
    const origin = req.headers["origin"] as string | undefined;
    if (this._origins === null) {
      // Default policy: block browser origins (has Origin header), allow non-browser.
      return !origin;
    }
    if (this._origins.includes("*")) {
      return true;
    }
    // Specific allow-list: non-browser (no Origin) always passes.
    if (!origin) return true;
    return this._origins.includes(origin);
  }
}
