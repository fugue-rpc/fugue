import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { create } from "@bufbuild/protobuf";
import { FugueTransport } from "@fugue-rpc/transport";
import { FugueProvider } from "@fugue-rpc/react";
import { useUnary } from "@fugue-rpc/react";
import { useServerStream } from "@fugue-rpc/react";
import { useBidiStream } from "@fugue-rpc/react";
import { MsgSchema } from "@gen/echo/v1/echo_pb.js";
import { EchoClient } from "@gen/echo/v1/echo_fugue.js";

// Connect via Vite proxy (/fugue → ws://localhost:8080/fugue).
const transport = new FugueTransport("/fugue/");

function msg(value: string) {
  return create(MsgSchema, { value });
}

// ── Transport status bar ──────────────────────────────────────────────────────
function StatusBar() {
  const [state, setState] = useState(transport.state);
  useEffect(() => {
    const id = setInterval(() => setState(transport.state), 500);
    return () => clearInterval(id);
  }, []);
  return (
    <div className="status-bar">
      <div className={`dot ${state === "open" ? "open" : "closed"}`} />
      transport: {state}
    </div>
  );
}

// ── Unary ─────────────────────────────────────────────────────────────────────
function UnaryPanel({ client }: { client: EchoClient }) {
  const [input, setInput] = useState("hello");
  const call = useCallback((req: string) => client.echo(msg(req)), [client]);
  const { state, execute, reset } = useUnary(call);

  return (
    <div className="card">
      <h2><span className="badge">Unary</span></h2>
      <div className="row">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && execute(input)}
          placeholder="message"
        />
        <button onClick={() => execute(input)} disabled={state.status === "loading"}>
          Echo
        </button>
        <button onClick={reset} disabled={state.status === "idle"}>
          Reset
        </button>
      </div>
      <div className="log">
        {state.status === "idle" && <div className="entry">Waiting…</div>}
        {state.status === "loading" && <div className="entry">Loading…</div>}
        {state.status === "success" && (
          <div className="entry recv">← {state.data.value}</div>
        )}
        {state.status === "error" && (
          <div className="entry err">Error: {state.error.message}</div>
        )}
      </div>
    </div>
  );
}

// ── Server streaming ──────────────────────────────────────────────────────────
function ServerStreamPanel({ client }: { client: EchoClient }) {
  const [input, setInput] = useState("ping");
  const call = useCallback((req: string) => client.echoStream(msg(req)), [client]);
  const { state, start, cancel, reset } = useServerStream(call);

  const messages = state.status !== "idle" ? state.messages : [];

  return (
    <div className="card">
      <h2><span className="badge">Server stream</span></h2>
      <div className="row">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && start(input)}
          placeholder="message"
        />
        <button onClick={() => start(input)} disabled={state.status === "streaming"}>
          Start
        </button>
        <button onClick={cancel} disabled={state.status !== "streaming"}>
          Cancel
        </button>
        <button onClick={reset}>Reset</button>
      </div>
      <div className="log">
        {messages.length === 0 && <div className="entry">Waiting…</div>}
        {messages.map((m, i) => (
          <div key={i} className="entry recv">← {m.value}</div>
        ))}
        {state.status === "done" && <div className="entry">Stream closed</div>}
        {state.status === "error" && (
          <div className="entry err">Error: {state.error.message}</div>
        )}
      </div>
    </div>
  );
}

// ── Bidi streaming ────────────────────────────────────────────────────────────
function BidiPanel({ client }: { client: EchoClient }) {
  const [input, setInput] = useState("");
  const logRef = useRef<HTMLDivElement>(null);

  const call = useCallback(() => client.echoBidi(), [client]);
  const { state, open, send, halfClose, cancel, reset } = useBidiStream(call);

  const messages = state.status !== "idle" ? state.messages : [];

  // Auto-scroll log.
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [messages]);

  function handleSend() {
    if (!input.trim()) return;
    send(msg(input));
    setInput("");
  }

  const isOpen = state.status === "open";

  return (
    <div className="card">
      <h2><span className="badge">Bidi stream</span></h2>
      <div className="row">
        <button onClick={() => open()} disabled={isOpen}>Connect</button>
        <button onClick={halfClose} disabled={!isOpen}>Half-close</button>
        <button className="danger" onClick={cancel} disabled={!isOpen}>Cancel</button>
        <button onClick={reset}>Reset</button>
      </div>
      <div className="row">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSend()}
          placeholder="type a message…"
          disabled={!isOpen}
        />
        <button onClick={handleSend} disabled={!isOpen}>Send</button>
      </div>
      <div className="log" ref={logRef}>
        {messages.length === 0 && state.status === "idle" && (
          <div className="entry">Click Connect to open a stream.</div>
        )}
        {messages.length === 0 && state.status === "open" && (
          <div className="entry">Stream open — type a message.</div>
        )}
        {messages.map((m, i) => (
          <div key={i} className="entry recv">← {m.value}</div>
        ))}
        {state.status === "done" && <div className="entry">Stream closed by server.</div>}
        {state.status === "error" && (
          <div className="entry err">Error: {state.error.message}</div>
        )}
      </div>
    </div>
  );
}

// ── Client streaming ──────────────────────────────────────────────────────────
function ClientStreamPanel({ client }: { client: EchoClient }) {
  const [input, setInput] = useState("");
  const [sent, setSent] = useState<string[]>([]);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const streamRef = useRef<ReturnType<EchoClient["echoCollect"]> | null>(null);

  function startStream() {
    if (streamRef.current) return;
    setSent([]);
    setResult(null);
    setError(null);
    streamRef.current = client.echoCollect();
  }

  function sendChunk() {
    if (!streamRef.current || !input.trim()) return;
    streamRef.current.send(msg(input));
    setSent((p) => [...p, input]);
    setInput("");
  }

  async function finish() {
    if (!streamRef.current) return;
    try {
      const res = await streamRef.current.closeAndReceive();
      setResult(res.value);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      streamRef.current = null;
    }
  }

  const isStreaming = streamRef.current !== null;

  return (
    <div className="card">
      <h2><span className="badge">Client stream</span></h2>
      <div className="row">
        <button onClick={startStream} disabled={isStreaming}>Open</button>
        <button onClick={finish} disabled={!isStreaming}>Close &amp; Receive</button>
      </div>
      <div className="row">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && sendChunk()}
          placeholder="chunk…"
          disabled={!isStreaming}
        />
        <button onClick={sendChunk} disabled={!isStreaming}>Send</button>
      </div>
      <div className="log">
        {sent.length === 0 && !result && <div className="entry">Open a stream, send chunks, then close.</div>}
        {sent.map((s, i) => <div key={i} className="entry sent">→ {s}</div>)}
        {result !== null && <div className="entry recv">← server joined: "{result}"</div>}
        {error && <div className="entry err">Error: {error}</div>}
      </div>
    </div>
  );
}

// ── Root ──────────────────────────────────────────────────────────────────────
export default function App() {
  const client = useMemo(() => new EchoClient(transport), []);

  return (
    <FugueProvider transport={transport}>
      <h1>fugue demo</h1>
      <p className="subtitle">
        All four gRPC RPC kinds over one WebSocket.
        Start the echo server: <code>cd examples/echo-server &amp;&amp; go run .</code>
      </p>
      <StatusBar />
      <br />
      <div className="grid">
        <UnaryPanel client={client} />
        <ServerStreamPanel client={client} />
        <ClientStreamPanel client={client} />
        <BidiPanel client={client} />
      </div>
    </FugueProvider>
  );
}
