import { spawn } from "node:child_process";
import { createConnection } from "node:net";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const echoServerDir = resolve(__dirname, "../../examples/echo-server");

export async function setup() {
  const proc = spawn("go", ["run", "."], {
    cwd: echoServerDir,
    stdio: ["ignore", "pipe", "pipe"],
    // Ensure the child is killed when the parent exits on Windows.
    detached: false,
  });

  proc.stdout?.on("data", (d: Buffer) =>
    process.stdout.write(`[echo-server] ${d}`),
  );
  proc.stderr?.on("data", (d: Buffer) =>
    process.stderr.write(`[echo-server] ${d}`),
  );

  await waitForPort(8080, 20_000);

  return async () => {
    proc.kill();
  };
}

function waitForPort(port: number, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const attempt = () => {
      const sock = createConnection({ port, host: "localhost" });
      sock.once("connect", () => {
        sock.destroy();
        resolve();
      });
      sock.once("error", () => {
        sock.destroy();
        if (Date.now() >= deadline) {
          reject(new Error(`echo-server did not open :${port} within ${timeoutMs}ms`));
        } else {
          setTimeout(attempt, 150);
        }
      });
    };
    attempt();
  });
}
