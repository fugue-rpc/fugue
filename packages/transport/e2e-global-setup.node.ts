import { spawn } from "node:child_process";
import { createConnection } from "node:net";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const nodeEchoServerDir = resolve(__dirname, "../../examples/node-echo-server");

export async function setup() {
  const proc = spawn("pnpm", ["start"], {
    cwd: nodeEchoServerDir,
    stdio: ["ignore", "pipe", "pipe"],
    detached: false,
    shell: process.platform === "win32",
  });

  proc.stdout?.on("data", (d: Buffer) =>
    process.stdout.write(`[node-echo-server] ${d}`),
  );
  proc.stderr?.on("data", (d: Buffer) =>
    process.stderr.write(`[node-echo-server] ${d}`),
  );

  await waitForPort(8080, 20_000);

  return async () => {
    // On Windows, killing the shell process (cmd.exe) does not propagate to
    // its child processes. Use taskkill /T to terminate the whole tree.
    if (process.platform === "win32" && proc.pid != null) {
      const { execSync } = await import("node:child_process");
      try { execSync(`taskkill /F /T /PID ${proc.pid}`, { stdio: "ignore" }); } catch { /* already gone */ }
    } else {
      proc.kill();
    }
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
          reject(new Error(`node-echo-server did not open :${port} within ${timeoutMs}ms`));
        } else {
          setTimeout(attempt, 150);
        }
      });
    };
    attempt();
  });
}
