import { createEcmaScriptPlugin, runNodeJs } from "@bufbuild/protoplugin";
import type { Schema } from "@bufbuild/protoplugin";
import type { DescFile, DescMethod } from "@bufbuild/protobuf";

function returnType(method: DescMethod): string {
  const i = method.input.name;
  const o = method.output.name;
  switch (method.methodKind) {
    case "unary":            return `UnaryCall<${o}>`;
    case "server_streaming": return `ServerStream<${o}>`;
    case "client_streaming": return `ClientStream<${i}, ${o}>`;
    case "bidi_streaming":   return `BidiStream<${i}, ${o}>`;
  }
}

function hasReqParam(method: DescMethod): boolean {
  return method.methodKind === "unary" || method.methodKind === "server_streaming";
}

function pbImportPath(file: DescFile): string {
  const basename = file.name.split("/").pop()!.replace(/\.proto$/, "");
  return `./${basename}_pb.js`;
}

function camelCase(name: string): string {
  return name.charAt(0).toLowerCase() + name.slice(1);
}

function methodPath(file: DescFile, serviceName: string, methodName: string): string {
  const pkg = file.proto.package;
  return `/${pkg ? pkg + "." : ""}${serviceName}/${methodName}`;
}

// Returns the lines of the method body (indented 4 spaces, without surrounding braces).
function methodBody(method: DescMethod, path: string): string[] {
  const i = method.input.name;
  const o = method.output.name;
  const iS = `${i}Schema`;
  const oS = `${o}Schema`;
  const p = JSON.stringify(path);

  switch (method.methodKind) {
    case "unary":
      return [
        `    return this.transport`,
        `      .openStream(${p})`,
        `      .unary(toBinary(${iS}, req), (b) => fromBinary(${oS}, b));`,
      ];
    case "server_streaming":
      return [
        `    return this.transport`,
        `      .openStream(${p})`,
        `      .serverStream(toBinary(${iS}, req), (b) => fromBinary(${oS}, b));`,
      ];
    case "client_streaming":
      return [
        `    return this.transport`,
        `      .openStream(${p})`,
        `      .clientStream(`,
        `        (req: ${i}) => toBinary(${iS}, req),`,
        `        (b) => fromBinary(${oS}, b),`,
        `      );`,
      ];
    case "bidi_streaming":
      return [
        `    return this.transport`,
        `      .openStream(${p})`,
        `      .bidiStream(`,
        `        (req: ${i}) => toBinary(${iS}, req),`,
        `        (b) => fromBinary(${oS}, b),`,
        `      );`,
      ];
  }
}

const plugin = createEcmaScriptPlugin({
  name: "protoc-gen-wsgrpc",
  version: "v0.1.0",
  generateTs(schema: Schema) {
    for (const file of schema.files) {
      if (file.services.length === 0) continue;

      const f = schema.generateFile(
        file.name.replace(/\.proto$/, "") + "_wsgrpc.ts",
      );

      f.preamble(file);

      // Collect unique message names and derive schema names.
      const msgNames = new Set<string>();
      for (const svc of file.services) {
        for (const method of svc.methods) {
          msgNames.add(method.input.name);
          msgNames.add(method.output.name);
        }
      }

      // Imports — fromBinary/toBinary always needed.
      f.print(`import { fromBinary, toBinary } from "@bufbuild/protobuf";`);
      f.print(`import type {`);
      f.print(`  BidiStream,`);
      f.print(`  ClientStream,`);
      f.print(`  ServerStream,`);
      f.print(`  UnaryCall,`);
      f.print(`  WsGrpcTransport,`);
      f.print(`} from "@grpcws/transport";`);

      // pb import: interleave type imports for message types and value imports for schemas.
      const pbParts: string[] = [];
      for (const name of [...msgNames].sort()) {
        pbParts.push(`  type ${name}`, `  ${name}Schema`);
      }
      f.print(`import {`);
      for (const part of pbParts) {
        f.print(`${part},`);
      }
      f.print(`} from "${pbImportPath(file)}";`);
      f.print();

      for (const svc of file.services) {
        f.print(`export class ${svc.name}Client {`);
        f.print(`  constructor(private readonly transport: WsGrpcTransport) {}`);
        f.print();

        for (const method of svc.methods) {
          const rt = returnType(method);
          const param = hasReqParam(method) ? `req: ${method.input.name}` : "";
          const path = methodPath(file, svc.name, method.name);
          const lines = methodBody(method, path);

          f.print(`  ${camelCase(method.name)}(${param}): ${rt} {`);
          for (const line of lines) {
            f.print(line);
          }
          f.print(`  }`);
          f.print();
        }

        f.print(`}`);
        f.print();
      }
    }
  },
});

runNodeJs(plugin);
