import { createEcmaScriptPlugin, runNodeJs } from "@bufbuild/protoplugin";
// Maps method.methodKind to the @grpcws/transport interface name.
function returnType(method) {
    const i = method.input.name;
    const o = method.output.name;
    switch (method.methodKind) {
        case "unary":
            return `UnaryCall<${o}>`;
        case "server_streaming":
            return `ServerStream<${o}>`;
        case "client_streaming":
            return `ClientStream<${i}, ${o}>`;
        case "bidi_streaming":
            return `BidiStream<${i}, ${o}>`;
    }
}
// Methods that take a request argument (unary and server-streaming).
function hasReqParam(method) {
    return method.methodKind === "unary" || method.methodKind === "server_streaming";
}
function pbImportPath(file) {
    const basename = file.name.split("/").pop().replace(/\.proto$/, "");
    return `./${basename}_pb.js`;
}
function camelCase(name) {
    return name.charAt(0).toLowerCase() + name.slice(1);
}
function methodPath(file, serviceName, methodName) {
    const pkg = file.proto.package;
    return `/${pkg ? pkg + "." : ""}${serviceName}/${methodName}`;
}
function openStreamCall(method, path) {
    switch (method.methodKind) {
        case "unary":
            return `return this.transport.openStream(${JSON.stringify(path)}).unary<${method.input.name}, ${method.output.name}>(req);`;
        case "server_streaming":
            return `return this.transport.openStream(${JSON.stringify(path)}).serverStream<${method.input.name}, ${method.output.name}>(req);`;
        case "client_streaming":
            return `return this.transport.openStream(${JSON.stringify(path)}).clientStream<${method.input.name}, ${method.output.name}>();`;
        case "bidi_streaming":
            return `return this.transport.openStream(${JSON.stringify(path)}).bidiStream<${method.input.name}, ${method.output.name}>();`;
    }
}
const plugin = createEcmaScriptPlugin({
    name: "protoc-gen-wsgrpc",
    version: "v0.1.0",
    generateTs(schema) {
        for (const file of schema.files) {
            if (file.services.length === 0)
                continue;
            const f = schema.generateFile(file.name.replace(/\.proto$/, "") + "_wsgrpc.ts");
            f.preamble(file);
            // Collect all unique message types referenced by this file's services.
            const msgNames = new Set();
            for (const svc of file.services) {
                for (const method of svc.methods) {
                    msgNames.add(method.input.name);
                    msgNames.add(method.output.name);
                }
            }
            // Collect which transport types are needed.
            const transportTypes = new Set();
            for (const svc of file.services) {
                for (const method of svc.methods) {
                    switch (method.methodKind) {
                        case "unary":
                            transportTypes.add("UnaryCall");
                            break;
                        case "server_streaming":
                            transportTypes.add("ServerStream");
                            break;
                        case "client_streaming":
                            transportTypes.add("ClientStream");
                            break;
                        case "bidi_streaming":
                            transportTypes.add("BidiStream");
                            break;
                    }
                }
            }
            f.print(`import type { WsGrpcTransport, ${[...transportTypes].join(", ")} } from "@grpcws/transport";`);
            f.print(`import type { ${[...msgNames].join(", ")} } from "${pbImportPath(file)}";`);
            f.print();
            for (const svc of file.services) {
                f.print(`export class ${svc.name}Client {`);
                f.print(`  constructor(private readonly transport: WsGrpcTransport) {}`);
                f.print();
                for (const method of svc.methods) {
                    const rt = returnType(method);
                    const param = hasReqParam(method) ? `req: ${method.input.name}` : "";
                    const path = methodPath(file, svc.name, method.name);
                    const call = openStreamCall(method, path);
                    f.print(`  ${camelCase(method.name)}(${param}): ${rt} {`);
                    f.print(`    ${call}`);
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
//# sourceMappingURL=index.js.map