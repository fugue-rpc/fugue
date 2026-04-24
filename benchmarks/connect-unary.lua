-- connect-unary.lua — wrk script for Connect-protocol unary echo benchmark.
--
-- Usage (from WSL with wrk installed):
--   wrk -t4 -c100 -d30s --latency \
--       -s benchmarks/connect-unary.lua \
--       http://$(ip route show default | awk '/default/{print $3}'):8090/echo.v1.Echo/Echo
--
-- The Windows host IP is needed because WSL can't use localhost to reach
-- a Windows process. Use `ip route show default` to find the gateway IP.
--
-- The request body is Msg{value:"stress"} (proto field 1, string "stress"):
--   proto bytes: \x0a\x06stress  (8 bytes, no envelope for unary)
--
-- Byte breakdown:
--   \10  proto tag byte: field 1, wire type 2 (length-delimited)
--   \6   string length: 6 bytes
--   stress  UTF-8 payload

local body = "\10\6stress"

wrk.method = "POST"
wrk.body   = body
wrk.headers["Content-Type"] = "application/proto"

function request()
    return wrk.format("POST", nil, nil, body)
end
