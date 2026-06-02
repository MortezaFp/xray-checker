# XRAY Batch Verifier

A concurrent xray proxy config tester that validates configs against Cloudflare and Google, then auto-pushes working results to GitHub for subscription use.

## Features

- Concurrent batch testing with configurable workers, timeout, and retries
- Supports vless://, vmess://, trojan://, and ss:// protocols
- Tests against Cloudflare HTTP + Google HTTPS for real traffic validation
- Auto-pushes valid configs to GitHub after each scan
- Outputs both human-readable and base64-encoded subscription files

## Usage

1. Place your proxy configs in `nodes.txt` (one per line)
2. (Optional) Add subscription URLs in `sub.txt`
3. Run the tester:

```bash
# Windows
xray-tester.exe

# Linux/Mac
go run main.go
```

4. Select workers, timeout, and retries when prompted
5. After the scan completes, choose whether to push results to GitHub

## Subscription URL

After running, use this URL in any v2ray client:

```
https://raw.githubusercontent.com/MortezaFp/xray-checker/master/valid_base64.txt
```

## Output Files

| File | Description |
|------|-------------|
| `valid.txt` | Plain text proxy configs (human-readable) |
| `valid_base64.txt` | Base64-encoded configs (for subscription use) |

## Requirements

- [Xray-core](https://github.com/XTLS/Xray-core) binary (`xray.exe` / `xray`) in the project directory
- Go 1.21+ (only if building from source)

## Building from Source

```bash
go build -o xray-tester.exe .
```

## How It Works

1. Loads configs from `nodes.txt` and subscription URLs in `sub.txt`
2. Deduplicates configs
3. Spawns concurrent workers to test each config
4. Each test starts a local xray SOCKS proxy and routes traffic through it
5. Validates by checking both Cloudflare and Google HTTP endpoints
6. Sorts working configs by response delay
7. Writes results and auto-commits/pushes to GitHub
