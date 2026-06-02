package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/term"
)

type ConfigResult struct {
	config string
	delay  time.Duration
}

func main() {
	defer promptExit()

	inputFile := "nodes.txt"
	outputFile := "valid.txt"
	subFile := "sub.txt"

	binName := "xray.exe"
	if runtime.GOOS != "windows" {
		binName = "xray"
	}

	xrayBin, err := filepath.Abs(binName)
	if err != nil {
		fmt.Printf("[-] Error resolving absolute path: %v\n", err)
		return
	}

	if _, err := os.Stat(xrayBin); os.IsNotExist(err) {
		fmt.Printf("[-] Error: Could not find '%s' in this folder!\n", binName)
		fmt.Println("Please make sure the official xray core file is sitting next to this script.")
		return
	}

	// --- Read nodes.txt ---
	configs := readConfigsFromFile(inputFile)
	fmt.Printf("[*] Loaded %d configs from %s\n", len(configs), inputFile)

	// --- Read sub.txt and fetch subscription URLs ---
	if _, err := os.Stat(subFile); err == nil {
		subConfigs := fetchSubConfigs(subFile)
		fmt.Printf("[*] Fetched %d configs from subscription URLs\n", len(subConfigs))
		configs = append(configs, subConfigs...)
	} else {
		fmt.Println("[*] No sub.txt found, skipping subscription fetch")
	}

	// Deduplicate
	seen := make(map[string]bool)
	unique := make([]string, 0, len(configs))
	for _, c := range configs {
		if !seen[c] {
			seen[c] = true
			unique = append(unique, c)
		}
	}
	configs = unique
	total := len(configs)

	if total == 0 {
		fmt.Printf("[-] Error: No vless://, trojan://, vmess://, or ss:// proxies discovered.\n")
		return
	}

	clearScreen()
	fmt.Printf("\x1b[36m\x1b[1m%s\x1b[0m\n", " XRAY CONCURRENT BATCH VERIFIER v3.0")
	fmt.Printf(" \x1b[90mConfigs loaded:\x1b[0m %d\n", total)
	fmt.Println()

	// --- Interactive Runtime Configuration ---
	reader := bufio.NewReader(os.Stdin)

	numWorkers := promptChoice(reader, "Workers", []choice{
		{"15 (balanced)", 15}, {"50 (fast)", 50}, {"100 (aggressive)", 100}}, 0)
	fmt.Printf("\x1b[6A\x1b[J")
	timeoutSec := promptChoice(reader, "Timeout", []choice{
		{"2s (quick)", 2}, {"4s (balanced)", 4}, {"8s (slow)", 8}}, 1)
	fmt.Printf("\x1b[6A\x1b[J")
	retries := promptChoice(reader, "Retries", []choice{
		{"1 (no retry)", 1}, {"3 (balanced)", 3}, {"5 (forgiving)", 5}}, 1)
	fmt.Printf("\x1b[6A\x1b[J")
	testMode := promptChoice(reader, "Test targets", []choice{
		{"Cloudflare + Google (both)", 3}, {"Cloudflare only", 1}, {"Google only", 2}}, 0)

	// --- Launch ---
	clearScreen()
	targetNames := map[int]string{1: "Cloudflare HTTP", 2: "Google HTTPS", 3: "Cloudflare HTTP + Google HTTPS"}
	fmt.Printf("\x1b[36m\x1b[1m XRAY BATCH VERIFIER\x1b[0m\n")
	fmt.Printf(" \x1b[90mWorkers:\x1b[0m %d  \x1b[90mTimeout:\x1b[0m %ds  \x1b[90mRetries:\x1b[0m %d\n", numWorkers, timeoutSec, retries)
	fmt.Printf(" \x1b[90mTesting:\x1b[0m %s\n", targetNames[testMode])
	fmt.Println()

	// --- Worker Infrastructure ---
	jobs := make(chan string, total)
	results := make(chan ConfigResult, total)
	var wg sync.WaitGroup
	var mu sync.Mutex

	var processedCount int32
	var liveCount int32
	var lastHost string

	startTime := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSocksPort := 25000 + (workerID * 10)

			for rawConfig := range jobs {
				ok, delay := testRealTraffic(xrayBin, rawConfig, localSocksPort, timeoutSec, retries, testMode)

				atomic.AddInt32(&processedCount, 1)

				if ok {
					atomic.AddInt32(&liveCount, 1)
					results <- ConfigResult{config: rawConfig, delay: delay}
				}

				mu.Lock()
				lastHost = extractHost(rawConfig)
				mu.Unlock()
			}
		}(i)
	}

	for _, cfg := range configs {
		jobs <- cfg
	}
	close(jobs)

	// Monitor goroutine - single source of progress updates (no scroll)
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cp := atomic.LoadInt32(&processedCount)
				cl := atomic.LoadInt32(&liveCount)
				elapsed := time.Since(startTime)
				rate := float64(cp) / elapsed.Seconds()
				var remStr string
				if rate > 0 {
					remaining := int(float64(total-int(cp)) / rate)
					remStr = formatDuration(time.Duration(remaining) * time.Second)
				} else {
					remStr = "?"
				}
				host := func() string { mu.Lock(); defer mu.Unlock(); return lastHost }()
				pct := float64(cp) * 100 / float64(total)
				fmt.Printf("\r\x1b[K\x1b[36m[%d/%d]\x1b[0m %5.1f%% \x1b[32mAlive: %d\x1b[0m Testing: %s \x1b[90mETA: %s\x1b[0m",
					cp, total, pct, cl, host, remStr)
				if cp >= int32(total) {
					return
				}
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	close(done)
	close(results)

	// --- Collect Results ---
	var working []ConfigResult
	for res := range results {
		working = append(working, res)
	}

	// Sort by delay (ascending)
	sort.Slice(working, func(i, j int) bool {
		return working[i].delay < working[j].delay
	})

	// --- Write sorted output ---
	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("\n[-] Error creating output file %s: %v\n", outputFile, err)
		return
	}
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)

	for _, res := range working {
		writer.WriteString(res.config + "\n")
	}
	writer.Flush()

	totalTime := time.Since(startTime)

	// --- Write base64 encoded sub file ---
	subOutputFile := "valid_base64.txt"
	rawContent, _ := os.ReadFile(outputFile)
	b64Content := base64.StdEncoding.EncodeToString(rawContent)
	_ = os.WriteFile(subOutputFile, []byte(b64Content), 0644)

	clearScreen()
	fmt.Printf("\x1b[36m\x1b[1m ====== SCAN COMPLETE ======\x1b[0m\n")
	fmt.Printf(" \x1b[90mChecked:\x1b[0m %d\n", total)
	if len(working) > 0 {
		fmt.Printf(" \x1b[32mLive:\x1b[0m    %d (fastest: %dms)\n", len(working), working[0].delay.Milliseconds())
	} else {
		fmt.Printf(" \x1b[31mLive:\x1b[0m    0\n")
	}
	fmt.Printf(" \x1b[90mTime:\x1b[0m    %s\n", formatDuration(totalTime))
	fmt.Printf(" \x1b[90mOutput:\x1b[0m  %s\n", outputFile)
	fmt.Printf(" \x1b[90mSub URL:\x1b[0m  %s\n", subOutputFile)

	// --- Push to GitHub? ---
	fmt.Println()
	pushChoice := promptChoice(reader, "Push to GitHub?", []choice{
		{"Yes", 1}, {"No", 0}}, 0)
	fmt.Printf("\x1b[5A\x1b[J")
	if pushChoice == 0 {
		fmt.Println("\x1b[90m[*] Skipping push.\x1b[0m")
		return
	}

	// --- Git commit & push ---
	fmt.Println()
	gitAdd := exec.Command("git", "add", outputFile, subOutputFile)
	gitAdd.Dir, _ = filepath.Abs(".")
	if err := gitAdd.Run(); err != nil {
		fmt.Printf("[-] git add failed: %v\n", err)
		return
	}

	commitMsg := fmt.Sprintf("update valid configs [%s]", time.Now().Format("2006-01-02 15:04:05"))
	gitCommit := exec.Command("git", "commit", "-m", commitMsg)
	gitCommit.Dir, _ = filepath.Abs(".")
	if out, err := gitCommit.CombinedOutput(); err != nil {
		fmt.Printf("[-] git commit failed: %v\n%s", err, out)
		return
	}

	gitPush := exec.Command("git", "push", "origin", "master")
	gitPush.Dir, _ = filepath.Abs(".")
	if out, err := gitPush.CombinedOutput(); err != nil {
		fmt.Printf("[-] git push failed: %v\n%s", err, out)
		return
	}

	fmt.Printf("\x1b[32m[+] Pushed to GitHub!\x1b[0m\n")
	fmt.Printf(" \x1b[90mSubscription:\x1b[0m https://raw.githubusercontent.com/MortezaFp/xray-checker/master/valid_base64.txt\n")
}

func readConfigsFromFile(filePath string) []string {
	var configs []string
	file, err := os.Open(filePath)
	if err != nil {
		return configs
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if cfg := extractShareLink(line); cfg != "" {
			configs = append(configs, cfg)
		}
	}
	return configs
}

func fetchSubConfigs(subFile string) []string {
	var configs []string
	file, err := os.Open(subFile)
	if err != nil {
		fmt.Printf("[-] Error opening %s: %v\n", subFile, err)
		return configs
	}
	defer file.Close()

	client := &http.Client{Timeout: 30 * time.Second}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		subURL := strings.TrimSpace(scanner.Text())
		if subURL == "" || strings.HasPrefix(subURL, "#") || strings.HasPrefix(subURL, "//") {
			continue
		}
		fmt.Printf("[*] Fetching subscription: %s\n", subURL)
		resp, err := client.Get(subURL)
		if err != nil {
			fmt.Printf("[-] Error fetching %s: %v\n", subURL, err)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("[-] Error reading response from %s: %v\n", subURL, err)
			continue
		}

		parsed := parseSubContent(string(body))
		if len(parsed) > 0 {
			fmt.Printf("[+] Got %d configs from %s\n", len(parsed), subURL)
			configs = append(configs, parsed...)
		} else {
			fmt.Printf("[-] No valid configs found in response from %s\n", subURL)
		}
	}
	return configs
}

func parseSubContent(content string) []string {
	var configs []string

	// Try base64 decode first (standard v2ray subscription format)
	decoded, err := base64Decode(strings.TrimSpace(content))
	if err == nil && len(decoded) > 0 {
		for _, line := range strings.Split(decoded, "\n") {
			line = strings.TrimSpace(line)
			if cfg := extractShareLink(line); cfg != "" {
				configs = append(configs, cfg)
			}
		}
		if len(configs) > 0 {
			return configs
		}
	}

	// Try raw text parsing
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if cfg := extractShareLink(line); cfg != "" {
			configs = append(configs, cfg)
		}
	}

	// Try finding proxy links anywhere in the content
	if len(configs) == 0 {
		for _, prefix := range []string{"vless://", "trojan://", "vmess://", "ss://", "anytls://"} {
			idx := 0
			for {
				pos := strings.Index(content[idx:], prefix)
				if pos == -1 {
					break
				}
				start := idx + pos
				end := strings.IndexAny(content[start:], " \t\n\r")
				if end == -1 {
					end = len(content[start:])
				}
				if cfg := extractShareLink(content[start : start+end]); cfg != "" {
					configs = append(configs, cfg)
				}
				idx = start + end
			}
		}
	}

	return configs
}

func base64Decode(s string) (string, error) {
	// Try standard base64
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err == nil {
		return string(decoded), nil
	}
	// Try URL-safe base64
	decoded, err = base64.URLEncoding.DecodeString(s)
	if err == nil {
		return string(decoded), nil
	}
	// Try with padding fix
	pad := 4 - len(s)%4
	if pad < 4 {
		s += strings.Repeat("=", pad)
	}
	decoded, err = base64.StdEncoding.DecodeString(s)
	if err == nil {
		return string(decoded), nil
	}
	decoded, err = base64.URLEncoding.DecodeString(s)
	if err == nil {
		return string(decoded), nil
	}
	return "", fmt.Errorf("base64 decode failed")
}

func extractShareLink(line string) string {
	for _, prefix := range []string{"vless://", "trojan://", "vmess://", "ss://"} {
		if idx := strings.Index(line, prefix); idx != -1 {
			// Extract until whitespace
			end := strings.IndexAny(line[idx:], " \t\n\r")
			if end == -1 {
				return line[idx:]
			}
			return line[idx : idx+end]
		}
	}
	return ""
}

func testRealTraffic(xrayPath string, shareLink string, basePort int, timeoutSec int, retries int, testMode int) (bool, time.Duration) {
	for attempt := 1; attempt <= retries; attempt++ {
		start := time.Now()
		ok := testOnce(xrayPath, shareLink, basePort, timeoutSec, testMode)
		elapsed := time.Since(start)

		if ok {
			return true, elapsed
		}

		if attempt < retries {
			time.Sleep(300 * time.Millisecond)
		}
	}
	return false, 0
}

func testOnce(xrayPath string, shareLink string, basePort int, timeoutSec int, testMode int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec+5)*time.Second)
	defer cancel()

	fullConfig := buildXrayConfig(shareLink, basePort)
	if fullConfig == nil {
		return false
	}

	configBytes, _ := json.Marshal(fullConfig)
	tmpConfigPath := filepath.Join(os.TempDir(), fmt.Sprintf("xray_run_%d.json", basePort))
	_ = os.WriteFile(tmpConfigPath, configBytes, 0644)
	defer os.Remove(tmpConfigPath)

	cmdRun := exec.CommandContext(ctx, xrayPath, "run", "-c", tmpConfigPath)
	if err := cmdRun.Start(); err != nil {
		return false
	}
	defer func() {
		if cmdRun.Process != nil {
			_ = cmdRun.Process.Kill()
		}
	}()

	time.Sleep(800 * time.Millisecond)

	transport := &http.Transport{
		Proxy: http.ProxyURL(&url.URL{
			Scheme: "socks5",
			Host:   fmt.Sprintf("127.0.0.1:%d", basePort),
		}),
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(timeoutSec) * time.Second,
	}

	check := func(url string) bool {
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK
	}

	// Check selected targets
	if testMode == 3 {
		return check("http://cp.cloudflare.com/generate_204") &&
			check("http://www.google.com/generate_204")
	} else if testMode == 1 {
		return check("http://cp.cloudflare.com/generate_204")
	} else {
		return check("http://www.google.com/generate_204")
	}
}

func buildXrayConfig(shareLink string, basePort int) map[string]interface{} {
	switch {
	case strings.HasPrefix(shareLink, "vless://"), strings.HasPrefix(shareLink, "trojan://"):
		parsed, err := url.Parse(shareLink)
		if err != nil {
			return nil
		}
		return buildVlessTrojanConfig(parsed, basePort)
	case strings.HasPrefix(shareLink, "vmess://"):
		return buildVmessConfig(shareLink, basePort)
	case strings.HasPrefix(shareLink, "ss://"):
		parsed, err := url.Parse(shareLink)
		if err != nil {
			return nil
		}
		return buildShadowsocksConfig(parsed, basePort)
	default:
		return nil
	}
}

func buildVlessTrojanConfig(parsed *url.URL, basePort int) map[string]interface{} {
	protocol := parsed.Scheme
	hostAndPort := parsed.Host
	var serverHost string
	serverPort := 443

	if strings.Contains(hostAndPort, ":") {
		sh, sp, err := splitHostPort(hostAndPort)
		if err == nil {
			serverHost = sh
			serverPort = sp
		}
	} else {
		serverHost = hostAndPort
	}

	userUUID := parsed.User.Username()
	query := parsed.Query()

	security := query.Get("security")
	sni := query.Get("sni")
	pbk := query.Get("pbk")
	sid := query.Get("sid")
	network := query.Get("type")
	path := query.Get("path")
	flow := query.Get("flow")

	if sni == "" {
		sni = serverHost
	}
	if network == "" {
		network = "tcp"
	}

	outbound := map[string]interface{}{
		"protocol": protocol,
		"settings": map[string]interface{}{
			"vnext": []map[string]interface{}{
				{
					"address": serverHost,
					"port":    serverPort,
					"users": []map[string]interface{}{
						{"id": userUUID, "encryption": "none", "level": 0},
					},
				},
			},
		},
		"streamSettings": map[string]interface{}{
			"network":  network,
			"security": security,
		},
	}

	if flow != "" {
		outbound["streamSettings"].(map[string]interface{})["flow"] = flow
	}

	if protocol == "trojan" {
		outbound["settings"] = map[string]interface{}{
			"servers": []map[string]interface{}{
				{
					"address":  serverHost,
					"port":     serverPort,
					"password": userUUID,
					"level":    0,
				},
			},
		}
	}

	if security == "reality" {
		outbound["streamSettings"].(map[string]interface{})["realitySettings"] = map[string]interface{}{
			"serverName":  sni,
			"publicKey":   pbk,
			"shortId":     sid,
			"fingerprint": "chrome",
		}
	} else if security == "tls" {
		tlsSettings := map[string]interface{}{
			"serverName": sni,
		}
		if alpn := query.Get("alpn"); alpn != "" {
			tlsSettings["alpn"] = strings.Split(alpn, ",")
		}
		if fp := query.Get("fp"); fp != "" {
			tlsSettings["fingerprint"] = fp
		}
		outbound["streamSettings"].(map[string]interface{})["tlsSettings"] = tlsSettings
	}

	if network == "ws" || network == "websocket" {
		wsSettings := map[string]interface{}{"path": path}
		if host := query.Get("host"); host != "" {
			wsSettings["host"] = host
		}
		outbound["streamSettings"].(map[string]interface{})["wsSettings"] = wsSettings
	}

	if network == "grpc" {
		grpcSettings := map[string]interface{}{}
		if sn := query.Get("serviceName"); sn != "" {
			grpcSettings["serviceName"] = sn
		}
		if mode := query.Get("mode"); mode != "" {
			grpcSettings["mode"] = mode
		}
		outbound["streamSettings"].(map[string]interface{})["grpcSettings"] = grpcSettings
	}

	if network == "xhttp" || network == "http" || network == "xhttp+" {
		xhttpSettings := map[string]interface{}{}
		if path != "" {
			xhttpSettings["path"] = path
		}
		if host := query.Get("host"); host != "" {
			xhttpSettings["host"] = host
		}
		if mode := query.Get("mode"); mode != "" {
			xhttpSettings["mode"] = mode
		}
		if extra := query.Get("extra"); extra != "" {
			var extraMap map[string]interface{}
			if json.Unmarshal([]byte(extra), &extraMap) == nil {
				xhttpSettings["extra"] = extraMap
			}
		}
		outbound["streamSettings"].(map[string]interface{})["xhttpSettings"] = xhttpSettings
		if network == "xhttp" || network == "xhttp+" {
			outbound["streamSettings"].(map[string]interface{})["network"] = "xhttp"
		}
	}

	fullConfig := map[string]interface{}{
		"log":       map[string]string{"loglevel": "none"},
		"inbounds":  []map[string]interface{}{{"port": basePort, "listen": "127.0.0.1", "protocol": "socks", "settings": map[string]string{"auth": "noauth"}}},
		"outbounds": []interface{}{outbound},
	}

	return fullConfig
}

func buildVmessConfig(rawLink string, basePort int) map[string]interface{} {
	// vmess://<base64-json>
	encoded := strings.TrimPrefix(rawLink, "vmess://")
	decoded, err := base64Decode(encoded)
	if err != nil {
		return nil
	}

	var vmessData map[string]interface{}
	if err := json.Unmarshal([]byte(decoded), &vmessData); err != nil {
		return nil
	}

	serverHost, _ := vmessData["add"].(string)
	portStr, _ := vmessData["port"].(string)
	port, _ := strconv.Atoi(portStr)
	id, _ := vmessData["id"].(string)
	aidStr, _ := vmessData["aid"].(string)
	aid, _ := strconv.Atoi(aidStr)
	network, _ := vmessData["net"].(string)
	security, _ := vmessData["tls"].(string)
	host, _ := vmessData["host"].(string)
	path, _ := vmessData["path"].(string)
	sni, _ := vmessData["sni"].(string)
	alpnStr, _ := vmessData["alpn"].(string)
	fp, _ := vmessData["fp"].(string)

	if host == "" {
		host = sni
	}
	if sni == "" {
		sni = host
	}
	if sni == "" {
		sni = serverHost
	}
	if network == "" {
		network = "tcp"
	}
	if security == "" {
		security = "none"
	}
	if port == 0 {
		port = 443
	}

	outbound := map[string]interface{}{
		"protocol": "vmess",
		"settings": map[string]interface{}{
			"vnext": []map[string]interface{}{
				{
					"address": serverHost,
					"port":    port,
					"users": []map[string]interface{}{
						{"id": id, "alterId": aid, "security": "auto"},
					},
				},
			},
		},
		"streamSettings": map[string]interface{}{
			"network":  network,
			"security": security,
		},
	}

	if security == "tls" {
		tlsSettings := map[string]interface{}{"serverName": sni}
		if alpnStr != "" {
			tlsSettings["alpn"] = strings.Split(alpnStr, ",")
		}
		if fp != "" {
			tlsSettings["fingerprint"] = fp
		}
		outbound["streamSettings"].(map[string]interface{})["tlsSettings"] = tlsSettings
	}

	if network == "ws" || network == "websocket" {
		wsSettings := map[string]interface{}{"path": path}
		if host != "" {
			wsSettings["host"] = host
		}
		outbound["streamSettings"].(map[string]interface{})["wsSettings"] = wsSettings
	} else if network == "grpc" {
		grpcSettings := map[string]interface{}{}
		if sn, ok := vmessData["serviceName"].(string); ok {
			grpcSettings["serviceName"] = sn
		}
		outbound["streamSettings"].(map[string]interface{})["grpcSettings"] = grpcSettings
	} else if network == "tcp" {
		headerType, _ := vmessData["type"].(string)
		if headerType == "http" {
			tcpSettings := map[string]interface{}{
				"header": map[string]interface{}{
					"type": "http",
					"request": map[string]interface{}{
						"version": "1.1",
						"method":  "GET",
						"path":    []string{"/"},
					},
				},
			}
			outbound["streamSettings"].(map[string]interface{})["tcpSettings"] = tcpSettings
		}
	}

	fullConfig := map[string]interface{}{
		"log":       map[string]string{"loglevel": "none"},
		"inbounds":  []map[string]interface{}{{"port": basePort, "listen": "127.0.0.1", "protocol": "socks", "settings": map[string]string{"auth": "noauth"}}},
		"outbounds": []interface{}{outbound},
	}

	return fullConfig
}

func buildShadowsocksConfig(parsed *url.URL, basePort int) map[string]interface{} {
	// ss://method:password@host:port
	userInfo := parsed.User
	if userInfo == nil {
		return nil
	}

	// Try base64 decoded userinfo (standard format: ss://base64(method:password)@host:port)
	encoded := userInfo.String()
	decoded, err := base64Decode(encoded)
	var method, password string

	if err == nil && strings.Contains(decoded, ":") {
		parts := strings.SplitN(decoded, ":", 2)
		method = parts[0]
		password = parts[1]
	} else if strings.Contains(encoded, ":") {
		parts := strings.SplitN(encoded, ":", 2)
		method = parts[0]
		password = parts[1]
	} else {
		return nil
	}

	hostAndPort := parsed.Host
	var serverHost string
	serverPort := 443

	if strings.Contains(hostAndPort, ":") {
		sh, sp, err := splitHostPort(hostAndPort)
		if err == nil {
			serverHost = sh
			serverPort = sp
		}
	} else {
		serverHost = hostAndPort
	}

	outbound := map[string]interface{}{
		"protocol": "shadowsocks",
		"settings": map[string]interface{}{
			"servers": []map[string]interface{}{
				{
					"address":  serverHost,
					"port":     serverPort,
					"method":   method,
					"password": password,
					"level":    0,
				},
			},
		},
	}

	fullConfig := map[string]interface{}{
		"log":       map[string]string{"loglevel": "none"},
		"inbounds":  []map[string]interface{}{{"port": basePort, "listen": "127.0.0.1", "protocol": "socks", "settings": map[string]string{"auth": "noauth"}}},
		"outbounds": []interface{}{outbound},
	}

	return fullConfig
}

func extractHost(shareLink string) string {
	if strings.HasPrefix(shareLink, "vmess://") {
		return extractVmessHost(shareLink)
	}
	parsed, err := url.Parse(shareLink)
	if err != nil {
		return "?"
	}
	h := parsed.Host
	if idx := strings.LastIndex(h, ":"); idx != -1 {
		return h[:idx]
	}
	return h
}

func extractPort(shareLink string) int {
	if strings.HasPrefix(shareLink, "vmess://") {
		return extractVmessPort(shareLink)
	}
	parsed, err := url.Parse(shareLink)
	if err != nil {
		return 0
	}
	h := parsed.Host
	if idx := strings.LastIndex(h, ":"); idx != -1 {
		p, _ := strconv.Atoi(h[idx+1:])
		return p
	}
	return 443
}

func extractVmessHost(shareLink string) string {
	encoded := strings.TrimPrefix(shareLink, "vmess://")
	decoded, err := base64Decode(encoded)
	if err != nil {
		return "?"
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(decoded), &data); err != nil {
		return "?"
	}
	host, _ := data["add"].(string)
	if host == "" {
		return "?"
	}
	return host
}

func extractVmessPort(shareLink string) int {
	encoded := strings.TrimPrefix(shareLink, "vmess://")
	decoded, err := base64Decode(encoded)
	if err != nil {
		return 0
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(decoded), &data); err != nil {
		return 0
	}
	portStr, _ := data["port"].(string)
	port, _ := strconv.Atoi(portStr)
	if port == 0 {
		if p, ok := data["port"].(float64); ok {
			port = int(p)
		}
	}
	return port
}

func splitHostPort(hp string) (string, int, error) {
	idx := strings.LastIndex(hp, ":")
	if idx == -1 {
		return hp, 443, nil
	}
	h := hp[:idx]
	p, err := strconv.Atoi(hp[idx+1:])
	if err != nil {
		return hp, 443, err
	}
	return h, p, nil
}

type choice struct {
	label string
	value int
}

func promptChoice(reader *bufio.Reader, title string, options []choice, defaultIdx int) int {
	fd := int(os.Stdin.Fd())
	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return promptChoiceFallback(reader, title, options, defaultIdx)
	}
	defer term.Restore(fd, oldState)

	sel := defaultIdx

	fmt.Print("\x1b[?25l")
	defer fmt.Print("\x1b[?25h")

	draw := func() {
		fmt.Printf("\r\x1b[K\x1b[36m\x1b[1m%s:\x1b[0m\n", title)
		for i, opt := range options {
			m, c := " ", ""
			if i == sel {
				m, c = ">", "\x1b[32m"
			}
			fmt.Printf("\r\x1b[K  %s%s%d) %s\x1b[0m\n", c, m, i+1, opt.label)
		}
		m, c := " ", ""
		if sel == len(options) {
			m, c = ">", "\x1b[32m"
		}
		fmt.Printf("\r\x1b[K  %s%s%d) Custom\x1b[0m\n", c, m, len(options)+1)
		fmt.Printf("\r\x1b[K\x1b[90m\xe2\x86\x91\xe2\x86\x93 move  Enter select  1-%d shortcut\x1b[0m\n", len(options)+1)
	}

	draw()
	menuLines := len(options) + 3

	for {
		key := readKey(fd)

		switch key {
		case "\x1b[A":
			if sel > 0 {
				sel--
				fmt.Printf("\x1b[%dA", menuLines)
				draw()
			}
		case "\x1b[B":
			if sel < len(options) {
				sel++
				fmt.Printf("\x1b[%dA", menuLines)
				draw()
			}
		case "\r", "\n":
			if sel == len(options) {
				term.Restore(fd, oldState)
				fmt.Print("\n\x1b[33mEnter custom value:\x1b[0m ")
				input, _ := reader.ReadString('\n')
				input = strings.TrimSpace(input)
				val, err := strconv.Atoi(input)
				if err == nil && val >= 0 {
					return val
				}
				fmt.Println("\x1b[31m[!] Invalid. Using default.\x1b[0m")
				return options[defaultIdx].value
			}
			return options[sel].value
		case "1", "2", "3", "4", "5", "6", "7", "8", "9":
			n := int(key[0]-'0') - 1
			if n < len(options) {
				return options[n].value
			}
			if n == len(options) {
				term.Restore(fd, oldState)
				fmt.Print("\n\x1b[33mEnter custom value:\x1b[0m ")
				input, _ := reader.ReadString('\n')
				input = strings.TrimSpace(input)
				val, err := strconv.Atoi(input)
				if err == nil && val >= 0 {
					return val
				}
				fmt.Println("\x1b[31m[!] Invalid. Using default.\x1b[0m")
				return options[defaultIdx].value
			}
		}
	}
}

func readKey(fd int) string {
	buf := make([]byte, 3)
	n, err := os.Stdin.Read(buf[:1])
	if err != nil || n == 0 {
		return ""
	}
	if buf[0] == 0x1b {
		n2, _ := os.Stdin.Read(buf[1:2])
		if n2 == 0 {
			return "\x1b"
		}
		if buf[1] == '[' {
			n3, _ := os.Stdin.Read(buf[2:3])
			if n3 == 0 {
				return "\x1b["
			}
			return string(buf[:3])
		}
		return string(buf[:2])
	}
	return string(buf[0])
}

func promptChoiceFallback(reader *bufio.Reader, title string, options []choice, defaultIdx int) int {
	fmt.Printf("\n%s:\n", title)
	for i, opt := range options {
		mark := " "
		if i == defaultIdx {
			mark = ">"
		}
		fmt.Printf("  %s %d) %s\n", mark, i+1, opt.label)
	}
	fmt.Printf("  %d) Custom\n", len(options)+1)
	fmt.Printf("Enter choice (1-%d, Enter for default #%d): ", len(options)+1, defaultIdx+1)

	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return options[defaultIdx].value
	}

	n, err := strconv.Atoi(input)
	if err != nil || n < 1 || n > len(options)+1 {
		fmt.Printf("[!] Invalid. Using default.\n")
		return options[defaultIdx].value
	}

	if n == len(options)+1 {
		fmt.Print("Enter custom value: ")
		custom, _ := reader.ReadString('\n')
		custom = strings.TrimSpace(custom)
		val, err := strconv.Atoi(custom)
		if err == nil && val >= 0 {
			return val
		}
		fmt.Printf("[!] Invalid. Using default.\n")
		return options[defaultIdx].value
	}

	return options[n-1].value
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func clearScreen() {
	fmt.Print("\x1b[H\x1b[2J\x1b[3J")
}

func promptExit() {
	fmt.Println("\nPress 'Enter' to exit...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}
