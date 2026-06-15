// Command footprint_go measures local idle footprint for the standard Go SDK
// REST client skeleton and the local turbo SDK engine registration path.
//
// It does not open real Mezon sockets. The standard Go SDK hardcodes the Mezon
// API host inside package constants, so this benchmark avoids credentials and
// network by constructing the generated REST API client stack that NewClient
// wraps after authentication.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2"
	turbo "github.com/mezon/mezon-go-sdk-turbo/lib/turbo"
	"github.com/mezon/mezon-go-sdk-turbo/lib/types"
	swagger "github.com/nccasia/mezon-go-sdk/mezon-api"
	"github.com/redis/go-redis/v9"
)

type goSDKClient struct {
	cfg   *swagger.Configuration
	api   *swagger.MezonApiService
	token string
}

type fakeLister struct{}

func (fakeLister) ListSince(context.Context, string, string, string, string, int32) ([]types.Message, string, error) {
	return nil, "", nil
}

type result struct {
	SDK            string  `json:"sdk"`
	Version        string  `json:"version"`
	Mode           string  `json:"mode"`
	Bots           int     `json:"bots"`
	ElapsedMS      float64 `json:"elapsed_ms"`
	RSSKiB         int64   `json:"rss_kib"`
	RSSDeltaKiB    int64   `json:"rss_delta_kib"`
	HeapBytes      uint64  `json:"heap_bytes"`
	HeapDeltaBytes uint64  `json:"heap_delta_bytes"`
	HeapObjects    uint64  `json:"heap_objects"`
	Goroutines     int     `json:"goroutines"`
	ObjectsKept    int     `json:"objects_kept"`
	Notes          string  `json:"notes"`
}

func rssKiB() int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func memStats() runtime.MemStats {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func main() {
	sdk := flag.String("sdk", "turbo", "turbo or go")
	bots := flag.Int("bots", 1, "bot/client count")
	hold := flag.Duration("hold", 250*time.Millisecond, "settle time before measurement")
	flag.Parse()

	beforeRSS := rssKiB()
	beforeMem := memStats()
	start := time.Now()

	kept := 0
	notes := ""
	version := ""
	var keepAlive any

	switch *sdk {
	case "go":
		clients := make([]goSDKClient, 0, *bots)
		for i := 0; i < *bots; i++ {
			cfg := swagger.NewConfiguration()
			cfg.BasePath = "https://api.mezon.ai"
			cfg.HTTPClient = &http.Client{Timeout: 15 * time.Second}
			cfg.AddDefaultHeader("Authorization", "Bearer token-"+strconv.Itoa(i))
			clients = append(clients, goSDKClient{
				cfg:   cfg,
				api:   swagger.NewAPIClient(cfg).MezonApi,
				token: "token-" + strconv.Itoa(i),
			})
		}
		kept = len(clients)
		keepAlive = clients
		version = "github.com/nccasia/mezon-go-sdk@v0.0.34"
		notes = "Generated REST API client skeleton only; NewClient/CreateSocket require real api.mezon.ai auth and websocket."
	case "turbo":
		mr, err := miniredis.Run()
		if err != nil {
			panic(err)
		}
		defer mr.Close()
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer rdb.Close()

		engine := turbo.New(turbo.Config{
			WSHost:        "127.0.0.1:9",
			WSSSL:         false,
			PollRPS:       1000,
			PollWorkers:   16,
			PollPageLimit: 20,
			StateTTL:      time.Hour,
		}, rdb, fakeLister{}, nil)
		for i := 0; i < *bots; i++ {
			engine.Register(types.BotRef{
				KeyID:       "key-" + strconv.Itoa(i),
				TenantID:    "tenant-" + strconv.Itoa(i%10),
				BotUserID:   strconv.FormatInt(184000000000+int64(i), 10),
				BotToken:    "token-" + strconv.Itoa(i),
				WorkspaceID: "workspace-" + strconv.Itoa(i%10),
				Plan:        "pro",
			})
		}
		kept = *bots
		keepAlive = engine
		version = "local github.com/mezon/mezon-go-sdk-turbo"
		notes = "Turbo engine plus Redis-backed state client and registered cold bots; no sockets opened."
	default:
		panic(fmt.Sprintf("unknown sdk %q", *sdk))
	}

	elapsed := time.Since(start).Seconds() * 1000
	if *hold > 0 {
		time.Sleep(*hold)
	}
	afterMem := memStats()
	runtime.KeepAlive(keepAlive)
	afterRSS := rssKiB()
	heapDelta := uint64(0)
	if afterMem.HeapAlloc > beforeMem.HeapAlloc {
		heapDelta = afterMem.HeapAlloc - beforeMem.HeapAlloc
	}

	_ = json.NewEncoder(os.Stdout).Encode(result{
		SDK:            map[bool]string{true: "go-sdk", false: "go-turbo-sdk"}[*sdk == "go"],
		Version:        version,
		Mode:           "construct-idle",
		Bots:           *bots,
		ElapsedMS:      elapsed,
		RSSKiB:         afterRSS,
		RSSDeltaKiB:    max64(0, afterRSS-beforeRSS),
		HeapBytes:      afterMem.HeapAlloc,
		HeapDeltaBytes: heapDelta,
		HeapObjects:    afterMem.HeapObjects,
		Goroutines:     runtime.NumGoroutine(),
		ObjectsKept:    kept,
		Notes:          notes,
	})
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
