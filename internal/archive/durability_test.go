package archive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

// requireReadablePublication reads every published pack of the tenant back
// through the blob store, as the Cold reader does: a committed window must
// never reference an index or block that cannot be decoded.
func requireReadablePublication(t *testing.T, a *Archiver, cfg Config, tenant string) int {
	t.Helper()
	ctx := context.Background()
	plan, err := a.Catalog.Select(ctx, catalog.Selection{Tenant: tenant, Start: cfg.CoverageStart, End: cfg.CoverageStart + cfg.Window.Milliseconds()})
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, window := range plan.Windows {
		for _, ref := range window.Packs {
			encoded, err := a.Store.Get(ctx, ref.IndexKey)
			if err != nil || pack.Sum(encoded) != ref.IndexSHA256 {
				t.Fatalf("published index unreadable: %v", err)
			}
			idx, err := pack.ParseIndex(encoded, cfg.PackLimits)
			if err != nil {
				t.Fatalf("published index %s does not parse: %v", ref.IndexKey, err)
			}
			for si, series := range idx.Series {
				for bi, block := range series.Blocks {
					data, err := a.Store.GetRange(ctx, ref.DataKey, block.Offset, block.Length)
					if err != nil {
						t.Fatal(err)
					}
					mb, err := pack.LoadBlock(idx, si, bi, data)
					if err != nil {
						t.Fatalf("published block does not load: %v", err)
					}
					if mb != nil {
						rows += mb.Block.RowsCount()
					}
				}
			}
		}
	}
	return rows
}

// faultyBlobs injects one fault into the n-th Put (1-based) and passes every
// other call through, so a fault can hit any object of a multi-pack window.
type faultyBlobs struct {
	objectstore.BlobStore
	mode   string
	failAt int
	mu     sync.Mutex
	puts   int
}

func (s *faultyBlobs) Put(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	s.puts++
	n := s.puts
	s.mu.Unlock()
	if n != s.failAt {
		return s.BlobStore.Put(ctx, key, data)
	}
	switch s.mode {
	case "error":
		return errors.New("injected PUT failure")
	case "torn":
		// A provider stores a prefix of the object but acknowledges success.
		return s.BlobStore.Put(ctx, key, data[:len(data)/2])
	case "stall":
		// The provider never answers; only the window deadline ends the call.
		<-ctx.Done()
		return ctx.Err()
	case "lost-ack":
		if err := s.BlobStore.Put(ctx, key, data); err != nil {
			return err
		}
		return errors.New("response lost after successful PUT")
	}
	return fmt.Errorf("unknown fault %q", s.mode)
}

// A fault in any object of a multi-pack window must leave the catalog and
// coverage unchanged; the next attempt publishes a complete, readable window,
// and a repeated pass after the commit uploads nothing.
func TestSQLiteUploadFaultsNeverPublishPartialWindows(t *testing.T) {
	// MaxBlocks=1 splits the fixture window into three packs, i.e. six PUTs:
	// data and index of each pack, in that order.
	for _, mode := range []string{"error", "torn", "stall", "lost-ack"} {
		for _, failAt := range []int{1, 4, 6} {
			t.Run(fmt.Sprintf("%s/put%d", mode, failAt), func(t *testing.T) {
				server, _ := newSQLiteSource(t, "multipack")
				cfg := baseConfig(server.URL)
				cfg.PackLimits.MaxBlocks = 1
				a, _ := newSQLiteArchiver(t, cfg)
				clean := a.Store
				a.Store = &faultyBlobs{BlobStore: clean, mode: mode, failAt: failAt}
				a.WindowTimeout = 2 * time.Second
				ctx := context.Background()
				result, err := a.RunPass(ctx, testNow)
				if err == nil || result.CommittedWindows != 0 {
					t.Fatalf("faulted attempt committed: %+v %v", result, err)
				}
				if mode == "stall" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("stalled PUT did not end at the window deadline: %v", err)
				}
				state, err := a.Catalog.Status(ctx)
				if err != nil || state.Generation != 0 || state.Coverage[0].End != cfg.CoverageStart {
					t.Fatalf("faulted attempt changed the catalog: %+v %v", state, err)
				}
				// A restarted archiver retries immediately with fresh attempt keys.
				restarted := &Archiver{Store: clean, Catalog: a.Catalog, Config: cfg, RetryInterval: time.Minute}
				result, err = restarted.RunPass(ctx, testNow.Add(time.Minute))
				if err != nil || result.CommittedWindows != 1 || result.Samples != 3 {
					t.Fatalf("retry after fault failed: %+v %v", result, err)
				}
				if rows := requireReadablePublication(t, restarted, cfg, "7:3"); rows == 0 {
					t.Fatal("retry published no readable rows")
				}
				// Archive the remaining eligible windows; once caught up, another
				// pass must neither commit nor upload.
				for i := 0; i < 4 && result.CommittedWindows > 0; i++ {
					if result, err = restarted.RunPass(ctx, testNow.Add(time.Minute)); err != nil {
						t.Fatal(err)
					}
				}
				puts := clean.(*objectstore.Local).Stats().Puts
				result, err = restarted.RunPass(ctx, testNow.Add(time.Minute))
				if err != nil || result.CommittedWindows != 0 || clean.(*objectstore.Local).Stats().Puts != puts {
					t.Fatalf("repeated pass was not idempotent: %+v %v", result, err)
				}
			})
		}
	}
}

// Pack indexes store labels as JSON, which cannot carry bytes that are not
// valid UTF-8. Such a series used to be published with an index that every
// later read rejects; the archiver must refuse the window instead.
func TestSQLiteRejectsLabelsThatJSONCannotRepresent(t *testing.T) {
	cfg := baseConfig("")
	start, end := cfg.StartMS, cfg.EndMS
	labels := map[string]string{"__name__": "x", "job": "a\xffb"}
	name := pack.MetricName(pack.Tenant{}, labels).MarshalNoAccountIDProjectID(nil)
	var block storage.Block
	block.Init(&storage.TSID{}, []int64{start, start + 10}, []int64{1, 3}, 0, 64)
	payload := block.MarshalPortable(nil)
	body := encoding.MarshalInt64(nil, start)
	body = encoding.MarshalInt64(body, end-1)
	body = encoding.MarshalUint32(body, uint32(len(name)))
	body = append(body, name...)
	body = encoding.MarshalUint32(body, uint32(len(payload)))
	body = append(body, payload...)
	// The stock JSON APIs emit the raw label bytes; JSON decoding maps them to
	// U+FFFD on both sides of the comparison, so the oracle agrees.
	metric := `{"__name__":"x","job":"a` + "\xff" + `b"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/export/native"):
			w.Header().Set("Content-Type", "VictoriaMetrics/native")
			_, _ = w.Write(body)
		case strings.HasPrefix(r.URL.Query().Get("query"), "{"):
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":%s,"values":[[%s,"1"],[%s,"3"]]}]}}`, metric, seconds(start), seconds(start+10))
		default:
			value := "2"
			if strings.HasPrefix(r.URL.Query().Get("query"), "stale_") {
				value = "0"
			}
			fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":%s,"value":[%s,%q]}]}}`, metric, seconds(end-1), value)
		}
	}))
	defer server.Close()
	cfg.Sources[0].URL = server.URL
	a, _ := newSQLiteArchiver(t, cfg)
	ctx := context.Background()
	result, err := a.RunPass(ctx, testNow)
	if result.CommittedWindows != 0 {
		requireReadablePublication(t, a, cfg, "7:3")
		t.Fatal("window with labels that JSON cannot represent was published")
	}
	if err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Fatalf("expected an explicit UTF-8 refusal: %v", err)
	}
	if puts := a.Store.(*objectstore.Local).Stats().Puts; puts != 0 {
		t.Fatalf("refused window uploaded %d objects", puts)
	}
	state, err := a.Catalog.Status(ctx)
	if err != nil || state.Generation != 0 {
		t.Fatalf("refused window changed the catalog: %+v %v", state, err)
	}
}
