package view

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestUpsertAndLookup(t *testing.T) {
	dir := t.TempDir()
	reg, err := Open(filepath.Join(dir, "views.bolt"))
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()

	v, err := reg.Register("users")
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Upsert([]byte("alice"), []byte("100")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := v.Lookup([]byte("alice"))
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if string(got) != "100" {
		t.Fatalf("want 100, got %q", got)
	}
	// Tombstone via nil.
	if err := v.Upsert([]byte("alice"), nil); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := v.Lookup([]byte("alice")); ok {
		t.Fatal("expected tombstoned key to be gone")
	}
}

func TestIncrCountAndHTTP(t *testing.T) {
	dir := t.TempDir()
	reg, err := Open(filepath.Join(dir, "views.bolt"))
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()

	v, err := reg.Register("counts")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := v.IncrCount([]byte("hits")); err != nil {
			t.Fatal(err)
		}
	}
	n, ok, err := v.LookupUint64([]byte("hits"))
	if err != nil || !ok || n != 3 {
		t.Fatalf("count = %d ok=%v err=%v", n, ok, err)
	}

	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/view/counts?key=hits")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out["uint64"] != "3" {
		t.Fatalf("want uint64=3, got %+v", out)
	}

	// 404 for missing key.
	resp2, _ := http.Get(srv.URL + "/view/counts?key=nope")
	defer resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("want 404, got %d", resp2.StatusCode)
	}

	// summary when no key provided.
	resp3, _ := http.Get(srv.URL + "/view/counts")
	defer resp3.Body.Close()
	body3, _ := io.ReadAll(resp3.Body)
	var summary map[string]any
	_ = json.Unmarshal(body3, &summary)
	if summary["name"] != "counts" {
		t.Fatalf("want name=counts, got %+v", summary)
	}
}
