package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

// Names of what provision makes. The role may publish and subscribe under
// benchTopicRoot, which is where every load command puts its topics.
const (
	benchApp       = "aapp_bench"
	benchExternal  = "bench"
	benchRole      = "rw"
	benchTopicRoot = "bench"
	tokenEnv       = "TETHER_ADMIN_TOKEN"
)

// adminClient calls a node's admin REST API.
type adminClient struct {
	base  string // for example http://127.0.0.1:8080/tether/v1
	token string
	http  *http.Client
}

func newAdminClient(base, token string) *adminClient {
	return &adminClient{base: base, token: token, http: &http.Client{Timeout: 30 * time.Second}}
}

// do sends one request and decodes a JSON reply into out. It returns the
// status, and an error for anything outside 2xx.
func (a *adminClient) do(ctx context.Context, method, path string, body, out any) (int, error) {
	var rd io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}

		rd = bytes.NewReader(raw)
	}

	// #nosec G704 -- the operator names the API base URL on the command line.
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, rd)
	if err != nil {
		return 0, err
	}

	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := a.http.Do(req) // #nosec G704 -- as above.
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}

	if resp.StatusCode/100 != 2 {
		return resp.StatusCode, fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(data))
	}

	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: decode %s: %w", method, path, data, err)
		}
	}

	return resp.StatusCode, nil
}

// provisioned is what provision made.
type provisioned struct {
	Tenant string
	Creds  []cred
}

// provision makes the bench tenant (or finds it, when an earlier run made it),
// a role that may publish and subscribe under bench/#, and n unpinned devices
// with that role. A device with no pinned client ID may connect under any client
// ID, so the load commands spread many connections over few credentials.
func provision(ctx context.Context, a *adminClient, n int) (provisioned, error) {
	var tn struct {
		ID string `json:"id"`
	}

	status, err := a.do(ctx, http.MethodPost, "/tenants", map[string]any{
		"app_id": benchApp, "external_id": benchExternal, "name": "Bench",
	}, &tn)

	switch {
	case status == http.StatusConflict:
		var list struct {
			Items []struct {
				ID         string `json:"id"`
				ExternalID string `json:"external_id"`
			} `json:"items"`
		}

		if _, lerr := a.do(ctx, http.MethodGet, "/tenants?limit=500", nil, &list); lerr != nil {
			return provisioned{}, lerr
		}

		for _, t := range list.Items {
			if t.ExternalID == benchExternal {
				tn.ID = t.ID
			}
		}

		if tn.ID == "" {
			return provisioned{}, errors.New("tenant exists but is not in the first 500")
		}
	case err != nil:
		return provisioned{}, err
	}

	status, err = a.do(ctx, http.MethodPost, "/tenants/"+tn.ID+"/roles", map[string]any{
		"slug": benchRole, "publish": []string{benchTopicRoot + "/#"}, "subscribe": []string{benchTopicRoot + "/#"},
	}, nil)
	if err != nil && status != http.StatusConflict {
		return provisioned{}, err
	}

	creds := make([]cred, n)
	errs := make([]error, n)
	jobs := make(chan int)

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			for i := range jobs {
				var d struct {
					Username string `json:"username"`
					Password string `json:"password"`
				}

				_, errs[i] = a.do(ctx, http.MethodPost, "/tenants/"+tn.ID+"/devices", map[string]any{
					"name": fmt.Sprintf("bench-%d", i), "roles": []string{benchRole},
				}, &d)
				creds[i] = cred{User: d.Username, Pass: d.Password}
			}
		})
	}

	for i := range n {
		jobs <- i
	}

	close(jobs)
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return provisioned{}, err
	}

	return provisioned{Tenant: tn.ID, Creds: creds}, nil
}

func runProvision(args []string, stdout, stderr io.Writer) error {
	fs := newFlags("provision", stderr)
	admin := fs.String("admin", "", "admin API base URL of any node, for example http://127.0.0.1:8080/tether/v1")
	token := fs.String("token", os.Getenv(tokenEnv), "admin bearer token (default $"+tokenEnv+")")
	devices := fs.Int("devices", 16, "how many devices to make")
	out := fs.String("out", "creds.csv", "where to write the credentials CSV (mode 0600)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *admin == "" || *devices < 1 {
		return errors.New("provision: --admin is required and --devices must be at least 1")
	}

	p, err := provision(context.Background(), newAdminClient(*admin, *token), *devices)
	if err != nil {
		return err
	}

	if err := writeCreds(*out, p.Creds); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "tenant %s: %d devices written to %s\n", p.Tenant, len(p.Creds), *out)

	return nil
}
