// The reload command: the operator's "make the daemon pick up my config
// change" button. It is the HTTP twin of `systemctl kill -s HUP unistord`
// and drives the same daemon-side path, so the reply names every section
// the daemon re-applied.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
)

// reloadReport mirrors the daemon's reloadReport.
type reloadReport struct {
	At       string `json:"at"`
	Reason   string `json:"reason,omitempty"`
	OK       bool   `json:"ok"`
	Sections []struct {
		Name    string `json:"name"`
		Applied bool   `json:"applied"`
		Detail  string `json:"detail,omitempty"`
		Error   string `json:"error,omitempty"`
	} `json:"sections"`
	RestartRequired []string `json:"restart_required,omitempty"`
}

// reloadCmd posts /v1/reload and prints the per-section report.
func reloadCmd(ctx context.Context, args []string) error {
	// Own flag set (no -type: reload has nothing to select), so the help
	// text stays honest; exportRequest drives the call.
	fs := flag.NewFlagSet("reload", flag.ExitOnError)
	httpURL := fs.String("http", "http://127.0.0.1:8443", "unistord admin URL")
	token := fs.String("token", "", "admin bearer token (when unistord runs with -admin-token)")
	mustParse(fs, args)
	f := &exportFlags{fs: fs, httpURL: httpURL, token: token}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: unistorctl reload [-http URL] [-token T]")
	}
	status, body, err := exportRequest(ctx, f, http.MethodPost, "/v1/reload", nil)
	if err != nil {
		return err
	}
	if status == http.StatusServiceUnavailable {
		// No quorum: the meta-writing sections cannot land here.
		return fmt.Errorf("%s", httpReplyError("reload", status, body))
	}
	var rep reloadReport
	if err := json.Unmarshal(body, &rep); err != nil || len(rep.Sections) == 0 {
		if status != http.StatusOK {
			return fmt.Errorf("%s", httpReplyError("reload", status, body))
		}
		return fmt.Errorf("reload: parse reply: %w", err)
	}
	for _, s := range rep.Sections {
		switch {
		case s.Error != "":
			fmt.Printf("  %-9s FAILED  %s\n", s.Name, s.Error)
		case s.Applied:
			fmt.Printf("  %-9s ok      %s\n", s.Name, s.Detail)
		default:
			fmt.Printf("  %-9s skipped %s\n", s.Name, s.Detail)
		}
	}
	if len(rep.RestartRequired) > 0 {
		fmt.Println("  needs a restart (a reload cannot adopt these):")
		for _, r := range rep.RestartRequired {
			fmt.Println("    - " + r)
		}
	}
	if !rep.OK {
		var failed []string
		for _, s := range rep.Sections {
			if s.Error != "" {
				failed = append(failed, s.Name)
			}
		}
		return fmt.Errorf("reload: %s did not apply, previous state stays in effect", strings.Join(failed, ", "))
	}
	fmt.Println("reload: every section applied")
	return nil
}
