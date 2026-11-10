// Runtime export commands: attach a volume to a machine through the
// running unistord admin API (POST/GET/DELETE /v1/exports). The daemon
// owns the data planes, so these commands speak HTTP — the meta
// directory is not involved (the CLI still opens it like every other
// command; -meta-dir must point at the node's unistord meta).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const exportDefaultType = "nbd"

var exportTypes = []string{"nbd", "vduse", "vub"}

// exportCmd dispatches: export create|ls|rm.
func exportCmd(ctx context.Context, args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("export: subcommand required (create/ls/rm)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		return exportCreateCmd(ctx, rest)
	case "ls":
		return exportListCmd(ctx, rest)
	case "rm":
		return exportRmCmd(ctx, rest)
	default:
		usage()
		return fmt.Errorf("export: unknown subcommand %q (create/ls/rm)", sub)
	}
}

// exportFlags are the flags every export subcommand shares.
type exportFlags struct {
	fs      *flag.FlagSet
	httpURL *string
	token   *string
	typ     *string
}

func newExportFlags(name string) *exportFlags {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	f := &exportFlags{fs: fs}
	f.httpURL = fs.String("http", "http://127.0.0.1:8443", "unistord admin URL")
	f.token = fs.String("token", "", "admin bearer token (when unistord runs with -admin-token)")
	f.typ = fs.String("type", exportDefaultType, "export type: nbd | vduse | vub")
	return f
}

// exportCreateCmd asks the daemon to attach a volume: nbd (kernel nbd
// client, the default), vduse (virtio-blk via the vduse kernel module,
// needs -devname, Linux) or vub (vhost-user-blk unix socket for
// QEMU/Dragonball).
func exportCreateCmd(ctx context.Context, args []string) error {
	f := newExportFlags("export create")
	devname := f.fs.String("devname", "", "vduse device name (required with -type vduse); vub export name (optional)")
	ro := f.fs.Bool("ro", false, "serve read-only (persisted on the volume record)")
	mustParse(f.fs, args)
	if f.fs.NArg() != 1 {
		return fmt.Errorf("usage: unistorctl export create <volume> [-type nbd|vduse|vub] [-devname N] [-ro]")
	}
	volume := f.fs.Arg(0)
	if err := checkExportType(*f.typ); err != nil {
		return err
	}
	if *f.typ == "vduse" && *devname == "" {
		return fmt.Errorf("export: -type vduse needs -devname (the kernel device name)")
	}
	body := map[string]any{"volume": volume, "type": *f.typ, "read_only": *ro}
	if *devname != "" {
		body["devname"] = *devname
	}
	status, respBody, err := exportRequest(ctx, f, http.MethodPost, "/v1/exports", body)
	if err != nil {
		return err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return fmt.Errorf("export create: %s", httpReplyError("POST /v1/exports", status, respBody))
	}
	var reply map[string]any
	if err := json.Unmarshal(respBody, &reply); err != nil {
		return fmt.Errorf("export create: parse reply: %w", err)
	}
	if status == http.StatusOK {
		fmt.Printf("%s export %q already attached to volume %q\n", *f.typ, exportReplyName(reply, volume), volume)
		return nil
	}
	fmt.Printf("%s export %s volume %q\n", *f.typ, exportReplyName(reply, volume), volume)
	for _, k := range []string{"socket", "devname"} {
		if s, ok := reply[k].(string); ok && s != "" && s != exportReplyName(reply, volume) {
			fmt.Printf("  %s: %s\n", k, s)
		}
	}
	if ro, ok := reply["read_only"].(bool); ok && ro {
		fmt.Println("  read-only")
	}
	if n, ok := reply["name"].(string); ok && *f.typ == "vub" {
		fmt.Printf("  qemu: -chardev socket,id=vub%s,path=%v -device vhost-user-blk-pci,chardev=vub%s\n",
			n, reply["socket"], n)
	}
	return nil
}

// exportListCmd prints the live exports per type.
func exportListCmd(ctx context.Context, args []string) error {
	f := newExportFlags("export ls")
	asJSON := f.fs.Bool("json", false, "print the raw reply instead of the table")
	mustParse(f.fs, args)
	status, body, err := exportRequest(ctx, f, http.MethodGet, "/v1/exports", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("export ls: %s", httpReplyError("GET /v1/exports", status, body))
	}
	if *asJSON {
		os.Stdout.Write(body)
		if len(body) > 0 && body[len(body)-1] != '\n' {
			fmt.Println()
		}
		return nil
	}
	var reply map[string][]map[string]any
	if err := json.Unmarshal(body, &reply); err != nil {
		return fmt.Errorf("export ls: parse reply: %w", err)
	}
	found := false
	for _, typ := range exportTypes {
		rows := reply[typ]
		if len(rows) == 0 {
			continue
		}
		found = true
		fmt.Printf("%s:\n", typ)
		for _, e := range rows {
			desc := fmt.Sprint(e["volume"])
			if desc == "" || desc == "<nil>" {
				desc = "-"
			}
			ro := ""
			if b, ok := e["read_only"].(bool); ok && b {
				ro = " (read-only)"
			}
			fmt.Printf("  %-20s volume=%-20s %s%s\n", fmt.Sprint(e["name"]), desc, exportDetail(typ, e), ro)
		}
	}
	if !found {
		fmt.Println("no live exports")
	}
	return nil
}

// exportDetail renders the per-type extra column of the ls table.
func exportDetail(typ string, e map[string]any) string {
	switch typ {
	case "nbd":
		if n, ok := e["conns"].(float64); ok {
			return fmt.Sprintf("conns=%d", int(n))
		}
	case "vub":
		attached := "detached"
		if b, ok := e["attached"].(bool); ok && b {
			attached = "attached"
		}
		return fmt.Sprintf("socket=%v %s", e["socket"], attached)
	}
	return ""
}

// exportRmCmd detaches an export. A busy export (a VM is still
// transmitting / the device is in use) answers 409: the caller must
// detach the consumer first.
func exportRmCmd(ctx context.Context, args []string) error {
	f := newExportFlags("export rm")
	mustParse(f.fs, args)
	if f.fs.NArg() != 1 {
		return fmt.Errorf("usage: unistorctl export rm <name> [-type nbd|vduse|vub]")
	}
	name := f.fs.Arg(0)
	if err := checkExportType(*f.typ); err != nil {
		return err
	}
	url := "/v1/exports/" + name + "?type=" + *f.typ
	status, body, err := exportRequest(ctx, f, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("export rm: no %s export %q", *f.typ, name)
	}
	if status == http.StatusConflict {
		return fmt.Errorf("export rm: %s export %q is in use — detach the consumer first", *f.typ, name)
	}
	if status != http.StatusOK {
		return fmt.Errorf("export rm: %s", httpReplyError("DELETE "+url, status, body))
	}
	fmt.Printf("removed %s export %q\n", *f.typ, name)
	return nil
}

// exportRequest runs one admin HTTP call and returns the status and
// raw body (errors are the caller's formatting job, so 409/404 can
// carry command-specific meaning).
func exportRequest(ctx context.Context, f *exportFlags, method, path string, body any) (int, []byte, error) {
	url := strings.TrimRight(*f.httpURL, "/") + path
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if *f.token != "" {
		req.Header.Set("Authorization", "Bearer "+*f.token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: read reply: %w", method, url, err)
	}
	return resp.StatusCode, respBody, nil
}

func httpReplyError(op string, status int, body []byte) string {
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		return fmt.Sprintf("%s: HTTP %d", op, status)
	}
	return fmt.Sprintf("%s: HTTP %d: %s", op, status, msg)
}

func checkExportType(t string) error {
	for _, v := range exportTypes {
		if t == v {
			return nil
		}
	}
	return fmt.Errorf("export: -type must be one of %s", strings.Join(exportTypes, ", "))
}

// exportReplyName picks the export identity out of a create reply:
// vduse names the device, vub the export, nbd the registration.
func exportReplyName(reply map[string]any, fallback string) string {
	for _, k := range []string{"name", "devname"} {
		if s, ok := reply[k].(string); ok && s != "" {
			return s
		}
	}
	return fallback
}
