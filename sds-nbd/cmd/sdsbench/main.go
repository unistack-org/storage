// Command sdsbench is a benchmark client for the sdsd NBD server.
//
// It performs the newstyle handshake (OPT_GO with structured replies, like
// the kernel and QEMU do), then pipelines READ/WRITE requests at a
// configurable queue depth on one connection and reports IOPS, bandwidth
// and latency percentiles. It measures the NBD data path end to end
// (TCP + server backend) without needing a VM.
//
// Usage:
//
//	sdsbench -addr 127.0.0.1:10809 -export vol0 -rw randread -bs 4K -size 64M -qd 32 -runtime 10s
package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// NBD protocol constants (mirrors of the unexported ones in package nbd).
const (
	nbdMagic           = 0x4e42444d41474943
	ihaveoptMagic      = 0x49484156454f5054
	repMagic           = 0x3e889045565a9
	requestMagic       = 0x25609513
	simpleReplyMagic   = 0x67446698
	structuredReplyMag = 0x668e33ef

	handshakeFixedNewstyle = 1 << 0
	clientFixedNewstyle    = 1 << 0

	optGo              = 7
	optStructuredReply = 8

	repACK   = 1
	repInfo  = 3
	repError = 0x80000000

	chunkTypeNone       = 0
	chunkTypeOffsetData = 1
	chunkErrorFlag      = 1 << 15
	chunkFlagDone       = 1 << 0

	cmdRead  = 0
	cmdWrite = 1

	infoTypeExport = 0
)

func main() {
	addr := flag.String("addr", "127.0.0.1:10809", "sdsd address")
	export := flag.String("export", "vol0", "export name")
	rw := flag.String("rw", "randread", "read | seqread | randread | write | seqwrite | randwrite")
	bs := flag.String("bs", "4K", "block size (e.g. 4K, 1M)")
	size := flag.String("size", "64M", "address range to exercise")
	qd := flag.Int("qd", 32, "queue depth (in-flight requests) per connection")
	conns := flag.Int("conns", 1, "number of parallel connections (total QD = qd*conns)")
	runDur := flag.Duration("runtime", 10*time.Second, "duration (0 = one pass over size)")
	flag.Parse()

	bsBytes, err := parseSize(*bs)
	if err != nil {
		fatalf("-bs: %v", err)
	}
	sizeBytes, err := parseSize(*size)
	if err != nil {
		fatalf("-size: %v", err)
	}
	if bsBytes <= 0 || bsBytes&(bsBytes-1) != 0 {
		fatalf("-bs must be a positive power of two")
	}
	if *conns < 1 {
		*conns = 1
	}

	type connWork struct {
		cl *client
	}
	work := make([]connWork, *conns)
	exportSize := int64(0)
	for i := range work {
		cl, es, err := dial(*addr, *export)
		if err != nil {
			fatalf("connect %d: %v", i, err)
		}
		work[i] = connWork{cl: cl}
		if i == 0 {
			exportSize = es
		} else if es != exportSize {
			fatalf("export size mismatch across connections")
		}
	}
	defer func() {
		for _, w := range work {
			w.cl.Close()
		}
	}()
	if sizeBytes > exportSize {
		fatalf("-size %d exceeds export size %d", sizeBytes, exportSize)
	}

	b := &bench{}
	switch *rw {
	case "read", "seqread", "randread":
		b.read = true
	case "write", "seqwrite", "randwrite":
		b.read = false
	default:
		fatalf("-rw: %q", *rw)
	}
	b.random = strings.HasPrefix(*rw, "rand")
	b.bs = int(bsBytes)
	b.nslots = int(sizeBytes / bsBytes)
	if b.nslots == 0 {
		fatalf("-size smaller than -bs")
	}

	fmt.Printf("sdsbench: export=%q size=%d rw=%s bs=%d range=%d qd=%d conns=%d runtime=%v\n",
		*export, exportSize, *rw, bsBytes, sizeBytes, *qd, *conns, *runDur)

	type res struct {
		ops, bytes int64
		lats       []time.Duration
		err        error
	}
	results := make([]res, len(work))
	start := time.Now()
	var wgC sync.WaitGroup
	for i := range work {
		wgC.Add(1)
		go func(i int) {
			defer wgC.Done()
			results[i].ops, results[i].bytes, results[i].lats, results[i].err =
				b.run(work[i].cl, *qd, *runDur, int64(i)+1)
		}(i)
	}
	wgC.Wait()
	elapsed := time.Since(start)
	var ops, bytes int64
	var lats []time.Duration
	for _, r := range results {
		if r.err != nil {
			fatalf("bench: %v", r.err)
		}
		ops += r.ops
		bytes += r.bytes
		lats = append(lats, r.lats...)
	}
	report(*rw, ops, bytes, elapsed, lats)
}

// bench issues pipelined NBD requests on a connection. Structured-reply
// servers may answer out of order, so send timestamps are tracked per
// outstanding handle and replies are matched by handle.
type bench struct {
	read   bool
	random bool
	bs     int
	nslots int
}

func (b *bench) run(cl *client, qd int, dur time.Duration, seed int64) (int64, int64, []time.Duration, error) {
	if qd < 1 {
		qd = 1
	}
	buf := make([]byte, b.bs)
	for i := range buf {
		buf[i] = 0xA5
	}
	lats := make([]time.Duration, 0, 1<<16)
	sendAt := make(map[uint64]time.Time, qd)

	rng := rand.New(rand.NewSource(seed))
	deadline := time.Now().Add(dur)

	var ops, bytes int64
	var nextSend uint64 // request handles
	inflight := 0

	off := func(h uint64) uint64 {
		if b.random {
			return uint64(rng.Intn(b.nslots)) * uint64(b.bs)
		}
		return (h % uint64(b.nslots)) * uint64(b.bs)
	}

	send := func(h uint64) error {
		var req [28]byte
		binary.BigEndian.PutUint32(req[0:4], requestMagic)
		binary.BigEndian.PutUint16(req[4:6], 0)
		if b.read {
			binary.BigEndian.PutUint16(req[6:8], cmdRead)
		} else {
			binary.BigEndian.PutUint16(req[6:8], cmdWrite)
		}
		binary.BigEndian.PutUint64(req[8:16], h)
		binary.BigEndian.PutUint64(req[16:24], off(h))
		binary.BigEndian.PutUint32(req[24:28], uint32(b.bs))
		sendAt[h] = time.Now()
		if b.read {
			_, err := cl.conn.Write(req[:])
			return err
		}
		vec := net.Buffers{req[:], buf}
		_, err := vec.WriteTo(cl.conn)
		return err
	}

	// recv reads one complete reply (any handle) and reports its handle.
	recv := func() (uint64, error) {
		var hdr [20]byte
		if _, err := io.ReadFull(cl.br, hdr[:16]); err != nil {
			return 0, err
		}
		switch binary.BigEndian.Uint32(hdr[0:4]) {
		case simpleReplyMagic:
			errCode := binary.BigEndian.Uint32(hdr[4:8])
			got := binary.BigEndian.Uint64(hdr[8:16])
			if _, ok := sendAt[got]; !ok {
				return 0, fmt.Errorf("reply handle %d not outstanding", got)
			}
			if errCode != 0 {
				return 0, fmt.Errorf("server NBD error %d", errCode)
			}
			if b.read {
				if _, err := io.CopyN(io.Discard, cl.br, int64(b.bs)); err != nil {
					return 0, err
				}
			}
			return got, nil
		case structuredReplyMag:
			if _, err := io.ReadFull(cl.br, hdr[16:20]); err != nil {
				return 0, err
			}
			var got uint64
			for {
				flags := binary.BigEndian.Uint16(hdr[4:6])
				typ := binary.BigEndian.Uint16(hdr[6:8])
				h := binary.BigEndian.Uint64(hdr[8:16])
				length := binary.BigEndian.Uint32(hdr[16:20])
				if got == 0 {
					got = h
				} else if h != got {
					return 0, fmt.Errorf("chunk handle %d continues reply %d", h, got)
				}
				if flags&chunkFlagDone != 0 {
					// payload first, then finish
					payload := make([]byte, length)
					if length > 0 {
						if _, err := io.ReadFull(cl.br, payload); err != nil {
							return 0, err
						}
					}
					if flags&chunkErrorFlag != 0 {
						code := uint32(5) // EIO
						if len(payload) >= 4 {
							code = binary.BigEndian.Uint32(payload[:4])
						}
						return 0, fmt.Errorf("server NBD error %d", code)
					}
					return got, nil
				}
				if flags&chunkErrorFlag != 0 {
					if _, err := io.CopyN(io.Discard, cl.br, int64(length)); err != nil {
						return 0, err
					}
					return 0, fmt.Errorf("server NBD error (non-final error chunk)")
				}
				switch typ {
				case chunkTypeOffsetData, chunkTypeNone:
					if length > 0 {
						if _, err := io.CopyN(io.Discard, cl.br, int64(length)); err != nil {
							return 0, err
						}
					}
				default:
					if _, err := io.CopyN(io.Discard, cl.br, int64(length)); err != nil {
						return 0, err
					}
					return 0, fmt.Errorf("unknown chunk type %d", typ)
				}
				// next chunk header
				if _, err := io.ReadFull(cl.br, hdr[:16]); err != nil {
					return 0, err
				}
				if binary.BigEndian.Uint32(hdr[0:4]) != structuredReplyMag {
					return 0, fmt.Errorf("bad chunk magic 0x%x", binary.BigEndian.Uint32(hdr[0:4]))
				}
				if _, err := io.ReadFull(cl.br, hdr[16:20]); err != nil {
					return 0, err
				}
			}
		default:
			return 0, fmt.Errorf("bad reply magic 0x%x", binary.BigEndian.Uint32(hdr[0:4]))
		}
	}
	for {
		// Refill the pipe.
		for inflight < qd {
			if dur == 0 && nextSend >= uint64(b.nslots) {
				break
			}
			if dur > 0 && time.Now().After(deadline) {
				break
			}
			if err := send(nextSend); err != nil {
				return ops, bytes, lats, err
			}
			nextSend++
			inflight++
		}
		if inflight == 0 {
			break // one-pass mode finished
		}
		// Collect one reply (bufio batches the rest).
		h, err := recv()
		if err != nil {
			return ops, bytes, lats, err
		}
		lats = append(lats, time.Since(sendAt[h]))
		delete(sendAt, h)
		inflight--
		ops++
		bytes += int64(b.bs)
		if dur > 0 && time.Now().After(deadline) && inflight == 0 {
			break
		}
	}
	// Drain any stragglers after the deadline.
	for inflight > 0 {
		h, err := recv()
		if err != nil {
			return ops, bytes, lats, err
		}
		lats = append(lats, time.Since(sendAt[h]))
		delete(sendAt, h)
		inflight--
		ops++
		bytes += int64(b.bs)
	}
	return ops, bytes, lats, nil
}

func report(rw string, ops, bytes int64, elapsed time.Duration, lats []time.Duration) {
	if ops == 0 || elapsed == 0 || len(lats) == 0 {
		fatalf("no completed operations")
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	pct := func(p float64) time.Duration {
		return lats[int(p*float64(len(lats)-1))]
	}
	fmt.Printf("%s: IOPS=%.0f MB/s=%.1f avg=%.3fms p50=%.3fms p99=%.3fms p99.9=%.3fms max=%.3fms\n",
		rw,
		float64(ops)/elapsed.Seconds(),
		float64(bytes)/elapsed.Seconds()/(1<<20),
		time.Duration(int64(sum)/int64(len(lats))).Seconds()*1000,
		pct(0.50).Seconds()*1000, pct(0.99).Seconds()*1000,
		pct(0.999).Seconds()*1000, lats[len(lats)-1].Seconds()*1000)
}

// --- minimal newstyle NBD client (simple replies via structured chunks) ---

type client struct {
	conn net.Conn
	br   *bufio.Reader
}

func dial(addr, export string) (*client, int64, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, 0, err
	}
	cl := &client{conn: c, br: bufio.NewReaderSize(c, 1<<20)}
	size, err := cl.handshake(export)
	if err != nil {
		c.Close()
		return nil, 0, err
	}
	return cl, size, nil
}

func (c *client) Close() error { return c.conn.Close() }

func (c *client) handshake(export string) (int64, error) {
	var hs [18]byte
	if _, err := io.ReadFull(c.br, hs[:]); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint64(hs[0:8]) != nbdMagic {
		return 0, errors.New("bad init magic")
	}
	if binary.BigEndian.Uint16(hs[16:18])&handshakeFixedNewstyle == 0 {
		return 0, errors.New("server lacks fixed newstyle")
	}
	var cf [4]byte
	binary.BigEndian.PutUint32(cf[:], clientFixedNewstyle)
	if _, err := c.conn.Write(cf[:]); err != nil {
		return 0, err
	}
	// Structured replies on (needed: OPT_GO info replies come as chunks).
	if err := c.option(optStructuredReply, nil); err != nil {
		return 0, err
	}
	// OPT_GO: u32 len + name + u16 info request count.
	data := make([]byte, 4+len(export)+2)
	binary.BigEndian.PutUint32(data[0:4], uint32(len(export)))
	copy(data[4:], export)
	binary.BigEndian.PutUint16(data[4+len(export):], 0)
	replies, err := c.optionReplies(optGo, data)
	if err != nil {
		return 0, err
	}
	var size int64 = -1
	for _, r := range replies {
		if r.typ == repInfo && len(r.data) >= 10 {
			ityp := binary.BigEndian.Uint16(r.data[0:2])
			if ityp == infoTypeExport {
				size = int64(binary.BigEndian.Uint64(r.data[2:10]))
			}
		}
	}
	if size < 0 {
		return 0, errors.New("no SIZE info reply")
	}
	return size, nil
}

type optionReply struct {
	typ  uint32
	data []byte
}

func (c *client) option(opt uint32, data []byte) error {
	replies, err := c.optionReplies(opt, data)
	if err != nil {
		return err
	}
	last := replies[len(replies)-1]
	if last.typ != repACK {
		return fmt.Errorf("option %d not acked (typ %d)", opt, last.typ)
	}
	return nil
}

func (c *client) optionReplies(opt uint32, data []byte) ([]optionReply, error) {
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[0:8], ihaveoptMagic)
	binary.BigEndian.PutUint32(hdr[8:12], opt)
	binary.BigEndian.PutUint32(hdr[12:16], uint32(len(data)))
	if _, err := c.conn.Write(append(hdr[:], data...)); err != nil {
		return nil, err
	}
	var replies []optionReply
	for {
		var rh [20]byte
		if _, err := io.ReadFull(c.br, rh[:]); err != nil {
			return nil, err
		}
		if binary.BigEndian.Uint64(rh[0:8]) != repMagic {
			return nil, errors.New("bad option reply magic")
		}
		ropt := binary.BigEndian.Uint32(rh[8:12])
		typ := binary.BigEndian.Uint32(rh[12:16])
		length := binary.BigEndian.Uint32(rh[16:20])
		if ropt != opt {
			return nil, fmt.Errorf("reply for option %d want %d", ropt, opt)
		}
		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(c.br, payload); err != nil {
				return nil, err
			}
		}
		if typ&repError != 0 {
			return nil, fmt.Errorf("option %d rejected: type 0x%x %q", opt, typ, payload)
		}
		replies = append(replies, optionReply{typ: typ, data: payload})
		if typ == repACK {
			return replies, nil
		}
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "sdsbench: "+format+"\n", args...)
	os.Exit(1)
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "K"):
		mult, s = 1<<10, strings.TrimSpace(s[:len(s)-1])
	case strings.HasSuffix(s, "M"):
		mult, s = 1<<20, strings.TrimSpace(s[:len(s)-1])
	case strings.HasSuffix(s, "G"):
		mult, s = 1<<30, strings.TrimSpace(s[:len(s)-1])
	}
	var v int64
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return 0, err
	}
	return v * mult, nil
}
