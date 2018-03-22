package sheepdog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"net"
	"time"

	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/sdstack/storage/kv"
	"github.com/sdstack/storage/proxy"
	"github.com/sdstack/storage/transport"
)

type config struct {
	Ctime          uint64
	Flags          uint16
	Copies         uint8
	Store          uint8
	Shutdown       uint8
	CopyPolicy     uint8
	BlockSizeShift uint8
	Version        uint16
	Space          uint64
	Epoch          uint32
	WorkDir        string
}

// Internal strect holds data used by internal cluster engine
type ProxySheepdog struct {
	engine *kv.KV
	cfg    *config
	ln     net.Listener
	done   chan struct{}
	listen []string
}

func init() {
	proxy.RegisterProxy("sheepdog", &ProxySheepdog{})
}

func (p *ProxySheepdog) Configure(engine *kv.KV, cfg interface{}) error {
	p.cfg = &config{Ctime: uint64(time.Now().Unix()), Copies: 2, BlockSizeShift: 22, Version: 9, Epoch: 1}
	p.engine = engine
	p.done = make(chan struct{})
	return nil
}
func (p *ProxySheepdog) Start() error {
	ln, err := transport.Listen("tcp", ":7000")
	if err != nil {
		return err
	}

	p.ln = ln //netutil.LimitListener(ln, 10240)

	go func() {
		for {
			c, err := p.ln.Accept()
			if err != nil {
				fmt.Printf(err.Error())
				continue
			}
			conn := newConn(c)
			go p.handleConn(conn)
		}
	}()

	<-p.done
	return nil
}

func (p *ProxySheepdog) Stop() error {
	close(p.done)
	p.ln.Close()
	return nil
}

const (
	SD_DEFAULT_PORT                = 7000
	SD_PROTO_VER_TRIM_ZERO_SECTORS = 0x02
)

const (
	// client2sheep
	SD_OP_CREATE_AND_WRITE_OBJ sdOpcode = 0x01
	SD_OP_READ_OBJ             sdOpcode = 0x02
	SD_OP_WRITE_OBJ            sdOpcode = 0x03
	SD_OP_REMOVE_OBJ           sdOpcode = 0x04
	SD_OP_DISCARD_OBJ          sdOpcode = 0x05

	SD_OP_NEW_VDI             sdOpcode = 0x11
	SD_OP_LOCK_VDI            sdOpcode = 0x12
	SD_OP_RELEASE_VDI         sdOpcode = 0x13
	SD_OP_GET_VDI_INFO        sdOpcode = 0x14
	SD_OP_READ_VDIS           sdOpcode = 0x15
	SD_OP_FLUSH_VDI           sdOpcode = 0x16
	SD_OP_DEL_VDI             sdOpcode = 0x17
	SD_OP_GET_CLUSTER_DEFAULT sdOpcode = 0x18

	// sheep2sheep
	SD_OP_GET_NODE_LIST            sdOpcode = 0x82
	SD_OP_MAKE_FS                  sdOpcode = 0x84
	SD_OP_SHUTDOWN                 sdOpcode = 0x85
	SD_OP_STAT_SHEEP               sdOpcode = 0x86
	SD_OP_STAT_CLUSTER             sdOpcode = 0x87
	SD_OP_GET_VDI_ATTR             sdOpcode = 0x89
	SD_OP_FORCE_RECOVER            sdOpcode = 0x8a
	SD_OP_GET_STORE_LIST           sdOpcode = 0x90
	SD_OP_SNAPSHOT                 sdOpcode = 0x91
	SD_OP_RESTORE                  sdOpcode = 0x92
	SD_OP_GET_SNAP_FILE            sdOpcode = 0x93
	SD_OP_CLEANUP                  sdOpcode = 0x94
	SD_OP_TRACE_STATUS             sdOpcode = 0x95
	SD_OP_TRACE_READ_BUF           sdOpcode = 0x96
	SD_OP_STAT_RECOVERY            sdOpcode = 0x97
	SD_OP_FLUSH_DEL_CACHE          sdOpcode = 0x98
	SD_OP_NOTIFY_VDI_DEL           sdOpcode = 0x99
	SD_OP_KILL_NODE                sdOpcode = 0x9A
	SD_OP_TRACE_ENABLE             sdOpcode = 0x9B
	SD_OP_TRACE_DISABLE            sdOpcode = 0x9C
	SD_OP_GET_OBJ_LIST             sdOpcode = 0xA1
	SD_OP_GET_EPOCH                sdOpcode = 0xA2
	SD_OP_CREATE_AND_WRITE_PEER    sdOpcode = 0xA3
	SD_OP_READ_PEER                sdOpcode = 0xA4
	SD_OP_WRITE_PEER               sdOpcode = 0xA5
	SD_OP_REMOVE_PEER              sdOpcode = 0xA6
	SD_OP_ENABLE_RECOVER           sdOpcode = 0xA8
	SD_OP_DISABLE_RECOVER          sdOpcode = 0xA9
	SD_OP_GET_VDI_COPIES           sdOpcode = 0xAB
	SD_OP_COMPLETE_RECOVERY        sdOpcode = 0xAC
	SD_OP_FLUSH_NODES              sdOpcode = 0xAD
	SD_OP_FLUSH_PEER               sdOpcode = 0xAE
	SD_OP_NOTIFY_VDI_ADD           sdOpcode = 0xAF
	SD_OP_MD_INFO                  sdOpcode = 0xB1
	SD_OP_MD_PLUG                  sdOpcode = 0xB2
	SD_OP_MD_UNPLUG                sdOpcode = 0xB3
	SD_OP_GET_HASH                 sdOpcode = 0xB4
	SD_OP_REWEIGHT                 sdOpcode = 0xB5
	SD_OP_STAT                     sdOpcode = 0xB8
	SD_OP_GET_LOGLEVEL             sdOpcode = 0xB9
	SD_OP_SET_LOGLEVEL             sdOpcode = 0xBA
	SD_OP_NFS_CREATE               sdOpcode = 0xBB
	SD_OP_NFS_DELETE               sdOpcode = 0xBC
	SD_OP_EXIST                    sdOpcode = 0xBD
	SD_OP_CLUSTER_INFO             sdOpcode = 0xBE
	SD_OP_ALTER_CLUSTER_COPY       sdOpcode = 0xBF
	SD_OP_ALTER_VDI_COPY           sdOpcode = 0xC0
	SD_OP_DECREF_OBJ               sdOpcode = 0xC1
	SD_OP_DECREF_PEER              sdOpcode = 0xC2
	SD_OP_REPAIR_REPLICA           sdOpcode = 0xC5
	SD_OP_OIDS_EXIST               sdOpcode = 0xC6
	SD_OP_VDI_STATE_CHECKPOINT_CTL sdOpcode = 0xC7
	SD_OP_INODE_COHERENCE          sdOpcode = 0xC8
	SD_OP_READ_DEL_VDIS            sdOpcode = 0xC9
	SD_OP_GET_RECOVERY             sdOpcode = 0xCA
	SD_OP_SET_RECOVERY             sdOpcode = 0xCB
	SD_OP_SET_VNODES               sdOpcode = 0xCC
	SD_OP_GET_VNODES               sdOpcode = 0xCD
)

var opcodeTypes = map[sdOpcode]string{
	SD_OP_CREATE_AND_WRITE_OBJ: "SD_OP_CREATE_AND_WRITE_OBJ",
	SD_OP_READ_OBJ:             "SD_OP_READ_OBJ",
	SD_OP_WRITE_OBJ:            "SD_OP_WRITE_OBJ",
	SD_OP_NEW_VDI:              "SD_OP_NEW_VDI",
	SD_OP_LOCK_VDI:             "SD_OP_LOCK_VDI",
	SD_OP_RELEASE_VDI:          "SD_OP_RELEASE_VDI",
	SD_OP_GET_VDI_INFO:         "SD_OP_GET_VDI_INFO",
	SD_OP_READ_VDIS:            "SD_OP_READ_VDIS",
	SD_OP_FLUSH_VDI:            "SD_OP_FLUSH_VDI",
	SD_OP_DEL_VDI:              "SD_OP_DEL_VDI",
	SD_OP_GET_CLUSTER_DEFAULT:  "SD_OP_GET_CLUSTER_DEFAULT",

	SD_OP_GET_NODE_LIST:            "SD_OP_GET_NODE_LIST",
	SD_OP_MAKE_FS:                  "SD_OP_MAKE_FS",
	SD_OP_SHUTDOWN:                 "SD_OP_SHUTDOWN",
	SD_OP_STAT_SHEEP:               "SD_OP_STAT_SHEEP",
	SD_OP_STAT_CLUSTER:             "SD_OP_STAT_CLUSTER",
	SD_OP_GET_VDI_ATTR:             "SD_OP_GET_VDI_ATTR",
	SD_OP_FORCE_RECOVER:            "SD_OP_FORCE_RECOVER",
	SD_OP_GET_STORE_LIST:           "SD_OP_GET_STORE_LIST",
	SD_OP_SNAPSHOT:                 "SD_OP_SNAPSHOT",
	SD_OP_RESTORE:                  "SD_OP_RESTORE",
	SD_OP_GET_SNAP_FILE:            "SD_OP_GET_SNAP_FILE",
	SD_OP_CLEANUP:                  "SD_OP_CLEANUP",
	SD_OP_TRACE_STATUS:             "SD_OP_TRACE_STATUS",
	SD_OP_TRACE_READ_BUF:           "SD_OP_TRACE_READ_BUF",
	SD_OP_STAT_RECOVERY:            "SD_OP_STAT_RECOVERY",
	SD_OP_FLUSH_DEL_CACHE:          "SD_OP_FLUSH_DEL_CACHE",
	SD_OP_NOTIFY_VDI_DEL:           "SD_OP_NOTIFY_VDI_DEL",
	SD_OP_KILL_NODE:                "SD_OP_KILL_NODE",
	SD_OP_TRACE_ENABLE:             "SD_OP_TRACE_ENABLE",
	SD_OP_TRACE_DISABLE:            "SD_OP_TRACE_DISABLE",
	SD_OP_GET_OBJ_LIST:             "SD_OP_GET_OBJ_LIST",
	SD_OP_GET_EPOCH:                "SD_OP_GET_EPOCH",
	SD_OP_CREATE_AND_WRITE_PEER:    "SD_OP_CREATE_AND_WRITE_PEER",
	SD_OP_READ_PEER:                "SD_OP_READ_PEER",
	SD_OP_WRITE_PEER:               "SD_OP_WRITE_PEER",
	SD_OP_REMOVE_PEER:              "SD_OP_REMOVE_PEER",
	SD_OP_ENABLE_RECOVER:           "SD_OP_ENABLE_RECOVER",
	SD_OP_DISABLE_RECOVER:          "SD_OP_DISABLE_RECOVER",
	SD_OP_GET_VDI_COPIES:           "SD_OP_GET_VDI_COPIES",
	SD_OP_COMPLETE_RECOVERY:        "SD_OP_COMPLETE_RECOVERY",
	SD_OP_FLUSH_NODES:              "SD_OP_FLUSH_NODES",
	SD_OP_FLUSH_PEER:               "SD_OP_FLUSH_PEER",
	SD_OP_NOTIFY_VDI_ADD:           "SD_OP_NOTIFY_VDI_ADD",
	SD_OP_MD_INFO:                  "SD_OP_MD_INFO",
	SD_OP_MD_PLUG:                  "SD_OP_MD_PLUG",
	SD_OP_MD_UNPLUG:                "SD_OP_MD_UNPLUG",
	SD_OP_GET_HASH:                 "SD_OP_GET_HASH",
	SD_OP_REWEIGHT:                 "SD_OP_REWEIGHT",
	SD_OP_STAT:                     "SD_OP_STAT",
	SD_OP_GET_LOGLEVEL:             "SD_OP_GET_LOGLEVEL",
	SD_OP_SET_LOGLEVEL:             "SD_OP_SET_LOGLEVEL",
	SD_OP_NFS_CREATE:               "SD_OP_NFS_CREATE",
	SD_OP_NFS_DELETE:               "SD_OP_NFS_DELETE",
	SD_OP_EXIST:                    "SD_OP_EXIST",
	SD_OP_CLUSTER_INFO:             "SD_OP_CLUSTER_INFO",
	SD_OP_ALTER_CLUSTER_COPY:       "SD_OP_ALTER_CLUSTER_COPY",
	SD_OP_ALTER_VDI_COPY:           "SD_OP_ALTER_VDI_COPY",
	SD_OP_DECREF_OBJ:               "SD_OP_DECREF_OBJ",
	SD_OP_DECREF_PEER:              "SD_OP_DECREF_PEER",
	SD_OP_REPAIR_REPLICA:           "SD_OP_REPAIR_REPLICA",
	SD_OP_OIDS_EXIST:               "SD_OP_OIDS_EXIST",
	SD_OP_VDI_STATE_CHECKPOINT_CTL: "SD_OP_VDI_STATE_CHECKPOINT_CTL",
	SD_OP_INODE_COHERENCE:          "SD_OP_INODE_COHERENCE",
	SD_OP_READ_DEL_VDIS:            "SD_OP_READ_DEL_VDIS",
	SD_OP_GET_RECOVERY:             "SD_OP_GET_RECOVERY",
	SD_OP_SET_RECOVERY:             "SD_OP_SET_RECOVERY",
	SD_OP_SET_VNODES:               "SD_OP_SET_VNODES",
	SD_OP_GET_VNODES:               "SD_OP_GET_VNODES",
}

type ClusterInfo struct {
	Proto          uint8
	Recovery       uint8
	NrNodes        uint16
	Epoch          uint32
	Ctime          uint64
	Flags          uint16
	NrCopies       uint8
	CopyPolicy     uint8
	Status         uint8
	BlockSizeShift uint8
	_              [3]byte
	DefaultStore   []byte
	//Node           []SDNodes
}

func (o sdOpcode) String() string {
	return opcodeTypes[o]
}

const (
	SD_HDR_SIZE = 16
	SD_REQ_SIZE = 48
	SD_RSP_SIZE = 48

	SECTOR_SIZE = 512 // 1 << 9 // get via ioctl BLKBSZGET
)

const (
	SD_MAX_VDI_LEN              = 256
	SD_MAX_VDI_TAG_LEN          = 256
	SD_MAX_VDI_ATTR_KEY_LEN     = 256
	SD_MAX_VDI_ATTR_VALUE_LEN   = 65536
	SD_MAX_SNAPSHOT_TAG_LEN     = 256
	SD_DEFAULT_BLOCK_SIZE_SHIFT = 22
	SD_NR_VDIS                  = uint64(1) << 24
)

const (
	SD_FLAG_CMD_WRITE     = 0x01
	SD_FLAG_CMD_COW       = 0x02
	SD_FLAG_CMD_CACHE     = 0x04 /* Writeback mode for cache */
	SD_FLAG_CMD_DIRECT    = 0x08 /* Don't use cache */
	SD_FLAG_CMD_PIGGYBACK = 0x10
	SD_FLAG_CMD_FWD       = 0x40
)

const (
	SD_RES_SUCCESS         = uint32(0x00) /* Success */
	SD_RES_UNKNOWN         = uint32(0x01) /* Unknown error */
	SD_RES_NO_OBJ          = uint32(0x02) /* No object found */
	SD_RES_EIO             = uint32(0x03) /* I/O error */
	SD_RES_VDI_EXIST       = uint32(0x04) /* Vdi exists already */
	SD_RES_INVALID_PARMS   = uint32(0x05) /* Invalid parameters */
	SD_RES_SYSTEM_ERROR    = uint32(0x06) /* System error */
	SD_RES_VDI_LOCKED      = uint32(0x07) /* Vdi is locked */
	SD_RES_NO_VDI          = uint32(0x08) /* No vdi found */
	SD_RES_NO_BASE_VDI     = uint32(0x09) /* No base vdi found */
	SD_RES_VDI_READ        = uint32(0x0A) /* Cannot read requested vdi */
	SD_RES_VDI_WRITE       = uint32(0x0B) /* Cannot write requested vdi */
	SD_RES_BASE_VDI_READ   = uint32(0x0C) /* Cannot read base vdi */
	SD_RES_BASE_VDI_WRITE  = uint32(0x0D) /* Cannot write base vdi */
	SD_RES_NO_TAG          = uint32(0x0E) /* Requested tag is not found */
	SD_RES_STARTUP         = uint32(0x0F) /* Sheepdog is on starting up */
	SD_RES_VDI_NOT_LOCKED  = uint32(0x10) /* Vdi is not locked */
	SD_RES_SHUTDOWN        = uint32(0x11) /* Sheepdog is shutting down */
	SD_RES_NO_MEM          = uint32(0x12) /* Cannot allocate memory */
	SD_RES_FULL_VDI        = uint32(0x13) /* we already have the maximum vdis */
	SD_RES_VER_MISMATCH    = uint32(0x14) /* Protocol version mismatch */
	SD_RES_NO_SPACE        = uint32(0x15) /* Server has no room for new objects */
	SD_RES_WAIT_FOR_FORMAT = uint32(0x16) /* Waiting for a format operation */
	SD_RES_WAIT_FOR_JOIN   = uint32(0x17) /* Waiting for other nodes joining */
	SD_RES_JOIN_FAILED     = uint32(0x18) /* Target node had failed to join sheepdog */
	SD_RES_HALT            = uint32(0x19) /* Sheepdog is stopped serving IO request */
	SD_RES_READONLY        = uint32(0x1A) /* Object is read-only */

	SD_RES_INODE_INVALIDATED = uint32(0x1D)
)

const (
	SD_EC_MAX_STRIP = 16
	SD_MAX_COPIES   = (SD_EC_MAX_STRIP*2 - 1)

	CURRENT_VDI_ID = 0

	LOCK_TYPE_NORMAL = 0
	LOCK_TYPE_SHARED = 1 /* for iSCSI multipath */

	VDI_BIT       = uint64(1) << 63
	VMSTATE_BIT   = uint64(1) << 62
	VDI_ATTR_BIT  = uint64(1) << 61
	VDI_BTREE_BIT = uint64(1) << 60
	LEDGER_BIT    = uint64(1) << 59

	VDI_SPACE_SHIFT = 32
	SD_VDI_MASK     = 0x00FFFFFF00000000

	MAX_DATA_OBJS = uint64(1) << 32
	//	MAX_CHILDREN = uint64(1) << 20 // STACK ERROR

	SD_INODE_SIZE            = 10
	SD_INODE_INDEX_SIZE      = 4 * MAX_DATA_OBJS
	SD_INODE_DATA_INDEX      = uint64(1) << 20
	SD_INODE_DATA_INDEX_SIZE = 4 * SD_INODE_DATA_INDEX
	//	SD_INODE_HEADER_SIZE
	SD_INODE_HEADER_SIZE = 4664
	SD_LEDGER_OBJ_SIZE   = uint64(1) << 22
	OLD_MAX_CHILDREN     = uint64(1024)
)

type generation_reference struct {
	generation int32
	count      int32
}

type NodeID struct {
	Addr      [16]byte
	Port      uint16
	IOAddr    [16]byte
	IOPort    uint16
	Reserved1 [4]byte
}

type DiskInfo struct {
	DiskID    uint64
	DiskSpace uint64
}

type Node struct {
	//RB       RBNode
	NID      NodeID
	VNodes   uint16
	Zone     uint32
	Space    uint64
	DiskInfo [0]DiskInfo
}

type GenRef struct {
	Generation uint32
	Count      uint32
}

type Inode struct {
	Name           [SD_MAX_VDI_LEN]byte
	Tag            [SD_MAX_VDI_TAG_LEN]byte
	Ctime          uint64
	SnapTime       uint64
	VMClockNSec    uint64
	VDISize        uint64
	VMStateSize    uint64
	CopyPolicy     uint8
	StorePolicy    uint8
	Copies         uint8
	BlockSizeShift uint8
	SnapId         uint32
	VdiId          uint32
	ParentVdiId    uint32
	BtreeCounter   uint32
	_              [OLD_MAX_CHILDREN - 1]byte
	DataVdiId      [SD_INODE_DATA_INDEX]uint32
	Gref           [SD_INODE_DATA_INDEX]GenRef
	//	CsumData       [SD_INODE_DATA_INDEX]uint32

	// 256*2+8*5+1*4+4*4+1024-1+1048576*4+1048576*8
	// 12584507
}

type sdOpcode uint8

type SheepdogHdr struct {
	Proto   uint8  // 1
	Opcode  uint8  // 1
	Flags   uint16 // 2
	Epoch   uint32 // 4
	ID      uint32 // 4
	DataLen uint32 // 4
	// 16 byte
}

func (c *Conn) Close() error {
	c.bpool.Close()
	return c.c.Close()
}

type Conn struct {
	c            net.Conn
	sdObjReq     *SheepdogObjReq
	sdVdiReq     *SheepdogVdiReq
	sdClusterReq *SheepdogClusterReq
	sdIntReq     *SheepdogIntReq
	sdRawReq     *SheepdogRawReq
	sdRawRsp     *SheepdogRawRsp
	sdObjRsp     *SheepdogObjRsp
	sdVdiRsp     *SheepdogVdiRsp
	sdClusterRsp *SheepdogClusterRsp
	sdIntRsp     *SheepdogIntRsp
	bpool        *util.BufferPool
}

func newConn(c net.Conn) *Conn {
	conn := &Conn{
		c:            c,
		sdObjReq:     &SheepdogObjReq{},
		sdVdiReq:     &SheepdogVdiReq{},
		sdClusterReq: &SheepdogClusterReq{},
		sdIntReq:     &SheepdogIntReq{},
		sdRawReq:     &SheepdogRawReq{},
		sdObjRsp:     &SheepdogObjRsp{},
		sdVdiRsp:     &SheepdogVdiRsp{},
		sdRawRsp:     &SheepdogRawRsp{},
		sdClusterRsp: &SheepdogClusterRsp{},
		sdIntRsp:     &SheepdogIntRsp{},
		bpool:        util.NewBufferPool(4 * 1024 * 1024),
	}
	return conn
}

func (c *Conn) writeClusterRsp() error {
	var err error

	buf := c.bpool.Get(SD_RSP_SIZE)
	defer c.bpool.Put(buf)

	buf[0] = c.sdClusterRsp.Proto
	buf[1] = c.sdClusterRsp.Opcode
	binary.LittleEndian.PutUint16(buf[2:4], c.sdClusterRsp.Flags)
	binary.LittleEndian.PutUint32(buf[4:8], c.sdClusterRsp.Epoch)
	binary.LittleEndian.PutUint32(buf[8:12], c.sdClusterRsp.ID)
	binary.LittleEndian.PutUint32(buf[12:16], c.sdClusterRsp.DataLen)
	binary.LittleEndian.PutUint32(buf[16:20], c.sdClusterRsp.Result)
	buf[20] = c.sdClusterRsp.NrCopies
	buf[21] = c.sdClusterRsp.CopyPolicy
	buf[22] = c.sdClusterRsp.BlockSizeShift

	_, err = c.c.Write(buf)
	return err
}

func (c *Conn) writeRawRsp() error {
	var err error

	buf := c.bpool.Get(SD_RSP_SIZE)
	defer c.bpool.Put(buf)

	buf[0] = c.sdRawRsp.Proto
	buf[1] = c.sdRawRsp.Opcode
	binary.LittleEndian.PutUint16(buf[2:4], c.sdRawRsp.Flags)
	binary.LittleEndian.PutUint32(buf[4:8], c.sdRawRsp.Epoch)
	binary.LittleEndian.PutUint32(buf[8:12], c.sdRawRsp.ID)
	binary.LittleEndian.PutUint32(buf[12:16], c.sdRawRsp.DataLen)
	binary.LittleEndian.PutUint32(buf[16:20], c.sdRawRsp.Result)

	//	binary.LittleEndian.PutUint32(buf[20:24], c.sdVdiRsp.Rsvd)
	//	binary.LittleEndian.PutUint32(buf[24:28], c.sdVdiRsp.VdiID)
	//	binary.LittleEndian.PutUint32(buf[28:32], c.sdVdiRsp.AttrID)
	//buf[32] = c.sdVdiRsp.Copies
	//buf[33] = c.sdVdiRsp.BlockSizeShift

	_, err = c.c.Write(buf)
	return err
}

func (c *Conn) writeVdiRsp(buf []byte) error {
	var err error
	var n int64

	hdr := c.bpool.Get(SD_RSP_SIZE)
	defer c.bpool.Put(hdr)

	hdr[0] = c.sdVdiRsp.Proto
	hdr[1] = c.sdVdiRsp.Opcode
	binary.LittleEndian.PutUint16(hdr[2:4], c.sdVdiRsp.Flags)
	binary.LittleEndian.PutUint32(hdr[4:8], c.sdVdiRsp.Epoch)
	binary.LittleEndian.PutUint32(hdr[8:12], c.sdVdiRsp.ID)
	binary.LittleEndian.PutUint32(hdr[12:16], c.sdVdiRsp.DataLen)
	binary.LittleEndian.PutUint32(hdr[16:20], c.sdVdiRsp.Result)

	binary.LittleEndian.PutUint32(hdr[20:24], c.sdVdiRsp.Rsvd)
	binary.LittleEndian.PutUint32(hdr[24:28], c.sdVdiRsp.VdiID)
	binary.LittleEndian.PutUint32(hdr[28:32], c.sdVdiRsp.AttrID)
	hdr[32] = c.sdVdiRsp.Copies
	hdr[33] = c.sdVdiRsp.BlockSizeShift

	bw := net.Buffers([][]byte{hdr, buf})
	n, err = bw.WriteTo(c.c)
	if n < int64(SD_RSP_SIZE+len(buf)) || err != nil {
		return fmt.Errorf("incomplete write")
	}
	return err
}

func (c *Conn) writeObjRsp(buf []byte) error {
	var err error
	var n int64

	hdr := c.bpool.Get(SD_RSP_SIZE)
	defer c.bpool.Put(hdr)

	hdr[0] = c.sdObjRsp.Proto
	hdr[1] = c.sdObjRsp.Opcode
	binary.LittleEndian.PutUint16(hdr[2:4], c.sdObjRsp.Flags)
	binary.LittleEndian.PutUint32(hdr[4:8], c.sdObjRsp.Epoch)
	binary.LittleEndian.PutUint32(hdr[8:12], c.sdObjRsp.ID)
	binary.LittleEndian.PutUint32(hdr[12:16], c.sdObjRsp.DataLen)
	binary.LittleEndian.PutUint32(hdr[16:20], c.sdObjRsp.Result)

	// may be move after checking
	hdr[20] = c.sdObjRsp.Copies
	hdr[21] = c.sdObjRsp.CopyPolicy
	hdr[22] = c.sdObjRsp.StorePolicy
	binary.LittleEndian.PutUint64(hdr[24:32], c.sdObjRsp.Offset)

	bw := net.Buffers([][]byte{hdr, buf})
	n, err = bw.WriteTo(c.c)
	if n < int64(SD_RSP_SIZE+len(buf)) || err != nil {
		return fmt.Errorf("incomplete write")
	}

	return err
}

func name2vdi(name []byte) uint32 {
	h := fnv.New64a()
	h.Write(name)
	defer h.Reset()
	return uint32(h.Sum64() & (SD_NR_VDIS - 1))
}

var (
	endian = binary.LittleEndian
)

/*
func discard_oid(cfg *Config, oID uint32) error {
	oflags := os.O_WRONLY
	fp, err := os.OpenFile(oid2filepath(cfg, uint64(oID)), oflags, os.FileMode(0660))
	if err != nil {
		return err
	}
	defer fp.Close()
	return unix.Fallocate(int(fp.Fd()), unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE, 0, 1<<cfg.BlockSizeShift)
}
*/
func is_discard_req(req *SheepdogObjReq, buf *bytes.Buffer) bool { // bytes.Equal uses asm and sse
	if !is_data_obj(req.OID) && req.DataLen == 4 && bytes.Equal(buf.Bytes(), []byte{0x0, 0x0, 0x0, 0x0}) {
		return true
	}
	return false
}

func is_vdi_obj(oid uint64) bool {
	return (oid & VDI_BIT) != 0
}

func is_vmstate_obj(oid uint64) bool {
	return (oid & VMSTATE_BIT) != 0
}

func is_vdi_attr_obj(oid uint64) bool {
	return (oid & VDI_ATTR_BIT) != 0
}

func is_vdi_btree_obj(oid uint64) bool {
	return (oid & VDI_BTREE_BIT) != 0
}

func is_ledger_object(oid uint64) bool {
	return (oid & LEDGER_BIT) != 0
}

func is_data_obj(oid uint64) bool {
	return !is_vdi_obj(oid) && is_vmstate_obj(oid) &&
		!is_vdi_attr_obj(oid) && is_vdi_btree_obj(oid) &&
		!is_ledger_object(oid)
}

//func is_erasure_oid(oid uint64) bool {
//	return is_data_obj(oid) && get_vdi_copy_policy(oid2vid(oid)) > 0
//}

func sector_aligned(x uint64) bool {
	return ((x) & (SECTOR_SIZE - 1)) == 0
}

func req_aligned(offset uint64, length uint32) bool {
	return sector_aligned(offset) && sector_aligned(uint64(length))
}

/*
func oid2filepath(cfg *Config, oID uint64) string {
	return fmt.Sprintf(
		"%s%c%x%c%016x",
		cfg.WorkDir, filepath.Separator, oid2vid(oID), filepath.Separator, oID,
	)
}
*/
/*
func vdi2filepath(cfg *Config, vdiID uint32) string {
	file_path := filepath.Join(cfg.WorkDir, fmt.Sprintf("%x", uint64(vdiID)))
	file_name := fmt.Sprintf("%x", VDI_BIT|(uint64(vdiID)<<VDI_SPACE_SHIFT))
	return filepath.Join(file_path, file_name)
}
*/
func oid2vid(oid uint64) uint32 {
	return uint32((oid &^ VDI_BIT) >> VDI_SPACE_SHIFT)
}

func data_oid_to_idx(oid uint64) uint64 {
	return oid & (MAX_DATA_OBJS - 1)
}

func vid_to_vdi_oid(vid uint32) uint64 {
	return VDI_BIT | (uint64(vid) << VDI_SPACE_SHIFT)
}

func vid_to_data_oid(vid uint32, idx uint64) uint64 {
	return (uint64(vid) << VDI_SPACE_SHIFT) | idx
}

func oid_to_vid(oid uint64) uint32 {
	return uint32((oid & SD_VDI_MASK) >> VDI_SPACE_SHIFT)
}

func vid_to_attr_oid(vid uint32, attrid uint32) uint64 {
	return (uint64(vid) << VDI_SPACE_SHIFT) | VDI_ATTR_BIT | uint64(attrid)
}

func vid_to_btree_oid(vid uint32, btreeid uint32) uint64 {
	return (uint64(vid) << VDI_SPACE_SHIFT) | VDI_BTREE_BIT | uint64(btreeid)
}

func vid_to_vmstate_oid(vid uint32, idx uint32) uint64 {
	return VMSTATE_BIT | (uint64(vid) << VDI_SPACE_SHIFT) | uint64(idx)
}

func vdi_is_snapshot(inode *Inode) bool {

	return inode.SnapTime != 0
}

func ledger_oid_to_data_oid(oid uint64) uint64 {
	return LEDGER_BIT ^ oid
}

func data_oid_to_ledger_oid(oid uint64) uint64 {
	return LEDGER_BIT | oid
}

type SheepdogObjReq struct {
	SheepdogHdr
	SheepdogObjReqData
}

type SheepdogObjReqData struct {
	OID         uint64   //8
	CowOID      uint64   //8
	Copies      uint8    //1
	CopyPolicy  uint8    //1
	StorePolicy uint8    //1
	_           [5]uint8 //5
	Offset      uint64   //8
}

type SheepdogObjRsp struct {
	SheepdogHdr
	SheepdogObjRspData
}

type SheepdogObjRspData struct {
	Result      uint32 // 4
	Copies      uint8  // 1
	CopyPolicy  uint8  // 1
	StorePolicy uint8  // 1
	_           [1]byte
	Offset      uint64
	_           [16]byte
}

type SheepdogIntRsp struct {
	SheepdogHdr
	SheepdogIntRspData
}

type SheepdogIntRspData struct {
	Result    uint32
	_         [8]byte
	NrNodes   uint32
	_         [16]byte
	StoreSize uint64
	StoreFree uint64
}

type SheepdogRawReq struct {
	SheepdogHdr
	SheepdogRawReqData []byte
}

type SheepdogRawRsp struct {
	SheepdogHdr
	SheepdogRawRspData
}

type SheepdogRawRspData struct {
	Result uint32
	_      [28]byte
}

type SheepdogNodeRsp struct {
	SheepdogHdr
	SheepdogNodeRspData
}

type SheepdogNodeRspData struct {
	Result    uint32
	NrNodes   uint32
	_         [8]byte
	StoreSize uint64
	StoreFree uint64
}

type SheepdogHashRsp struct {
	SheepdogHdr
	SheepdogHashRspData
}

type SheepdogHashRspData struct {
	Result uint32
	_      [8]byte
	Digest [20]byte
}

type SheepdogVdiReq struct {
	SheepdogHdr
	SheepdogVdiReqData
}

type SheepdogVdiReqData struct {
	Size           uint64
	Base           uint32
	Copies         uint8
	CopyPolicy     uint8
	StorePolicy    uint8
	BlockSizeShift uint8
	SnapshotID     uint32
	Type           uint32
	_              [8]byte
}

type SheepdogVdiRsp struct {
	SheepdogHdr
	SheepdogVdiRspData
}

type SheepdogVdiRspData struct {
	Result         uint32
	Rsvd           uint32
	VdiID          uint32
	AttrID         uint32
	Copies         uint8
	BlockSizeShift uint8
	_              [14]byte
}

type SheepdogClusterRsp struct {
	SheepdogHdr
	SheepdogClusterRspData
}

type SheepdogClusterRspData struct {
	Result         uint32
	NrCopies       uint8
	CopyPolicy     uint8
	BlockSizeShift uint8
	_              [25]byte
}

type SheepdogClusterReq struct {
	SheepdogHdr
	SheepdogClusterReqData
}

type SheepdogClusterReqData struct {
	OID            uint64
	Ctime          uint64
	Copies         uint8
	CopyPolicy     uint8
	Cflags         uint16
	Tag            uint32
	NrNodes        uint32
	BlockSizeShift uint8
	_              [3]byte
}

type SheepdogVdiStateReq struct {
	SheepdogHdr
	SheepdogVdiStateReqData
}

type SheepdogVdiStateReqData struct {
	OldVID         uint32
	NewVID         uint32
	Copies         uint8
	SetBitmap      uint8
	CopyPolicy     uint8
	BlockSizeShift uint8
	_              [20]byte
}

type SheepdogIntReq struct {
	SheepdogHdr
	SheepdogIntReqData
}

type SheepdogIntReqData struct {
	Result  uint32
	NrNodes uint32
}

type SheepdogRefReq struct {
	SheepdogHdr
	SheepdogRefReqData
}

type SheepdogRefReqData struct {
	OID        uint64
	Generation uint32
	Count      uint32
	_          [20]byte
}

type SheepdogFwdReq struct {
	SheepdogHdr
	SheepdogFwdReqData
}

type SheepdogFwdReqData struct {
	OID  uint64
	Addr [16]byte
	Port uint16
	_    [6]byte
}

type SheepdogVdiStateChkpntReq struct {
	SheepdogHdr
	SheepdogVdiStateChkpntReqData
}

type SheepdogVdiStateChkpntReqData struct {
	Get      uint32
	TgtEpoch uint32
	VID      uint32
	_        [20]byte
}

type SheepdogInodeCohReq struct {
	SheepdogHdr
	SheepdogInodeCohReqData
}

type SheepdogInodeCohReqData struct {
	VID      uint32
	Validate uint32
	_        [24]byte
}

func (p *ProxySheepdog) sdGetClusterDefault(c *Conn) error {

	c.sdClusterRsp.SheepdogHdr = c.sdClusterReq.SheepdogHdr

	if p.cfg.Version == 0 {
		c.sdClusterRsp.Result = SD_RES_WAIT_FOR_FORMAT
		return c.writeClusterRsp()
	}

	c.sdClusterRsp.Epoch = p.cfg.Epoch
	c.sdClusterRsp.Result = SD_RES_SUCCESS
	c.sdClusterRsp.NrCopies = p.cfg.Copies
	c.sdClusterRsp.CopyPolicy = p.cfg.CopyPolicy
	c.sdClusterRsp.BlockSizeShift = p.cfg.BlockSizeShift
	return c.writeClusterRsp()
}

func (p *ProxySheepdog) sdReadVdis(c *Conn) error {
	var err error
	//var n int
	c.sdRawRsp.SheepdogHdr = c.sdRawReq.SheepdogHdr
	c.sdRawRsp.Result = SD_RES_EIO
	if p.cfg.Version == 0 {
		c.sdRawRsp.Result = SD_RES_WAIT_FOR_FORMAT
		c.writeRawRsp()
		return err
	}
	c.sdRawRsp.Result = SD_RES_SUCCESS
	c.sdRawRsp.Epoch = p.cfg.Epoch
	//	c.sdRawRsp.SheepdogRawRspData = make([]byte, 10)
	//	c.sdRawRsp.SheepdogRawRspData[0] = byte(1)

	return c.writeRawRsp()
}

func (p *ProxySheepdog) sdGetVdiInfo(c *Conn) error {
	var err error
	var ndata int
	var nparity int

	//fmt.Printf("sdGetVdiInfo\n")
	c.sdVdiRsp.SheepdogHdr = c.sdVdiReq.SheepdogHdr
	c.sdVdiRsp.Result = SD_RES_EIO
	if p.cfg.Version == 0 {
		c.sdVdiRsp.Result = SD_RES_WAIT_FOR_FORMAT
		c.writeVdiRsp(nil)
		return err
	}

	ndata = int(c.sdVdiReq.CopyPolicy)
	nparity = int(c.sdVdiReq.StorePolicy)
	if ndata == 0 {
		ndata = int(p.cfg.Copies)
	}
	c.sdVdiRsp.Epoch = p.cfg.Epoch

	buf := c.bpool.Get(int(c.sdVdiReq.DataLen))
	defer c.bpool.Put(buf)
	_, err = c.c.Read(buf)
	if err != nil {
		c.writeVdiRsp(nil)
		return err
	}

	vdiID := name2vdi(bytes.Trim(buf[:SD_MAX_VDI_LEN], "\x00"))
	file_name := fmt.Sprintf("%016x", VDI_BIT|(uint64(vdiID)<<VDI_SPACE_SHIFT))

	exists, err := p.engine.Exists(file_name, ndata, nparity)
	if err != nil {
		c.writeVdiRsp(nil)
		return err
	}
	if !exists {
		c.sdVdiRsp.Result = SD_RES_NO_VDI
	} else {
		c.sdVdiRsp.Result = SD_RES_SUCCESS
		c.sdVdiRsp.VdiID = vdiID
	}

	return c.writeVdiRsp(nil)
}

func (p *ProxySheepdog) sdNewVdi(c *Conn) error {
	var err error
	var ndata int
	var nparity int

	//var n uint64
	//fmt.Printf("sdNewVdi\n")
	c.sdVdiRsp.SheepdogHdr = c.sdVdiReq.SheepdogHdr
	c.sdVdiRsp.Result = SD_RES_EIO

	if p.cfg.Version == 0 {
		c.sdVdiRsp.Result = SD_RES_WAIT_FOR_FORMAT
		c.writeVdiRsp(nil)
		return err
	}
	c.sdVdiRsp.Epoch = p.cfg.Epoch

	buf := c.bpool.Get(int(c.sdVdiReq.DataLen))
	defer c.bpool.Put(buf)
	_, err = c.c.Read(buf)
	if err != nil {
		c.writeVdiRsp(nil)
		return err
	}

	vdiID := name2vdi(bytes.Trim(buf, "\x00"))
	file_name := fmt.Sprintf("%016x", VDI_BIT|(uint64(vdiID)<<VDI_SPACE_SHIFT))

	//oflags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	ndata = int(c.sdVdiReq.CopyPolicy)
	nparity = int(c.sdVdiReq.StorePolicy)
	if ndata == 0 {
		ndata = int(p.cfg.Copies)
	}
	exists, err := p.engine.Exists(file_name, ndata, nparity)
	if err != nil {
		c.writeVdiRsp(nil)
		return err
	}
	if exists {
		c.sdVdiRsp.Result = SD_RES_VDI_EXIST
		c.writeVdiRsp(nil)
		return err
	}

	inode := Inode{
		Ctime:          uint64(time.Now().Unix()),
		VDISize:        c.sdVdiReq.Size,
		CopyPolicy:     c.sdVdiReq.CopyPolicy,
		StorePolicy:    c.sdVdiReq.StorePolicy,
		Copies:         c.sdVdiReq.Copies,
		BlockSizeShift: c.sdVdiReq.BlockSizeShift,
		VdiId:          vdiID,
		ParentVdiId:    c.sdVdiReq.Base,
	}
	if inode.Copies == 0 {
		inode.Copies = p.cfg.Copies
	}
	if inode.BlockSizeShift == 0 {
		inode.BlockSizeShift = p.cfg.BlockSizeShift
	}
	if inode.CopyPolicy == 0 {
		inode.CopyPolicy = p.cfg.CopyPolicy
	}

	copy(inode.Name[:], buf)

	b := bytes.NewBuffer(nil)
	defer b.Reset()

	if err = binary.Write(b, binary.LittleEndian, inode); err != nil {
		c.writeVdiRsp(nil)
		return err
	}

	_, err = p.engine.WriteAt(file_name, b.Bytes(), 0, ndata, 0)
	if err != nil {
		c.writeVdiRsp(nil)
		return err
	}

	c.sdVdiRsp.Result = SD_RES_SUCCESS
	c.sdVdiRsp.VdiID = vdiID

	return c.writeVdiRsp(nil)
}

func (p *ProxySheepdog) sdCreateWriteObj(c *Conn) error {
	var err error
	var l uint32
	var n int
	var ndata int
	var nparity int
	//fmt.Printf("sdCreateWriteObj\n")
	c.sdObjRsp.SheepdogHdr = c.sdObjReq.SheepdogHdr
	c.sdObjRsp.Result = SD_RES_EIO
	if p.cfg.Version == 0 {
		c.sdObjRsp.Result = SD_RES_WAIT_FOR_FORMAT
		c.writeObjRsp(nil)
		return err
	}
	c.sdObjRsp.Epoch = p.cfg.Epoch

	buf := c.bpool.Get(int(c.sdObjReq.DataLen))
	defer c.bpool.Put(buf)

	ndata = int(c.sdObjReq.CopyPolicy)
	nparity = int(c.sdObjReq.StorePolicy)
	if ndata == 0 {
		ndata = int(p.cfg.Copies)
	}

	/*
		f, err := c.c.(*net.TCPConn).File()
		if err != nil {
			c.writeObjRsp(nil)
			return err
		}


			n, err = sio.Readv(f, [][]byte{buf})
			if n < int(c.sdObjReq.DataLen) {
				c.writeObjRsp(nil)
				return fmt.Errorf("io error %d < %d", n, c.sdObjReq.DataLen)
			}
	*/
	for l < c.sdObjReq.DataLen {
		n, err = c.c.Read(buf[l:])
		if err != nil {
			c.writeVdiRsp(nil)
			return err
		}
		l += uint32(n)
	}

	/*
		//fmt.Printf("%#+v\n", c.sdObjRsp)
		//r := io.LimitReader(c.c, int64(c.sdObjReq.DataLen))
		//w := &kv.RW{KV: p.engine, Name: fmt.Sprintf("%016x", c.sdObjReq.OID), Size: int64(c.sdObjReq.DataLen), Offset: int64(c.sdObjReq.Offset)}
		/*
			oflags := os.O_CREATE | os.O_WRONLY // | os.O_EXCL
			if is_data_obj(c.sdObjReq.OID) &&
				c.sdObjReq.Flags&SD_FLAG_CMD_DIRECT != 0 {
				if req_aligned(c.sdObjReq.Offset, c.sdObjReq.DataLen) {
					oflags |= unix.O_DIRECT
				}
			}
	*/

	if err = p.engine.Allocate(fmt.Sprintf("%016x", c.sdObjReq.OID), 1<<p.cfg.BlockSizeShift, ndata, nparity); err != nil {
		c.writeObjRsp(nil)
		return err
	}

	if n, err = p.engine.WriteAt(fmt.Sprintf("%016x", c.sdObjReq.OID), buf, int64(c.sdObjReq.Offset), ndata, nparity); err != nil {
		c.writeObjRsp(nil)
		return err
	}

	//fmt.Printf("%#+v\n", c.sdObjReq)
	c.sdObjRsp.Result = SD_RES_SUCCESS
	c.sdObjRsp.Copies = c.sdObjReq.Copies
	c.sdObjRsp.CopyPolicy = c.sdObjReq.CopyPolicy
	c.sdObjRsp.StorePolicy = c.sdObjReq.StorePolicy

	return c.writeObjRsp(nil)
}

func (p *ProxySheepdog) sdWriteObj(c *Conn) error {
	var err error
	var l uint32
	var n int
	var ndata int
	var nparity int

	//fmt.Printf("sdWriteObj\n")
	c.sdObjRsp.SheepdogHdr = c.sdObjReq.SheepdogHdr
	c.sdObjRsp.Result = SD_RES_EIO
	if p.cfg.Version == 0 {
		c.sdObjRsp.Result = SD_RES_WAIT_FOR_FORMAT
		c.writeObjRsp(nil)
		return err
	}
	ndata = int(c.sdObjReq.CopyPolicy)
	nparity = int(c.sdObjReq.StorePolicy)
	if ndata == 0 {
		ndata = int(p.cfg.Copies)
	}

	c.sdObjRsp.Epoch = p.cfg.Epoch

	/*
		oflags := os.O_WRONLY
		if is_data_obj(c.sdObjReq.OID) &&
			c.sdObjReq.Flags&SD_FLAG_CMD_DIRECT != 0 &&
			req_aligned(c.sdObjReq.Offset, c.sdObjReq.DataLen) {
			oflags |= unix.O_DIRECT
		}
	*/

	buf := c.bpool.Get(int(c.sdObjReq.DataLen))
	defer c.bpool.Put(buf)

	for l < c.sdObjReq.DataLen {
		n, err = c.c.Read(buf[l:])
		if err != nil {
			c.writeObjRsp(nil)
			return err
		}
		l += uint32(n)
	}

	_, err = p.engine.WriteAt(fmt.Sprintf("%016x", c.sdObjReq.OID), buf, int64(c.sdObjReq.Offset), ndata, nparity)
	if err != nil {
		c.writeObjRsp(nil)
		return err
	}

	/*
		if is_discard_req(c.sdObjReq, buf) {
			var oid uint32
			if err = binary.Read(obj, binary.LittleEndian, &oid); err != nil {
				c.writeObjRsp()
				return err
			}
			//if err = discard_oid(c.cfg, oid); err != nil {
			//	c.writeObjRsp()
			//	return err
			//}
			if _, err = obj.Seek(int64(c.sdObjReq.Offset), io.SeekStart); err != nil {
				c.writeObjRsp()
				return err
			}
		}
	*/

	c.sdObjRsp.Result = SD_RES_SUCCESS
	c.sdObjRsp.Copies = c.sdObjReq.Copies
	c.sdObjRsp.CopyPolicy = c.sdObjReq.CopyPolicy
	c.sdObjRsp.StorePolicy = c.sdObjReq.StorePolicy

	return c.writeObjRsp(nil)
}

//func sdGetNodeList(c *Conn, req *SheepdogIntReq) error {
//	rsp := &SheepdogIntRsp{SheepdogHdr: req.SheepdogHdr}
//	rsp.Epoch = c.cfg.Epoch
//
//	return binary.Write(c.c, binary.LittleEndian, rsp)
//}

func (p *ProxySheepdog) sdFlushVdi(c *Conn) error {
	var err error
	//fmt.Printf("sdFlushVdi\n")

	c.sdVdiRsp.SheepdogHdr = c.sdVdiReq.SheepdogHdr
	c.sdVdiRsp.Result = SD_RES_EIO
	c.sdVdiRsp.Epoch = p.cfg.Epoch

	buf := c.bpool.Get(int(c.sdVdiReq.DataLen))
	defer c.bpool.Put(buf)

	_, err = c.c.Read(buf)
	if err != nil {
		c.writeVdiRsp(nil)
		return err
	}

	vdiID := name2vdi(bytes.Trim(buf, "\x00"))
	// if object cache is on return SUCCESS or SD_RES_INVALID_PARMS if not, now returns always SUCCESS{
	c.sdVdiRsp.Result = SD_RES_SUCCESS
	//}
	c.sdVdiRsp.VdiID = vdiID

	return c.writeVdiRsp(nil)
}

func (p *ProxySheepdog) sdReleaseVdi(c *Conn) error {
	var err error
	//fmt.Printf("sdReleaseVdi\n")

	c.sdVdiRsp.SheepdogHdr = c.sdVdiReq.SheepdogHdr
	c.sdVdiRsp.Result = SD_RES_EIO
	c.sdVdiRsp.Epoch = p.cfg.Epoch

	buf := c.bpool.Get(int(c.sdVdiReq.DataLen))
	defer c.bpool.Put(buf)

	_, err = c.c.Read(buf)
	if err != nil {
		c.writeVdiRsp(nil)
		return err
	}

	vdiID := name2vdi(bytes.Trim(buf, "\x00"))

	c.sdVdiRsp.Result = SD_RES_SUCCESS
	c.sdVdiRsp.VdiID = vdiID

	return c.writeVdiRsp(nil)
}

func (p *ProxySheepdog) sdReadObj(c *Conn) error {
	var err error
	var n int
	var ndata int
	var nparity int

	//fmt.Printf("sdReadObj\n")
	c.sdObjRsp.SheepdogHdr = c.sdObjReq.SheepdogHdr
	c.sdObjRsp.Result = SD_RES_EIO
	c.sdObjRsp.Epoch = p.cfg.Epoch
	ndata = int(c.sdObjReq.CopyPolicy)
	nparity = int(c.sdObjReq.StorePolicy)

	if ndata == 0 {
		ndata = int(p.cfg.Copies)
	}
	/*
		oflags := os.O_RDONLY
		if is_data_obj(c.sdObjReq.OID) &&
			c.sdObjReq.Flags&SD_FLAG_CMD_DIRECT != 0 &&
			req_aligned(c.sdObjReq.Offset, c.sdObjReq.DataLen) {
			oflags |= unix.O_DIRECT
		}
	*/
	buf := c.bpool.Get(int(c.sdObjReq.DataLen))
	defer c.bpool.Put(buf)

	n, err = p.engine.ReadAt(fmt.Sprintf("%016x", c.sdObjReq.OID), buf, int64(c.sdObjReq.Offset), ndata, nparity)
	if err != nil {
		c.writeObjRsp(nil)
		return err
	}

	c.sdObjRsp.Result = SD_RES_SUCCESS
	c.sdObjRsp.Copies = c.sdObjReq.Copies
	c.sdObjRsp.CopyPolicy = c.sdObjReq.CopyPolicy
	c.sdObjRsp.StorePolicy = c.sdObjReq.StorePolicy
	c.sdObjRsp.DataLen = uint32(n)

	if err = c.writeObjRsp(buf); err != nil {
		return err
	}
	/*
		if _, err = c.c.Write(buf); err != nil {
			return err
		}
	*/
	return nil
}

func (p *ProxySheepdog) handleConn(c *Conn) {
	var err error
	var n int
	var hdr SheepdogHdr
	defer c.Close()

	buf := c.bpool.Get(SD_REQ_SIZE)
	defer c.bpool.Put(buf)

	for {
		n, err = c.c.Read(buf)
		if n < SD_REQ_SIZE || err != nil {
			return
		}

		hdr.Proto = buf[0]
		hdr.Opcode = buf[1]
		hdr.Flags = binary.LittleEndian.Uint16(buf[2:4])
		hdr.Epoch = binary.LittleEndian.Uint32(buf[4:8])
		hdr.ID = binary.LittleEndian.Uint32(buf[8:12])
		hdr.DataLen = binary.LittleEndian.Uint32(buf[12:16])
		switch sdOpcode(hdr.Opcode) {
		case SD_OP_CREATE_AND_WRITE_OBJ, SD_OP_READ_OBJ, SD_OP_WRITE_OBJ:
			c.sdObjReq.SheepdogHdr = hdr
			c.sdObjReq.OID = binary.LittleEndian.Uint64(buf[16:24])
			c.sdObjReq.CowOID = binary.LittleEndian.Uint64(buf[24:32])
			c.sdObjReq.Copies = buf[32]
			c.sdObjReq.CopyPolicy = buf[33]
			c.sdObjReq.StorePolicy = buf[34]
			c.sdObjReq.Offset = binary.LittleEndian.Uint64(buf[40:48])
		case SD_OP_RELEASE_VDI, SD_OP_FLUSH_VDI, SD_OP_LOCK_VDI, SD_OP_GET_VDI_INFO, SD_OP_NEW_VDI:
			c.sdVdiReq.SheepdogHdr = hdr
			c.sdVdiReq.Size = binary.LittleEndian.Uint64(buf[16:24])
			c.sdVdiReq.Base = binary.LittleEndian.Uint32(buf[24:28])
			c.sdVdiReq.Copies = buf[28]
			c.sdVdiReq.CopyPolicy = buf[29]
			c.sdVdiReq.StorePolicy = buf[30]
			c.sdVdiReq.BlockSizeShift = buf[31]
			c.sdVdiReq.SnapshotID = binary.LittleEndian.Uint32(buf[32:36])
			c.sdVdiReq.Type = binary.LittleEndian.Uint32(buf[36:40])
		case SD_OP_READ_VDIS:
			c.sdRawReq.SheepdogHdr = hdr
			c.sdRawReq.SheepdogRawReqData = buf[16:48]
		case SD_OP_GET_NODE_LIST:
			c.sdIntReq.SheepdogHdr = hdr
		case SD_OP_GET_CLUSTER_DEFAULT:
			c.sdClusterReq.SheepdogHdr = hdr
			c.sdClusterReq.OID = binary.LittleEndian.Uint64(buf[16:24])
			c.sdClusterReq.Ctime = binary.LittleEndian.Uint64(buf[24:32])
			c.sdClusterReq.Copies = buf[32]
			c.sdClusterReq.CopyPolicy = buf[33]
			c.sdClusterReq.Cflags = binary.LittleEndian.Uint16(buf[34:36])
			c.sdClusterReq.Tag = binary.LittleEndian.Uint32(buf[36:40])
			c.sdClusterReq.NrNodes = binary.LittleEndian.Uint32(buf[40:44])
			c.sdClusterReq.BlockSizeShift = buf[44]
		}

		switch sdOpcode(hdr.Opcode) {
		case SD_OP_READ_VDIS:
			err = p.sdReadVdis(c)
		//		case SD_OP_GET_NODE_LIST:
		//			err = sdGetNodeList(c)
		case SD_OP_GET_CLUSTER_DEFAULT:
			err = p.sdGetClusterDefault(c)
		case SD_OP_CREATE_AND_WRITE_OBJ:
			err = p.sdCreateWriteObj(c)
		case SD_OP_READ_OBJ:
			err = p.sdReadObj(c)
		case SD_OP_WRITE_OBJ:
			err = p.sdWriteObj(c)
		case SD_OP_RELEASE_VDI:
			err = p.sdReleaseVdi(c)
		case SD_OP_FLUSH_VDI:
			err = p.sdFlushVdi(c)
		case SD_OP_LOCK_VDI:
			err = p.sdGetVdiInfo(c)
		case SD_OP_GET_VDI_INFO:
			err = p.sdGetVdiInfo(c)
		case SD_OP_NEW_VDI:
			err = p.sdNewVdi(c)
		default:
			err = fmt.Errorf("unknown opcode: |%d| |%x|", buf[1], sdOpcode(buf[1]))
		}

		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}

	}
}
