//go:build linux
// +build linux

package ibverbs

//#include <infiniband/verbs.h>
import "C"
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

var SockSyncMsg string = "sync"

type qpInfo struct {
	Lid   uint16
	QpNum uint32
	Psn   uint32
	Rkey  uint32
	Raddr uint64
}

func ConnectQpClient(ctx *rdmaContext, qp *QueuePair, mr *MemoryRegion) error {
	c, err := net.Dial("tcp", "localhost:8008")
	if err != nil {
		log.Println("dial error:", err)
		return err
	}
	if c == nil {
		return err
	}
	defer c.Close()

	bufNew := &bytes.Buffer{}
	localQpInfo := qpInfo{Lid: uint16(ctx.portAttr.lid), QpNum: qp.Qpn(), Psn: qp.Psn(), Rkey: mr.RemoteKey(), Raddr: uint64(uintptr(mr.mr.addr))}
	err = binary.Write(bufNew, binary.BigEndian, localQpInfo)
	if err != nil {
		return err
	}
	c.Write(bufNew.Bytes())

	fmt.Println(localQpInfo)

	buf := make([]byte, 64)
	cnt, err := c.Read(buf)
	if err != nil || cnt == 0 {
		return err
	}
	remoteQpInfo := qpInfo{}
	bufNew = bytes.NewBuffer(buf)
	err = binary.Read(bufNew, binary.BigEndian, &remoteQpInfo)
	if err != nil {
		return err
	}

	fmt.Println(remoteQpInfo)

	mr.remoteAddr = remoteQpInfo.Raddr
	mr.remoteKey = remoteQpInfo.Rkey

	err = modify_qp_to_rts(qp, remoteQpInfo.Lid, remoteQpInfo.QpNum, remoteQpInfo.Psn)
	if err != nil {
		return err
	}

	/* sync with clients */
	c.Write([]byte(SockSyncMsg))
	c.Read(buf)

	return nil

}

func ConnectQpServer(ctx *rdmaContext, qp *QueuePair, mr *MemoryRegion) error {
	l, err := net.Listen("tcp", ":8008")
	if err != nil {
		log.Println("listen error:", err)
		return err
	}

	c, err := l.Accept()
	if err != nil {
		log.Println("accept error:", err)
		return err
	}
	if c == nil {
		return err
	}
	defer c.Close()

	buf := make([]byte, 64)
	cnt, err := c.Read(buf)
	if err != nil || cnt == 0 {
		return err
	}

	remoteQpInfo := qpInfo{}
	bufNew := bytes.NewBuffer(buf)
	err = binary.Read(bufNew, binary.BigEndian, &remoteQpInfo)
	if err != nil {
		return err
	}

	fmt.Println(remoteQpInfo)

	bufNew = &bytes.Buffer{}
	localQpInfo := qpInfo{Lid: uint16(ctx.portAttr.lid), QpNum: qp.Qpn(), Psn: qp.Psn(), Rkey: mr.RemoteKey(), Raddr: uint64(uintptr(mr.mr.addr))}
	err = binary.Write(bufNew, binary.BigEndian, localQpInfo)
	if err != nil {
		return err
	}
	c.Write(bufNew.Bytes())

	fmt.Println(localQpInfo)

	mr.remoteAddr = remoteQpInfo.Raddr
	mr.remoteKey = remoteQpInfo.Rkey

	err = modify_qp_to_rts(qp, remoteQpInfo.Lid, remoteQpInfo.QpNum, remoteQpInfo.Psn)
	if err != nil {
		return err
	}

	/* sync with clients */
	c.Read(buf)
	c.Write([]byte(SockSyncMsg))

	return nil
}

func modify_qp_to_rts(qp *QueuePair, destLid uint16, destQpNum uint32, destPsn uint32) error {
	err := qp.Init()
	if err != nil {
		return err
	}

	err = qp.Ready2Receive(destLid, destQpNum, destPsn)
	if err != nil {
		return err
	}

	err = qp.Ready2Send()
	if err != nil {
		return err
	}
	return nil
}
