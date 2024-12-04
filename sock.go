package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"bytes"
	"encoding/binary"
	"fmt"
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

func ConnectQpClient(ctx *rdmaContext, qp *QueuePair, mr *MemoryRegion, server string, port int) error {
	if server == "" {
		server = "localhost"
	}
	
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", server, port))
	if err != nil {
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
	_, err = c.Write(bufNew.Bytes())
	if err != nil {
		return err
	}

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

	mr.remoteAddr = remoteQpInfo.Raddr
	mr.remoteKey = remoteQpInfo.Rkey

	err = modify_qp_to_rts(ctx, qp, remoteQpInfo.Lid, remoteQpInfo.QpNum, remoteQpInfo.Psn)
	if err != nil {
		return err
	}

	/* sync with clients */
	_, err = c.Write([]byte(SockSyncMsg))
	if err != nil {
		return err
	}
	_, err = c.Read(buf)
	if err != nil {
		return err
	}

	return nil

}

func ConnectQpServer(ctx *rdmaContext, qp *QueuePair, mr *MemoryRegion, port int, portSelection chan int) error {
	if port <= 0 {
		p, err := getFreePort()
		if err != nil {
			return err
		}
		port = p
	}

	if portSelection != nil {
		portSelection <- port
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	c, err := l.Accept()
	if err != nil {
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

	bufNew = &bytes.Buffer{}
	localQpInfo := qpInfo{Lid: uint16(ctx.portAttr.lid), QpNum: qp.Qpn(), Psn: qp.Psn(), Rkey: mr.RemoteKey(), Raddr: uint64(uintptr(mr.mr.addr))}
	err = binary.Write(bufNew, binary.BigEndian, localQpInfo)
	if err != nil {
		return err
	}
	_, err = c.Write(bufNew.Bytes())
	if err != nil {
		return err
	}

	mr.remoteAddr = remoteQpInfo.Raddr
	mr.remoteKey = remoteQpInfo.Rkey

	err = modify_qp_to_rts(ctx, qp, remoteQpInfo.Lid, remoteQpInfo.QpNum, remoteQpInfo.Psn)
	if err != nil {
		return err
	}

	/* sync with clients */
	_, err = c.Read(buf)
	if err != nil {
		return err
	}
	_, err = c.Write([]byte(SockSyncMsg))
	if err != nil {
		return err
	}

	return nil
}

func modify_qp_to_rts(ctx *rdmaContext, qp *QueuePair, destLid uint16, destQpNum uint32, destPsn uint32) error {
	err := qp.Init()
	if err != nil {
		return err
	}

	err = qp.Ready2Receive(ctx, destLid, destQpNum, destPsn)
	if err != nil {
		return err
	}

	err = qp.Ready2Send()
	if err != nil {
		return err
	}
	return nil
}

func getFreePort() (int, error) {
	// Open a listening socket on port 0
	listener, _ := net.Listen("tcp", ":0")
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	// We receive a port allocated by the operating system
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("Listener address is not a TCP address")
	}

	return addr.Port, nil
}
