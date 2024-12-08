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

func ConnectQpClient(ctx *RdmaContext, qp *QueuePair, mr *MemoryRegion, server string, port int) error {
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

	localQpInfo := qpInfo{
		Lid:   HostToNetShort(uint16(ctx.portAttr.lid)),
		Gid:   ctx.gid,
		QpNum: HostToNetLong(qp.Qpn()),
		Psn:   HostToNetLong(qp.Psn()),
		Rkey:  HostToNetLong(mr.RemoteKey()),
		Raddr: HostToNetLongLong(mr.RemoteAddr()),
	}
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
	bufQpInfo := qpInfo{}
	bufNew = bytes.NewBuffer(buf)
	err = binary.Read(bufNew, binary.BigEndian, &bufQpInfo)
	if err != nil {
		return err
	}

	mr.qp = qpInfo{
		Lid:   NetToHostShort(bufQpInfo.Lid),
		Gid:   bufQpInfo.Gid,
		QpNum: NetToHostLong(bufQpInfo.QpNum),
		Psn:   NetToHostLong(bufQpInfo.Psn),
		Rkey:  NetToHostLong(bufQpInfo.Rkey),
		Raddr: NetToHostLongLong(bufQpInfo.Raddr),
		MTU:   NetToHostLong(bufQpInfo.MTU),
	}

	err = modify_qp_to_rts(qp, mr.qp.MTU, mr.qp.Lid, mr.qp.Gid, mr.qp.QpNum, mr.qp.Psn)
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

func ConnectQpServer(ctx *RdmaContext, qp *QueuePair, mr *MemoryRegion, port int, portSelection chan int) error {
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

	bufQpInfo := qpInfo{}
	bufNew := bytes.NewBuffer(buf)
	err = binary.Read(bufNew, binary.BigEndian, &bufQpInfo)
	if err != nil {
		return err
	}

	bufNew = &bytes.Buffer{}
	localQpInfo := qpInfo{
		Lid:   HostToNetShort(uint16(ctx.portAttr.lid)),
		Gid:   ctx.gid,
		QpNum: HostToNetLong(qp.Qpn()),
		Psn:   HostToNetLong(qp.Psn()),
		Rkey:  HostToNetLong(mr.RemoteKey()),
		Raddr: HostToNetLongLong(mr.RemoteAddr()),
		MTU:   HostToNetLong(uint32(ctx.IBV_MTU)),
	}
	err = binary.Write(bufNew, binary.BigEndian, localQpInfo)
	if err != nil {
		return err
	}
	_, err = c.Write(bufNew.Bytes())
	if err != nil {
		return err
	}

	mr.qp = qpInfo{
		Lid:   NetToHostShort(bufQpInfo.Lid),
		Gid:   bufQpInfo.Gid,
		QpNum: NetToHostLong(bufQpInfo.QpNum),
		Psn:   NetToHostLong(bufQpInfo.Psn),
		Rkey:  NetToHostLong(bufQpInfo.Rkey),
		Raddr: NetToHostLongLong(bufQpInfo.Raddr),
		MTU:   uint32(ctx.IBV_MTU),
	}

	err = modify_qp_to_rts(qp, mr.qp.MTU, mr.qp.Lid, mr.qp.Gid, mr.qp.QpNum, mr.qp.Psn)
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

func modify_qp_to_rts(qp *QueuePair, mtu uint32, destLid uint16, destGid [16]byte, destQpNum uint32, destPsn uint32) error {
	err := qp.Init()
	if err != nil {
		return err
	}

	err = qp.Ready2Receive(mtu, destLid, destGid, destQpNum, destPsn)
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
