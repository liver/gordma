package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

var SockSyncMsg string = "sync"

func ConnectQpClient(ctx *RdmaContext, qp *QueuePair, mr *MemoryRegion, server string, port int) (net.Conn, error) {
	if server == "" {
		server = "localhost"
	}
	
	c, err := net.Dial("tcp", net.JoinHostPort(server, fmt.Sprintf("%d", port)))
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errors.New("Connection is nullable.")
	}

	bufNew := &bytes.Buffer{}

	localQpInfo := qpInfo{
		Gid:         ctx.gid,
		Lid:         HostToNetShort(uint16(ctx.portAttr.lid)),
		QpNum:       HostToNetLong(qp.Qpn()),
		Psn:         HostToNetLong(qp.Psn()),
		BufRkey:     HostToNetLong(mr.BufRemoteKey()),
		BufRaddr:    HostToNetLongLong(mr.BufRemoteAddr()),
		NoticeRkey:  HostToNetLong(mr.NoticeRemoteKey()),
		NoticeRaddr: HostToNetLongLong(mr.NoticeRemoteAddr()),
	}
	err = binary.Write(bufNew, binary.BigEndian, localQpInfo)
	if err != nil {
		c.Close()
		return nil, err
	}
	_, err = c.Write(bufNew.Bytes())
	if err != nil {
		c.Close()
		return nil, err
	}

	buf := make([]byte, 64)
	cnt, err := c.Read(buf)
	if err != nil || cnt == 0 {
		c.Close()
		return nil, err
	}
	bufQpInfo := qpInfo{}
	bufNew = bytes.NewBuffer(buf)
	err = binary.Read(bufNew, binary.BigEndian, &bufQpInfo)
	if err != nil {
		c.Close()
		return nil, err
	}

	mr.qp = qpInfo{
		Gid:         bufQpInfo.Gid,
		Lid:         NetToHostShort(bufQpInfo.Lid),
		QpNum:       NetToHostLong(bufQpInfo.QpNum),
		Psn:         NetToHostLong(bufQpInfo.Psn),
		BufRkey:     NetToHostLong(bufQpInfo.BufRkey),
		BufRaddr:    NetToHostLongLong(bufQpInfo.BufRaddr),
		NoticeRkey:  NetToHostLong(bufQpInfo.NoticeRkey),
		NoticeRaddr: NetToHostLongLong(bufQpInfo.NoticeRaddr),
		MTU:         NetToHostLong(bufQpInfo.MTU),
	}

	err = modify_qp_to_rts(qp, mr.qp.MTU, mr.qp.Lid, mr.qp.Gid, mr.qp.QpNum, mr.qp.Psn)
	if err != nil {
		c.Close()
		return nil, err
	}

	/* sync with clients */
	_, err = c.Write([]byte(SockSyncMsg))
	if err != nil {
		c.Close()
		return nil, err
	}
	_, err = c.Read(buf)
	if err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func ConnectQpServer(ctx *RdmaContext, qp *QueuePair, mr *MemoryRegion, port int, portSelection chan int) (net.Conn, error) {
	if port < 0 {
		port = 0
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	if portSelection != nil {
		portSelection <- l.Addr().(*net.TCPAddr).Port
	}

	c, err := l.Accept()
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errors.New("Connection is nullable.")
	}

	buf := make([]byte, 64)
	cnt, err := c.Read(buf)
	if err != nil || cnt == 0 {
		c.Close()
		return nil, err
	}

	bufQpInfo := qpInfo{}
	bufNew := bytes.NewBuffer(buf)
	err = binary.Read(bufNew, binary.BigEndian, &bufQpInfo)
	if err != nil {
		c.Close()
		return nil, err
	}

	bufNew = &bytes.Buffer{}
	localQpInfo := qpInfo{
		Gid:         ctx.gid,
		Lid:         HostToNetShort(uint16(ctx.portAttr.lid)),
		QpNum:       HostToNetLong(qp.Qpn()),
		Psn:         HostToNetLong(qp.Psn()),
		BufRkey:     HostToNetLong(mr.BufRemoteKey()),
		BufRaddr:    HostToNetLongLong(mr.BufRemoteAddr()),
		NoticeRkey:  HostToNetLong(mr.NoticeRemoteKey()),
		NoticeRaddr: HostToNetLongLong(mr.NoticeRemoteAddr()),
		MTU:         HostToNetLong(uint32(ctx.IBV_MTU)),
	}
	err = binary.Write(bufNew, binary.BigEndian, localQpInfo)
	if err != nil {
		c.Close()
		return nil, err
	}
	_, err = c.Write(bufNew.Bytes())
	if err != nil {
		c.Close()
		return nil, err
	}

	mr.qp = qpInfo{
		Gid:         bufQpInfo.Gid,
		Lid:         NetToHostShort(bufQpInfo.Lid),
		QpNum:       NetToHostLong(bufQpInfo.QpNum),
		Psn:         NetToHostLong(bufQpInfo.Psn),
		BufRkey:     NetToHostLong(bufQpInfo.BufRkey),
		BufRaddr:    NetToHostLongLong(bufQpInfo.BufRaddr),
		NoticeRkey:  NetToHostLong(bufQpInfo.NoticeRkey),
		NoticeRaddr: NetToHostLongLong(bufQpInfo.NoticeRaddr),
		MTU:         uint32(ctx.IBV_MTU),
	}

	err = modify_qp_to_rts(qp, mr.qp.MTU, mr.qp.Lid, mr.qp.Gid, mr.qp.QpNum, mr.qp.Psn)
	if err != nil {
		c.Close()
		return nil, err
	}

	/* sync with clients */
	_, err = c.Read(buf)
	if err != nil {
		c.Close()
		return nil, err
	}
	_, err = c.Write([]byte(SockSyncMsg))
	if err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
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
