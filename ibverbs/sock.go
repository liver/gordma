//go:build linux
// +build linux

package ibverbs

//#include <infiniband/verbs.h>
import "C"
import (
	"log"
	"net"
	"encoding/binary"
	"bytes"
)
type qpInfo struct{
	lid uint16
	qp_num uint32
}

func ConnectQpClient(ctx *rdmaContext, qp *queuePair) error {
	c, err := net.Dial("tcp", "localhost:8888")
	if err != nil {
		log.Println("dial error:", err)
		return err
	}
	if c == nil {
		return err
	}
	defer c.Close()
	
	return nil
	
}

func ConnectQpServer(ctx *rdmaContext, qp *queuePair) error {
	l, err := net.Listen("tcp", ":8888")
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

	buf := make([]byte, 4096)
	cnt, err := c.Read(buf)
	if err != nil || cnt == 0 {
		c.Close()
		return err
	}
	remoteQpInfo := qpInfo{}
	bufNew := bytes.NewBuffer(buf)
	err = binary.Read(bufNew, binary.BigEndian, &remoteQpInfo)
	if err != nil {
		c.Close()
		return err
	}

	bufNew = &bytes.Buffer{}
	localQpInfo := qpInfo{lid:uint16(ctx.portAttr.lid),qp_num:qp.Qpn()}
	err = binary.Write(bufNew, binary.BigEndian, localQpInfo)
	if err != nil {
		c.Close()
		return err
	}
	c.Write(bufNew.Bytes())

	err = modifyQpToRts(qp,remoteQpInfo.qp_num,remoteQpInfo.lid)

	return nil
}

func modifyQpToRts(qp *queuePair, targetQpNum uint32, targetLid uint16) error{
	
	return nil
}
