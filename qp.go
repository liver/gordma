package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// queuePair QP
type QueuePair struct {
	psn             uint32
	port            int
	qp              *C.struct_ibv_qp
	cq              *C.struct_ibv_cq
	CompletionQueue *CompletionQueue
}

type qpInfo struct {
	Lid   uint16
	Gid   [16]byte
	QpNum uint32
	Psn   uint32
	Rkey  uint32
	Raddr uint64
	MTU   uint32
}

func NewQueuePair(ctx *RdmaContext, pd *ProtectDomain, cq *CompletionQueue) (*QueuePair, error) {
	initAttr := C.struct_ibv_qp_init_attr{}
	initAttr.send_cq = cq.cq
	initAttr.recv_cq = cq.cq
	cqe := cq.Cqe()
	initAttr.cap.max_send_wr = C.uint32_t(cqe)
	initAttr.cap.max_recv_wr = C.uint32_t(cqe)
	initAttr.cap.max_send_sge = 1
	initAttr.cap.max_recv_sge = 1
	//initAttr.cap.max_inline_data = 64
	initAttr.qp_type = IBV_QPT_RC
	// make everything signaled. avoids the problem with inline
	// sends filling up the send queue of the cq
	initAttr.sq_sig_all = 1

	qpC, err := C.ibv_create_qp(pd.pd, &initAttr)
	if qpC == nil {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("qp: unknown error")
	}

	// create psn
	psn := rand.New(rand.NewSource(time.Now().UnixNano())).Uint32() & 0xffffff
	return &QueuePair{
		psn:             psn,
		port:            ctx.Port,
		qp:              qpC,
		cq:              cq.cq,
		CompletionQueue: cq,
	}, nil
}

func (q QueuePair) CQ() *C.struct_ibv_cq {
	return q.cq
}

func (q *QueuePair) Psn() uint32 {
	return q.psn
}

func (q *QueuePair) Qpn() uint32 {
	return uint32(q.qp.qp_num)
}

func (q *QueuePair) State() uint32 {
	return uint32(q.qp.state)
}

func (q *QueuePair) Close() error {
	if q.qp == nil {
		return nil
	}

	errno := C.ibv_destroy_qp(q.qp)
	if errno != 0 {
		return errors.New("ibv_destroy_qp failed")
	}
	q.qp = nil
	return nil
}

func (q *QueuePair) modify(attr *C.struct_ibv_qp_attr, mask int) error {
	errno := C.ibv_modify_qp(q.qp, attr, C.int(mask))
	return NewErrorOrNil("ibv_modify_qp", int32(int32(errno)))
}

func (q *QueuePair) Init() error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_INIT
	attr.pkey_index = 0
	attr.port_num = C.uint8_t(q.port)
	// allow RDMA write
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
	mask := C.IBV_QP_STATE | C.IBV_QP_PKEY_INDEX | C.IBV_QP_PORT | C.IBV_QP_ACCESS_FLAGS
	return q.modify(&attr, mask)
}

// Ready2Receive RTR
func (q *QueuePair) Ready2Receive(mtu uint32, destGidLocal uint16, destGidGlobal [16]byte, destQpn, destPsn uint32) error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_RTR
	attr.path_mtu = mtu
	attr.dest_qp_num = C.uint32_t(destQpn)
	attr.rq_psn = C.uint32_t(destPsn)
	// this must be > 0 to avoid IBV_WC_REM_INV_REQ_ERR
	attr.max_dest_rd_atomic = 1
	// Minimum RNR NAK timer (range 0..31)
	attr.min_rnr_timer = 24
	attr.ah_attr.dlid = C.uint16_t(destGidLocal)
	//  attr.ah_attr.dlid = C.uint16_t(destLid)
	attr.ah_attr.sl = 0
	attr.ah_attr.src_path_bits = 0
	attr.ah_attr.port_num = C.uint8_t(q.port)
	mask := C.IBV_QP_STATE | C.IBV_QP_AV | C.IBV_QP_PATH_MTU | C.IBV_QP_DEST_QPN |
		    C.IBV_QP_RQ_PSN | C.IBV_QP_MAX_DEST_RD_ATOMIC | C.IBV_QP_MIN_RNR_TIMER
	
	// for Soft-RoCE (aka RXE)
	attr.ah_attr.is_global = 1
	attr.ah_attr.grh.dgid = destGidGlobal
	attr.ah_attr.grh.flow_label = 0;
	attr.ah_attr.grh.hop_limit = 1;
	attr.ah_attr.grh.sgid_index = C.uint8_t(0)
	attr.ah_attr.grh.traffic_class = 0;
	//

	return q.modify(&attr, mask)
	
}

// Ready2Send RTS
func (q *QueuePair) Ready2Send() error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_RTS
	// Local ack timeout for primary path.
	// Timeout is calculated as 4.096e-6*(2**attr.timeout) seconds.
	attr.timeout = 14
	// Retry count (7 means forever)
	attr.retry_cnt = 6
	// RNR retry (7 means forever)
	attr.rnr_retry = 6
	attr.sq_psn = C.uint32_t(q.psn)
	// this must be > 0 to avoid IBV_WC_REM_INV_REQ_ERR
	attr.max_rd_atomic = 1
	mask := C.IBV_QP_STATE | C.IBV_QP_TIMEOUT | C.IBV_QP_RETRY_CNT | C.IBV_QP_RNR_RETRY |
		C.IBV_QP_SQ_PSN | C.IBV_QP_MAX_QP_RD_ATOMIC
	return q.modify(&attr, mask)
}

func (q *QueuePair) PostSendWithWait(wr *SendWorkRequest) error {
	err := q.PostSend(wr)
	if err != nil {
		return err
	}

	err = q.CompletionQueue.WaitForCompletion()
	if err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
	}

	return nil
}

func (q *QueuePair) PostSend(wr *SendWorkRequest) error {
	return q.PostSendImm(wr, 0)
}

func (q *QueuePair) PostSendImm(wr *SendWorkRequest, imm uint32) error {
	if imm > 0 {
		// post_send_immediately
		wr.sendWr.opcode = IBV_WR_SEND_WITH_IMM
		// always send inline if there is immediate data
		wr.sendWr.send_flags = IBV_SEND_INLINE
		binary.LittleEndian.PutUint32(wr.sendWr.anon0[:4], imm)
	} else {
		// post_send
		wr.sendWr.opcode = IBV_WR_SEND
		wr.sendWr.send_flags = IBV_SEND_SIGNALED
	}

	if wr.mr != nil {
		wr.sge.addr = C.uint64_t(uintptr(wr.mr.mrNotice.addr))
		wr.sge.length = C.uint32_t(wr.mr.mrNotice.length)
		wr.sge.lkey = wr.mr.mrNotice.lkey
		wr.sendWr.sg_list = wr.sge
		wr.sendWr.num_sge = 1
		wr.sendWr.next = nil
	} else {
		// send inline if there is no memory region to send
		wr.sendWr.send_flags = IBV_SEND_INLINE
	}
	wr.sendWr.wr_id = wr.createWrId()
	var bad *C.struct_ibv_send_wr

	errno := C.ibv_post_send(q.qp, wr.sendWr, &bad)
	return NewErrorOrNil("ibv_post_send", int32(errno))
}

func (q *QueuePair) PostReceiveWithWait(wr *ReceiveWorkRequest) error {
	err := q.PostReceive(wr)
	if err != nil {
		return err
	}

	err = q.CompletionQueue.WaitForCompletion()
	if err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
	}

	return nil
}

func (q *QueuePair) PostReceive(wr *ReceiveWorkRequest) error {
	if q.qp == nil {
		return QPClosedErr
	}

	var bad *C.struct_ibv_recv_wr
	wr.sge.addr = C.uint64_t(uintptr(wr.mr.mrNotice.addr))
	wr.sge.length = C.uint32_t(wr.mr.mrNotice.length)
	wr.sge.lkey = wr.mr.mrNotice.lkey
	wr.recvWr.sg_list = wr.sge
	wr.recvWr.num_sge = 1
	wr.recvWr.next = nil
	wr.recvWr.wr_id = wr.createWrId()

	errno := C.ibv_post_recv(q.qp, wr.recvWr, &bad)
	return NewErrorOrNil("ibv_post_recv", int32(errno))
}

func (q *QueuePair) PostWriteWithWait(wr *SendWorkRequest, remoteAddr uint64, rkey uint32) error {
	err := q.PostWriteImm(wr, remoteAddr, rkey, 0)
	if err != nil {
		return err
	}

	err = q.CompletionQueue.WaitForCompletion()
	if err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
	}

	return nil
}

func (q *QueuePair) PostWrite(wr *SendWorkRequest, remoteAddr uint64, rkey uint32) error {
	return q.PostWriteImm(wr, remoteAddr, rkey, 0)
}

func (q *QueuePair) PostWriteImm(wr *SendWorkRequest, remoteAddr uint64, rkey uint32, imm uint32) error {
	if q.qp == nil {
		return QPClosedErr
	}

	// if imm > 0 {
	// }else {
	// }

	var bad *C.struct_ibv_send_wr
	wr.sendWr.wr_id = wr.createWrId()
	wr.sendWr.opcode = IBV_WR_RDMA_WRITE
	wr.sendWr.send_flags = IBV_SEND_SIGNALED
	wr.sge.addr = C.uint64_t(uintptr(wr.mr.mrBuf.addr))
	wr.sge.length = C.uint32_t(wr.mr.mrBuf.length)
	wr.sge.lkey = wr.mr.mrBuf.lkey
	wr.sendWr.sg_list = wr.sge
	wr.sendWr.num_sge = 1
	wr.sendWr.next = nil
	binary.LittleEndian.PutUint64(wr.sendWr.wr[:8], remoteAddr)
	binary.LittleEndian.PutUint32(wr.sendWr.wr[8:12], rkey)

	errno := C.ibv_post_send(q.qp, wr.sendWr, &bad)
	return NewErrorOrNil("[PostWrite]ibv_post_send", int32(errno))
}

func (q *QueuePair) PostReadWithWait(wr *SendWorkRequest, remoteAddr uint64, rkey uint32) error {
	err := q.PostRead(wr, remoteAddr, rkey)
	if err != nil {
		return err
	}

	err = q.CompletionQueue.WaitForCompletion()
	if err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
	}

	return nil
}

func (q *QueuePair) PostRead(wr *SendWorkRequest, remoteAddr uint64, rkey uint32) error {
	var bad *C.struct_ibv_send_wr
	wr.sendWr.opcode = IBV_WR_RDMA_READ
	wr.sendWr.send_flags = IBV_SEND_SIGNALED
	wr.sge.addr = C.uint64_t(uintptr(wr.mr.mrBuf.addr))
	wr.sge.length = C.uint32_t(wr.mr.mrBuf.length)
	wr.sge.lkey = wr.mr.mrBuf.lkey
	wr.sendWr.sg_list = wr.sge
	wr.sendWr.num_sge = 1
	wr.sendWr.next = nil
	binary.LittleEndian.PutUint64(wr.sendWr.wr[:8], remoteAddr)
	binary.LittleEndian.PutUint32(wr.sendWr.wr[8:12], rkey)

	wr.sendWr.wr_id = wr.createWrId()

	errno := C.ibv_post_send(q.qp, wr.sendWr, &bad)
	return NewErrorOrNil("[PostWrite]ibv_post_send", int32(errno))
}
