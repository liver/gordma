package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

type CompletionQueue struct {
	cqe     int
	cq      *C.struct_ibv_cq
	channel *C.struct_ibv_comp_channel
}

// NewCompletionQueue create new completion queue.
//
// Parameters:
//   - ctx: context
//   - cqe: amount of entries in queue
func NewCompletionQueue(ctx *RdmaContext, cqe int) (*CompletionQueue, error) {
	compChannel, err := C.ibv_create_comp_channel(ctx.ctx)
	if err != nil {
		return nil, err
	}
	if compChannel == nil {
		return nil, errors.New("failed to create compChannel")
	}
	
	cq, err := C.ibv_create_cq(ctx.ctx, C.int(cqe), nil, compChannel, 0)
	if cq == nil {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("unknown error")
	}
	if ret := C.ibv_req_notify_cq(cq, 0); ret != 0 {
		return nil, fmt.Errorf("failed to request CQ notifications: %d", ret)
	}
	
	return &CompletionQueue{
		cqe:      cqe,
		cq:       cq,
		channel:  compChannel,
	}, nil
}

func (c *CompletionQueue) Cq() *C.struct_ibv_cq {
	return c.cq
}

func (c *CompletionQueue) Cqe() int {
	return c.cqe
}

func (c *CompletionQueue) CompChannel() *C.struct_ibv_comp_channel {
	return c.channel
}

func (c *CompletionQueue) Close() error {
	channel := c.cq.channel
	errno := destroyCQ(c.cq)
	if errno != 0 {
		return errors.New("ibv_destroy_cq failed")
	}
	if channel != nil {
		errno := destroyCompChannel(channel)
		if errno != 0 {
			return errors.New("ibv_destroy_comp_channel failed")
		}
	}
	return nil
}

func destroyCQ(cq *C.struct_ibv_cq) C.int {
	return C.ibv_destroy_cq(cq)
}

func destroyCompChannel(channel *C.struct_ibv_comp_channel) C.int {
	return C.ibv_destroy_comp_channel(channel)
}

func(cq *CompletionQueue) WaitForCompletion() error {
	var evCQ *C.struct_ibv_cq
	var evCtx unsafe.Pointer

	ret := C.ibv_get_cq_event(cq.channel, &evCQ, &evCtx)
	if ret != 0 {
		return fmt.Errorf("failed to get CQ event: %d", ret)
	}

	if evCQ != cq.cq {
		return errors.New("unexpected CQ event")
	}

	C.ibv_ack_cq_events(cq.cq, 1)

	if ret := C.ibv_req_notify_cq(cq.cq, 0); ret != 0 {
		return fmt.Errorf("failed to request CQ notifications: %d", ret)
	}

	var wc C.struct_ibv_wc
	for {
		num := C.ibv_poll_cq(cq.cq, 1, &wc)
		if num < 0 {
			return errors.New("failed to poll CQ")
		}
		if num == 0 {
			break
		}

		if wc.status != C.IBV_WC_SUCCESS {
			return fmt.Errorf("work completion failed: wr_id=%d status=%s", wc.wr_id, C.GoString(C.ibv_wc_status_str(wc.status)))
		}
	}

	return nil
}
