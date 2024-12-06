package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

type CompletionQueue struct {
	cqe     int
	cq      *C.struct_ibv_cq
	channel *C.struct_ibv_comp_channel
}

func NewCompletionQueue(ctx *RdmaContext, cqe int) (*CompletionQueue, error) {
	compChannel, err := C.ibv_create_comp_channel(ctx.ctx)
	if err != nil {
		return nil, err
	}
	if compChannel == nil {
		return nil, errors.New("failed to create compChannel")
	}
	
	if err := unix.SetNonblock(int(compChannel.fd), true); err != nil {
		return nil, err
	}
	cq, err := C.ibv_create_cq(ctx.ctx, C.int(cqe), nil, compChannel, 0)
	if cq == nil {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("unknown error")
	}
	return &CompletionQueue{
		cqe:     cqe,
		cq:      cq,
		channel: compChannel,
	}, nil
}

func (c *CompletionQueue) Cqe() int {
	return c.cqe
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

func WaitForCompletion(cq *C.struct_ibv_cq) error {
    var wc C.struct_ibv_wc // structure to store completion information

    // Waiting for completion event
    for {
        // We receive one event from CQ
        numEvents := C.ibv_poll_cq(cq, 1, &wc)
        if numEvents < 0 {
            return errors.New("polling CQ failed")
        }

        if numEvents == 0 {
			// If there are no completed operations, we continue to wait
            continue
        }

        // Checking the completion status
        if wc.status != C.IBV_WC_SUCCESS {
            return fmt.Errorf("work completion failed with status:%d wr_id:%d", wc.status, wc.wr_id)
        }

        return nil
    }
}
