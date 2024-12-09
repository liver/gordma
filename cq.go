package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"errors"
	"fmt"
	"time"

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

func (cq *CompletionQueue) WaitForCompletion() ([]C.struct_ibv_wc, error) {
    // cq.cqe Maximum Work Completions per call
    wc := make([]C.struct_ibv_wc, cq.cqe)

	ticker := time.NewTicker(time.Nanosecond)
	defer ticker.Stop()

    for { //nolint:gosimple
		select {
		case <-ticker.C:
			// 1. CQ survey for Work Completion
			numEvents := C.ibv_poll_cq(cq.cq, C.int(len(wc)), &wc[0])
			if numEvents < 0 {
				return nil, errors.New("polling CQ failed")
			}
	
			// 2. If there are no events
			if numEvents == 0 {
				continue
			}
	
			// 3. Check the status of each completed WR
			completed := wc[:numEvents]
			for _, w := range completed {
				if w.status != C.IBV_WC_SUCCESS {
					return nil, fmt.Errorf("work completion failed: status=%d wr_id=%d", w.status, w.wr_id)
				}
			}
	
			return completed, nil // Return back successful Work Completions
		}
    }
}
