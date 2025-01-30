package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"
)

type CompletionQueue struct {
	cqe       int
	cq        *C.struct_ibv_cq
	channel   *C.struct_ibv_comp_channel
	isClosed  bool
	isClosing bool
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
	c.isClosing = true
	if c.isClosed {
		return fmt.Errorf("CQ is already closed")
	}

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
	c.isClosed = true
	return nil
}

func destroyCQ(cq *C.struct_ibv_cq) C.int {
	return C.ibv_destroy_cq(cq)
}

func destroyCompChannel(channel *C.struct_ibv_comp_channel) C.int {
	return C.ibv_destroy_comp_channel(channel)
}

// Notify request notifications for new events from the CQ.
func (c *CompletionQueue) Notify() error {
	ret := C.ibv_req_notify_cq(c.cq, 0)
	if ret != 0 {
		// Error occurred while requesting CQ notifications.
		return fmt.Errorf("failed to request CQ notifications: %d", ret)
	}
	return nil
}

// WaitForCompletion waits for the completion of work in the Completion Queue (CQ).
// This method handles CQ events, requests completion notifications, and checks the status of completed operations.
// It also waits until a completion event is received and verifies its correctness.
func(cq *CompletionQueue) WaitForCompletion(ctx context.Context) (map[uint64]bool, error) {
	// Variables to receive CQ event
	var evCQ *C.struct_ibv_cq
	var evCtx unsafe.Pointer
	
	// Get a CQ event from the channel using ibv_get_cq_event.
    // This function blocks until an event is received.
	ret := C.ibv_get_cq_event(cq.channel, &evCQ, &evCtx)
	if ret != 0 {
		// Error occurred while getting CQ event.
		return nil, fmt.Errorf("failed to get CQ event: %d", ret)
	}

	result := make(map[uint64]bool)
	defer func(r map[uint64]bool, evCQ *C.struct_ibv_cq) {
		// Check if the received event matches the expected completion queue.
		if evCQ != cq.cq {
			fmt.Println(fmt.Errorf("unexpected CQ event"))
			return
		}

		// Acknowledge the CQ event using ibv_ack_cq_events.
		// This resets the unprocessed event counter in the CQ.
		C.ibv_ack_cq_events(cq.cq, C.uint(len(r)))
	}(result, evCQ)

	// Process completed operations in the CQ.
	wc := make([]C.struct_ibv_wc, cq.cqe)
	for {
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("interrupted by timeout")
		default:
		}

		// Poll the CQ for completed operations
        numEvents := C.ibv_poll_cq(cq.cq, C.int(len(wc)), &wc[0])
        if numEvents < 0 {
			// Error occurred during polling
            return result, errors.New("polling CQ failed")
        }

        if numEvents == 0 {
			// If no completed events, break polling
            break
        }

		// Slice of completed work completions
        completed := wc[:numEvents]
        for _, w := range completed {
			result[uint64(w.wr_id)] = w.status == C.IBV_WC_SUCCESS
        }
	}

	// If all completed work items have been successfully processed, return
	return result, nil
}

// WaitForCompletionBusy waits for the completion of work in the Completion Queue (CQ).
// This method continuously polls the CQ for completed work requests and checks the status of each completed operation.
// If there are no completed operations, it continues polling; otherwise, it processes the completed operations.
func (cq *CompletionQueue) WaitForCompletionBusy(ctx context.Context, sleep time.Duration) (map[uint64]bool, error) {
	// Create a slice to hold work completions
    wc := make([]C.struct_ibv_wc, cq.cqe)
	result := make(map[uint64]bool)

    for {
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("interrupted by timeout")
		default:
		}

		if cq.isClosed || cq.isClosing {
			_ = C.ibv_poll_cq(cq.cq, C.int(len(wc)), &wc[0])
			return nil, fmt.Errorf("CompletionQueue closing")
		}

		// Poll the CQ for completed operations
        numEvents := C.ibv_poll_cq(cq.cq, C.int(len(wc)), &wc[0])
        if numEvents < 0 {
			// Error occurred during polling
            return result, errors.New("polling CQ failed")
        }

        if numEvents == 0 {
			// If no completed events, break polling
            break
        }

		// Slice of completed work completions
        completed := wc[:numEvents]
        for _, w := range completed {
			result[uint64(w.wr_id)] = w.status == C.IBV_WC_SUCCESS
        }

		time.Sleep(sleep)
    }

	// If all completed work items have been successfully processed, return
	return result, nil
}

// WaitForCompletionId waits for the work request with the specified identifier to complete.
//
// Parameters:
//   - ctx: context
//   - id: work request identifier
//   - sleep: interval between iteration
func (cq *CompletionQueue) WaitForCompletionId(ctx context.Context, id uint64, sleep time.Duration) (map[uint64]bool, error) {
    wc := make([]C.struct_ibv_wc, cq.cqe)
	result := make(map[uint64]bool)

    for {
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("interrupted by timeout")
		default:
		}

		if cq.isClosed || cq.isClosing {
			_ = C.ibv_poll_cq(cq.cq, C.int(len(wc)), &wc[0])
			return nil, fmt.Errorf("CompletionQueue closing")
		}
		
        numEvents := C.ibv_poll_cq(cq.cq, C.int(len(wc)), &wc[0])
        if numEvents < 0 {
            return result, errors.New("polling CQ failed")
        }

        if numEvents == 0 {
			time.Sleep(sleep)
            continue
        }

        completed := wc[:numEvents]
        for _, w := range completed {
			result[uint64(w.wr_id)] = w.status == C.IBV_WC_SUCCESS
        }

		if _, exists := result[id]; exists {
			return result, nil
		}

		time.Sleep(sleep)
    }
}
