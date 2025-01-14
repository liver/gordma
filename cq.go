package gordma

//#include <infiniband/verbs.h>
import "C"
import (
	"context"
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

	// Check if the received event matches the expected completion queue.
	if evCQ != cq.cq {
		return nil, errors.New("unexpected CQ event")
	}

	// Acknowledge the CQ event using ibv_ack_cq_events.
    // This resets the unprocessed event counter in the CQ.
	C.ibv_ack_cq_events(cq.cq, 1)

	// Request notifications for new events from the CQ.
	if ret := C.ibv_req_notify_cq(cq.cq, 0); ret != 0 {
		// Error occurred while requesting CQ notifications.
		return nil, fmt.Errorf("failed to request CQ notifications: %d", ret)
	}

	result := make(map[uint64]bool)

	// Process completed operations in the CQ.
	var wc C.struct_ibv_wc
	for {
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("interrupted by timeout")
		default:
		}

		// Call ibv_poll_cq to poll the completion queue and get the work completion result.
		num := C.ibv_poll_cq(cq.cq, 1, &wc)
		if num < 0 {
			// Error occurred while polling CQ.
			return result, errors.New("failed to poll CQ")
		}
		if num == 0 {
			// If there are no new operations in the CQ, break out of the loop.
			break
		}

		result[uint64(wc.wr_id)] = wc.status == C.IBV_WC_SUCCESS
		// // Check the status of the completed operation.
		// switch wc.status {
		// case C.IBV_WC_SUCCESS:
		// 	result[uint64(wc.wr_id)] = true
		// default:
		// 	result[uint64(wc.wr_id)] = false
		// 	// If the operation's status is not successful, return an error with detailed information.
		// 	//return result, fmt.Errorf("work completion failed: wr_id=%d status=%s", wc.wr_id, C.GoString(C.ibv_wc_status_str(wc.status)))
		// }
	}

	// If all operations are successful, return nil.
	return result, nil
}

func(cq *CompletionQueue) WaitForCompletionId(ctx context.Context, wr_id uint64) error {
	cps, err := cq.WaitForCompletion(ctx)
	if err != nil {
		status, ok := Contains(cps, wr_id)
		if ok && status {
			return nil
		}

		return fmt.Errorf("WaitForCompletion failed ok:%t status:%t :: %v\n", ok, status, err)
	}

	status, ok := Contains(cps, wr_id)
	if ok && status {
		return nil
	}

	return fmt.Errorf("wr_id:%d not WaitForCompletion ok:%t status:%t", wr_id, ok, status)
}

// WaitForCompletionBusy waits for the completion of work in the Completion Queue (CQ).
// This method continuously polls the CQ for completed work requests and checks the status of each completed operation.
// If there are no completed operations, it continues polling; otherwise, it processes the completed operations.
func (cq *CompletionQueue) WaitForCompletionBusy(ctx context.Context) (map[uint64]bool, error) {
	// Create a slice to hold work completions
    wc := make([]C.struct_ibv_wc, cq.cqe)
	result := make(map[uint64]bool)

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
			// If no completed events, continue polling
            continue
        }

		// Slice of completed work completions
        completed := wc[:numEvents]
        for _, w := range completed {
			result[uint64(w.wr_id)] = w.status == C.IBV_WC_SUCCESS
			// // Check the status of each completed work item
			// switch w.status {
			// case C.IBV_WC_SUCCESS:
			// 	result[uint64(w.wr_id)] = true
			// default:
			// 	result[uint64(w.wr_id)] = false
			// 	//return result, fmt.Errorf("work completion failed: status=%d wr_id=%d", w.status, w.wr_id)
			// }
        }

		// If all completed work items have been successfully processed, return nil
        return result, nil
    }
}
