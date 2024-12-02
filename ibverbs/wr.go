//go:build linux
// +build linux

package ibverbs

/*
#include <infiniband/verbs.h>
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

type sendWorkRequest struct {
	mr     *MemoryRegion
	sendWr *C.struct_ibv_send_wr
}

type receiveWorkRequest struct {
	mr     *MemoryRegion
	recvWr *C.struct_ibv_recv_wr
}

func NewSendWorkRequest(mr *MemoryRegion) *sendWorkRequest {
	// for safe reference passing from Go to C
	sendWr := (*C.struct_ibv_send_wr)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ibv_send_wr{}))))

	return &sendWorkRequest{
		mr:     mr,
		sendWr: sendWr,
	}
}

func NewReceiveWorkRequest(mr *MemoryRegion) *receiveWorkRequest {
	// for safe reference passing from Go to C
	recvWr := (*C.struct_ibv_recv_wr)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ibv_recv_wr{}))))
	return &receiveWorkRequest{
		mr:     mr,
		recvWr: recvWr,
	}
}

func (s *sendWorkRequest) createWrId() C.uint64_t {
	return C.uint64_t(uintptr(unsafe.Pointer(&(s.sendWr))))
}

func (r *receiveWorkRequest) createWrId() C.uint64_t {
	return C.uint64_t(uintptr(unsafe.Pointer(&(r.recvWr))))
}

func allocateAligned(size, alignment int) (unsafe.Pointer, error) {
    var ptr unsafe.Pointer
    errno := C.posix_memalign(&ptr, C.size_t(alignment), C.size_t(size))
    if errno != 0 {
        return nil, errors.New("posix_memalign failed with error: " + C.GoString(C.strerror(errno)))
    }
    return ptr, nil
}
