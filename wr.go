package gordma

/*
#include <infiniband/verbs.h>
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

type SendWorkRequest struct {
	mr     *MemoryRegion
	sendWr *C.struct_ibv_send_wr
	sge    *C.struct_ibv_sge
}

type ReceiveWorkRequest struct {
	mr     *MemoryRegion
	recvWr *C.struct_ibv_recv_wr
	sge    *C.struct_ibv_sge
}

func NewSendWorkRequest(mr *MemoryRegion) *SendWorkRequest {
	// for safe reference passing from Go to C
	sendWr := (*C.struct_ibv_send_wr)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ibv_send_wr{}))))
	sge := (*C.struct_ibv_sge)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ibv_sge{}))))

	return &SendWorkRequest{
		mr:     mr,
		sendWr: sendWr,
		sge:    sge,
	}
}

func NewReceiveWorkRequest(mr *MemoryRegion) *ReceiveWorkRequest {
	// for safe reference passing from Go to C
	recvWr := (*C.struct_ibv_recv_wr)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ibv_recv_wr{}))))
	sge := (*C.struct_ibv_sge)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ibv_sge{}))))

	return &ReceiveWorkRequest{
		mr:     mr,
		recvWr: recvWr,
		sge:    sge,
	}
}

func (s *SendWorkRequest) createWrId() C.uint64_t {
	return C.uint64_t(uintptr(unsafe.Pointer(&(s.sendWr))))
}

func (wr *SendWorkRequest) String() string {
	return fmt.Sprintf(
		"WR: \n addr: %d\n key: %d\n",
		wr.mr.BufRemoteAddr(),
		wr.mr.BufRemoteKey())
}

func (r *ReceiveWorkRequest) createWrId() C.uint64_t {
	return C.uint64_t(uintptr(unsafe.Pointer(&(r.recvWr))))
}

func (wr *ReceiveWorkRequest) String() string {
	return fmt.Sprintf(
		"WR: \n addr: %d\n key: %d\n",
		wr.mr.BufRemoteAddr(),
		wr.mr.BufRemoteKey())
}

func (wr *ReceiveWorkRequest) Close() {
	C.free(unsafe.Pointer(wr.recvWr))
	C.free(unsafe.Pointer(wr.sge))
}

func (wr *SendWorkRequest) Close() {
	C.free(unsafe.Pointer(wr.sendWr))
	C.free(unsafe.Pointer(wr.sge))
}

func allocateAligned(size, alignment int) (unsafe.Pointer, error) {
    var ptr unsafe.Pointer
    errno := C.posix_memalign(&ptr, C.size_t(alignment), C.size_t(size))
    if errno != 0 {
        return nil, errors.New("posix_memalign failed with error: " + C.GoString(C.strerror(errno)))
    }
    return ptr, nil
}
