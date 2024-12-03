package gordma

/*
#include <stdlib.h>
#include <infiniband/verbs.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

type MemoryRegion struct {
	remoteKey  uint32
	remoteAddr uint64
	PD         *protectDomain
	mr         *C.struct_ibv_mr
	buf        uintptr // link to buffer
	bufSize    int // buffer length
}

func NewMemoryRegion(pd *protectDomain, size int) (*MemoryRegion, error) {
	const mrPort = unix.PROT_READ | unix.PROT_WRITE
	const mrFlags = unix.MAP_PRIVATE | unix.MAP_ANONYMOUS
	buf, err := unix.Mmap(-1, 0, size, mrPort, mrFlags)
	if err != nil {
		return nil, errors.New("mmap: failed to Mmap the buf")
	}

	const access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
	mrC := C.ibv_reg_mr(pd.pd, unsafe.Pointer(&buf[0]), C.size_t(size), access)
	if mrC == nil {
		_ = unix.Munmap(buf)
		return nil, errors.New("ibv_reg_mr: failed to reg mr")
	}
	mr := &MemoryRegion{
		PD:        pd,
		mr:        mrC,
		remoteKey: uint32(mrC.rkey),
		buf:       uintptr(unsafe.Pointer(&buf[0])),
		bufSize:   size,
	}
	runtime.SetFinalizer(mr, (*MemoryRegion).finalize)
	return mr, nil
}

func (m *MemoryRegion) Buffer() *[]byte {
	buffer := (*[]byte)(unsafe.Pointer(&struct {
		addr uintptr
		len  int
		cap  int
	}{
		uintptr(m.mr.addr),
		int(m.mr.length),
		int(m.mr.length),
	}))
	return buffer
}

func (m *MemoryRegion) RemoteKey() uint32 {
	return m.remoteKey
}

func (m *MemoryRegion) RemoteAddr() uint64 {
	return m.remoteAddr
}
func (m *MemoryRegion) LocalKey() uint32 {
	return uint32(m.mr.lkey)
}

func (m *MemoryRegion) String() string {
	return fmt.Sprintf(
		"memoryRegion@%d(0x%x)[%d]",
		uintptr(unsafe.Pointer(&(*m.Buffer())[0])),
		uintptr(unsafe.Pointer(&(*m.Buffer())[0])),
		len(*m.Buffer()))
}

func (m *MemoryRegion) finalize() {
	panic("finalized unclosed memory region")
}

func (m *MemoryRegion) Close() error {
	errno := C.ibv_dereg_mr(m.mr)
	if errno != 0 {
		return errors.New("failed to dealloc mr")
	}

	// Convert uintptr back to []byte slice
	// Please note: the slice is created without copying data.
	memory := *(*[]byte)(unsafe.Pointer(&struct {
		addr uintptr
		len  int
		cap  int
	}{m.buf, m.bufSize, m.bufSize}))

	err := unix.Munmap(memory)
	if err != nil {
		return err
	}

	return nil
}
