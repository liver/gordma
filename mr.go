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
	PD         *ProtectDomain
	mr         *C.struct_ibv_mr
	buf        uintptr // link to buffer
	bufSize    int // buffer length
	qp         qpInfo
}

func NewMemoryRegion(pd *ProtectDomain, size int) (*MemoryRegion, error) {
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
		PD:         pd,
		mr:         mrC,
		buf:        uintptr(unsafe.Pointer(&buf[0])),
		bufSize:    size,
		qp:         qpInfo{
			Rkey:  uint32(mrC.rkey),
			Raddr: uint64(uintptr(unsafe.Pointer(&buf[0]))),
		},
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
	return m.qp.Rkey
}

func (m *MemoryRegion) RemoteAddr() uint64 {
	return m.qp.Raddr
}
func (m *MemoryRegion) LocalKey() uint32 {
	return uint32(m.mr.lkey)
}

func (m *MemoryRegion) String() string {
	return fmt.Sprintf(
		"MemoryRegion RemoteAddr:%d LocalKey:%d RemoteKey:%d len:%d",
		m.RemoteAddr(),
		m.mr.lkey,
		m.RemoteKey(),
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
