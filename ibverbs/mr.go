//go:build linux
// +build linux

package ibverbs

//#include <infiniband/verbs.h>
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
	isMmap     bool
	Buf        []byte
	PD         *protectDomain
	mr         *C.struct_ibv_mr
}

func NewMemoryRegion(pd *protectDomain, size int, isMmap bool) (*MemoryRegion, error) {
	var (
		buf []byte
		err error
	)
	if isMmap {
		// create buffer: why can not directly use make([]byte, size) ?
		const mrPort = unix.PROT_READ | unix.PROT_WRITE
		const mrFlags = unix.MAP_PRIVATE | unix.MAP_ANONYMOUS
		buf, err = unix.Mmap(-1, 0, size, mrPort, mrFlags)
		if err != nil {
			return nil, errors.New("mmap: failed to Mmap the buf")
		}
	} else {
		buf = make([]byte, size)
	}
	const access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
	mrC := C.ibv_reg_mr(pd.pd, unsafe.Pointer(&buf[0]), C.size_t(size), access)
	if mrC == nil {
		return nil, errors.New("ibv_reg_mr: failed to reg mr")
	}
	mr := &MemoryRegion{
		Buf:       buf,
		PD:        pd,
		mr:        mrC,
		remoteKey: uint32(mrC.rkey),
	}
	runtime.SetFinalizer(mr, (*MemoryRegion).finalize)
	return mr, nil
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
	if m.Buf == nil {
		return "memoryRegion@closed"
	}
	return fmt.Sprintf("memoryRegion@%x[%d]", &m.Buf[0], len(m.Buf))
}

func (m *MemoryRegion) finalize() {
	panic("finalized unclosed memory region")
}

func (m *MemoryRegion) Close() error {
	errno := C.ibv_dereg_mr(m.mr)
	if errno != 0 {
		return errors.New("failed to dealloc mr")
	}
	if m.isMmap {
		err := unix.Munmap(m.Buf)
		if err != nil {
			return err
		}
	} else {
		m.Buf = nil
	}

	return nil
}
