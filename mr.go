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
	mrBuf      *C.struct_ibv_mr
	mrNotice   *C.struct_ibv_mr
	buf        uintptr // link to buffer
	bufSize    int // buffer length
	notice     uintptr // link to notice
	noticeSize int // notice length
	qp         qpInfo
}

func NewMemoryRegion(pd *ProtectDomain, bufSize int, noticeSize int) (*MemoryRegion, error) {
	const mrPort = unix.PROT_READ | unix.PROT_WRITE
	const mrFlags = unix.MAP_PRIVATE | unix.MAP_ANONYMOUS
	
	buf, err := unix.Mmap(-1, 0, bufSize, mrPort, mrFlags)
	if err != nil {
		return nil, errors.New("mmap: failed to Mmap the buf")
	}
	notice, err := unix.Mmap(-1, 0, noticeSize, mrPort, mrFlags)
	if err != nil {
		return nil, errors.New("mmap: failed to Mmap the buf")
	}

	const access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
	mrBuf := C.ibv_reg_mr(pd.pd, unsafe.Pointer(&buf[0]), C.size_t(bufSize), access)
	if mrBuf == nil {
		_ = unix.Munmap(buf)
		return nil, errors.New("ibv_reg_mr: failed to reg mr")
	}
	mrNotice := C.ibv_reg_mr(pd.pd, unsafe.Pointer(&notice[0]), C.size_t(noticeSize), access)
	if mrNotice == nil {
		_ = unix.Munmap(notice)
		return nil, errors.New("ibv_reg_mr: failed to reg mr")
	}
	mr := &MemoryRegion{
		PD:         pd,
		mrBuf:      mrBuf,
		mrNotice:   mrNotice,
		buf:        uintptr(unsafe.Pointer(&buf[0])),
		bufSize:    bufSize,
		notice:     uintptr(unsafe.Pointer(&notice[0])),
		noticeSize: noticeSize,
		qp:         qpInfo{
			Rkey:  uint32(mrBuf.rkey),
			Raddr: uint64(uintptr(unsafe.Pointer(&buf[0]))),
		},
	}

	runtime.SetFinalizer(mr, (*MemoryRegion).finalize)
	return mr, nil
}

func (m *MemoryRegion) Buffer() *[]byte {
	memory := (*[]byte)(unsafe.Pointer(&struct {
		addr uintptr
		len  int
		cap  int
	}{
		uintptr(m.mrBuf.addr),
		int(m.mrBuf.length),
		int(m.mrBuf.length),
	}))
	return memory
}

func (m *MemoryRegion) Notice() *[]byte {
	memory := (*[]byte)(unsafe.Pointer(&struct {
		addr uintptr
		len  int
		cap  int
	}{
		uintptr(m.mrNotice.addr),
		int(m.mrNotice.length),
		int(m.mrNotice.length),
	}))
	return memory
}

func (m *MemoryRegion) BufferLength() int {
	return int(m.mrBuf.length)
}

func (m *MemoryRegion) NoticeLength() int {
	return int(m.mrNotice.length)
}

func (m *MemoryRegion) BufferPtr() unsafe.Pointer {
	return m.mrBuf.addr
}

func (m *MemoryRegion) NoticePtr() unsafe.Pointer {
	return m.mrNotice.addr
}

func (m *MemoryRegion) RemoteKey() uint32 {
	return m.qp.Rkey
}

func (m *MemoryRegion) RemoteAddr() uint64 {
	return m.qp.Raddr
}
func (m *MemoryRegion) LocalKey() uint32 {
	return uint32(m.mrBuf.lkey)
}

func (m *MemoryRegion) String() string {
	return fmt.Sprintf(
		"MemoryRegion RemoteAddr:%d LocalKey:%d RemoteKey:%d len:%d",
		m.RemoteAddr(),
		m.mrBuf.lkey,
		m.RemoteKey(),
		len(*m.Buffer()))
}

func (m *MemoryRegion) finalize() {
	panic("finalized unclosed memory region")
}

func (m *MemoryRegion) Close() error {
	// buf
	errno := C.ibv_dereg_mr(m.mrBuf)
	if errno != 0 {
		return errors.New("failed to dealloc mr")
	}

	memory := *(*[]byte)(unsafe.Pointer(&struct {
		addr uintptr
		len  int
		cap  int
	}{m.buf, m.bufSize, m.bufSize}))

	err := unix.Munmap(memory)
	if err != nil {
		return err
	}
	//

	// notice
	errno = C.ibv_dereg_mr(m.mrNotice)
	if errno != 0 {
		return errors.New("failed to dealloc mr")
	}

	memory = *(*[]byte)(unsafe.Pointer(&struct {
		addr uintptr
		len  int
		cap  int
	}{m.notice, m.noticeSize, m.noticeSize}))

	err = unix.Munmap(memory)
	if err != nil {
		return err
	}
	///

	return nil
}