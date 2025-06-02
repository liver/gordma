package gordma

/*
#cgo LDFLAGS: -lnuma
#include <stdlib.h>
#include <infiniband/verbs.h>
#include <numa.h>    // For numa_alloc_onnode, numa_free, numa_available, numa_max_node
#include <sys/mman.h> // For mmap, munmap, PROT_*, MAP_*
#include <stdio.h>    // For perror (optional, for debugging)

// alloc_type_t: 0 for NUMA, 1 for mmap, -1 for error
typedef int alloc_type_t;

// allocate_memory_numa_aware tries to allocate memory on the specified NUMA node.
// Otherwise, falls back to mmap.
// Sets *ptr_out to the allocated memory.
// Returns alloc_type_t.
alloc_type_t allocate_memory_numa_aware(size_t size, int node, void** ptr_out) {
    if (numa_available() == 0 && node >= 0 && node <= numa_max_node()) {
        *ptr_out = numa_alloc_onnode(size, node);
        if (*ptr_out != NULL) {
            // numa_bind(*ptr_out, size, node); // Can be added for explicit binding, but numa_alloc_onnode is usually sufficient
            return 0; // NUMA allocation successful
        }
        // numa_alloc_onnode failed, proceed to mmap fallback
        // perror("numa_alloc_onnode failed, falling back to mmap");
    }

    // Fallback to mmap or if node is < 0
    *ptr_out = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (*ptr_out == MAP_FAILED) {
        // perror("mmap failed");
        *ptr_out = NULL;
        return -1; // Error
    }
    return 1; // Mmap allocation successful
}

void free_memory_numa_aware(void* ptr, size_t size, alloc_type_t alloc_type) {
    if (ptr == NULL) return;

    if (alloc_type == 0) { // Allocated with NUMA
        numa_free(ptr, size);
    } else if (alloc_type == 1) { // Allocated with mmap
        munmap(ptr, size);
    }
    // If alloc_type is -1 (error) or unknown, ptr should be NULL or we have a problem
}
*/
import "C"
import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

// NumaNodeAny specifies that memory should be allocated without being tied to a specific NUMA node.
// In the current implementation this would result in the use of mmap.
const NumaNodeAny = -1

type MemoryRegion struct {
	PD         *ProtectDomain
	mrBuf      *C.struct_ibv_mr
	mrNotice   *C.struct_ibv_mr

	bufPtr     unsafe.Pointer // Pointer to the main buffer
	bufSize    int            // Size of the main buffer
	noticePtr  unsafe.Pointer // Pointer to the notice buffer
	noticeSize int            // Size of the notice buffer

	bufAllocType    C.alloc_type_t // Allocation type of the main buffer
	noticeAllocType C.alloc_type_t // Allocation type of the notice buffer

	qp         qpInfo
	isClosed   bool
}

// NewMemoryRegion allocates memory using mmap (defaulting to no specific NUMA node).
func NewMemoryRegion(pd *ProtectDomain, bufSize int, noticeSize int) (*MemoryRegion, error) {
	// Use -1 for node, so allocate_memory_numa_aware will use mmap
	return newMemoryRegionInternal(pd, bufSize, noticeSize, NumaNodeAny, NumaNodeAny)
}

// NewMemoryRegionByNuma allocates memory on specified NUMA nodes.
// If a node ID is < 0, mmap will be used for that buffer.
func NewMemoryRegionByNuma(pd *ProtectDomain, bufSize int, noticeSize int, bufNode int, noticeNode int) (*MemoryRegion, error) {
	return newMemoryRegionInternal(pd, bufSize, noticeSize, bufNode, noticeNode)
}

func newMemoryRegionInternal(pd *ProtectDomain, bufSize int, noticeSize int, bufNode int, noticeNode int) (*MemoryRegion, error) {
	const access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE

	mr := &MemoryRegion{
		PD:         pd,
		bufSize:    bufSize,
		noticeSize: noticeSize,
	}

	// Allocate memory for the main buffer
	mr.bufAllocType = C.allocate_memory_numa_aware(C.size_t(bufSize), C.int(bufNode), &mr.bufPtr)
	if mr.bufAllocType < 0 || mr.bufPtr == nil {
		return nil, errors.New("failed to allocate memory for buffer")
	}

	// Allocate memory for the notice buffer
	mr.noticeAllocType = C.allocate_memory_numa_aware(C.size_t(noticeSize), C.int(noticeNode), &mr.noticePtr)
	if mr.noticeAllocType < 0 || mr.noticePtr == nil {
		C.free_memory_numa_aware(mr.bufPtr, C.size_t(mr.bufSize), mr.bufAllocType) // Free previously allocated bufPtr
		return nil, errors.New("failed to allocate memory for notice")
	}

	// Register the main buffer
	mr.mrBuf = C.ibv_reg_mr(pd.pd, mr.bufPtr, C.size_t(bufSize), access)
	if mr.mrBuf == nil {
		C.free_memory_numa_aware(mr.noticePtr, C.size_t(mr.noticeSize), mr.noticeAllocType)
		C.free_memory_numa_aware(mr.bufPtr, C.size_t(mr.bufSize), mr.bufAllocType)
		return nil, errors.New("ibv_reg_mr: failed to reg mr buf")
	}

	// Register the notice buffer
	mr.mrNotice = C.ibv_reg_mr(pd.pd, mr.noticePtr, C.size_t(noticeSize), access)
	if mr.mrNotice == nil {
		C.ibv_dereg_mr(mr.mrBuf)
		C.free_memory_numa_aware(mr.noticePtr, C.size_t(mr.noticeSize), mr.noticeAllocType)
		C.free_memory_numa_aware(mr.bufPtr, C.size_t(mr.bufSize), mr.bufAllocType)
		return nil, errors.New("ibv_reg_mr: failed to reg mr notice")
	}

	mr.qp = qpInfo{
		BufRkey:     uint32(mr.mrBuf.rkey),
		BufRaddr:    uint64(uintptr(mr.bufPtr)), // Use the direct pointer
		NoticeRkey:  uint32(mr.mrNotice.rkey),
		NoticeRaddr: uint64(uintptr(mr.noticePtr)), // Use the direct pointer
	}

	// Enable finalizer
	runtime.SetFinalizer(mr, (*MemoryRegion).finalize)
	return mr, nil
}

//nolint
func (m *MemoryRegion) Buffer() *[]byte {
	// Create a Go slice header that points to the C memory.
	// This is unsafe and requires careful management of memory lifetime,
	// ensuring the C memory outlives the slice. unsafe.Slice sets Cap == Len.
	if m.bufPtr == nil {
		// Should not happen for a valid MemoryRegion, but good for safety
		var emptySlice []byte
		return &emptySlice
	}
	slice := (*[1 << 30]byte)(m.bufPtr)[:m.bufSize:m.bufSize]
	return &slice
}

func (m *MemoryRegion) Notice() *[]byte {
	// Create a Go slice header that points to the C memory.
	// This is unsafe and requires careful management of memory lifetime,
	// ensuring the C memory outlives the slice. unsafe.Slice sets Cap == Len.
	if m.noticePtr == nil {
		// Should not happen for a valid MemoryRegion, but good for safety
		var emptySlice []byte
		return &emptySlice
	}
	slice := (*[1 << 30]byte)(m.noticePtr)[:m.noticeSize:m.noticeSize]
 	return &slice
}

func (m *MemoryRegion) BufferLength() int {
	return m.bufSize
}

func (m *MemoryRegion) NoticeLength() int {
	return m.noticeSize
}

func (m *MemoryRegion) BufferPtr() unsafe.Pointer {
	return m.bufPtr // Return the stored pointer
}

func (m *MemoryRegion) NoticePtr() unsafe.Pointer {
	return m.noticePtr // Return the stored pointer
}

func (m *MemoryRegion) BufRemoteKey() uint32 {
	return m.qp.BufRkey
}

func (m *MemoryRegion) BufRemoteAddr() uint64 {
	return m.qp.BufRaddr
}
func (m *MemoryRegion) BufLocalKey() uint32 {
	if m.mrBuf != nil {
		return uint32(m.mrBuf.lkey)
	}
	return 0
}

func (m *MemoryRegion) NoticeRemoteKey() uint32 {
	return m.qp.NoticeRkey
}

func (m *MemoryRegion) NoticeRemoteAddr() uint64 {
	return m.qp.NoticeRaddr
}
func (m *MemoryRegion) NoticeLocalKey() uint32 {
	if m.mrNotice != nil {
		return uint32(m.mrNotice.lkey)
	}
	return 0
}

func (m *MemoryRegion) String() string {
	return fmt.Sprintf(
		"MemoryRegion BufRemoteAddr:%d BufLocalKey:%d BufRemoteKey:%d BufLen:%d NoticeRemoteAddr:%d NoticeLocalKey:%d NoticeRemoteKey:%d NoticeLen:%d",
		m.BufRemoteAddr(),
		m.BufLocalKey(),
		m.BufRemoteKey(),
		m.bufSize,
		m.NoticeRemoteAddr(),
		m.NoticeLocalKey(),
		m.NoticeRemoteKey(),
		m.noticeSize)
}

func (m *MemoryRegion) finalize() {
	// It's generally better to explicitly Close resources.
	// A panic here might indicate a resource leak.
	// Consider logging instead of panicking if finalizers are essential for cleanup.
	// log.Printf("warning: finalized unclosed memory region: %p", m)
	// m.Close() // Attempt to close, but this can be problematic in finalizers.
	panic(fmt.Sprintf("finalized unclosed memory region: %p. bufPtr: %p, noticePtr: %p", m, m.bufPtr, m.noticePtr))
}

func (m *MemoryRegion) Close() error {
	if m.isClosed {
		return fmt.Errorf("MR is already closed")
	}

	var firstErr error

	// Deregister and free notice buffer
	if m.mrNotice != nil {
		if errno := C.ibv_dereg_mr(m.mrNotice); errno != 0 {
			firstErr = fmt.Errorf("ibv_dereg_mr(notice) failed with errno %d", errno)
		}
		m.mrNotice = nil // Prevent double deregistration
	}
	if m.noticePtr != nil {
		C.free_memory_numa_aware(m.noticePtr, C.size_t(m.noticeSize), m.noticeAllocType)
		m.noticePtr = nil // Prevent double free
	}

	// Deregister and free main buffer
	if m.mrBuf != nil {
		if errno := C.ibv_dereg_mr(m.mrBuf); errno != 0 && firstErr == nil {
			firstErr = fmt.Errorf("ibv_dereg_mr(buf) failed with errno %d", errno)
		}
		m.mrBuf = nil
	}
	if m.bufPtr != nil {
		C.free_memory_numa_aware(m.bufPtr, C.size_t(m.bufSize), m.bufAllocType)
		m.bufPtr = nil
	}

	// Disable finalizer
	runtime.SetFinalizer(m, nil)
	m.isClosed = true
	return firstErr
}
