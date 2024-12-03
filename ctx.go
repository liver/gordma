package gordma

//#include <infiniband/verbs.h>
//#cgo linux LDFLAGS: -libverbs
//#include <stdlib.h>
import "C"
import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"unsafe"
)

type rdmaContext struct {
	Name     string
	Port     int
	Guid     net.HardwareAddr
	ctx      *C.struct_ibv_context
	portAttr C.struct_ibv_port_attr
	gid      C.union_ibv_gid
	IBV_MTU  int
}

type rlimir struct {
	cur uint64
	max uint64
}

func init() {
	// skip memlock check if there is no IB hardware
	var r rlimir
	_, _, err := unix.Syscall(unix.SYS_GETRLIMIT, unix.RLIMIT_MEMLOCK, uintptr(unsafe.Pointer(&r)), 0)
	if err != 0 {
		panic(err.Error())
	}
}

func CCharArrayToString(cArray [64]C.char) string {
    // Find the null terminator to determine the length of a string
    n := 0
    for n < len(cArray) && cArray[n] != 0 {
        n++
    }
    // Convert to Go string
    return C.GoStringN((*C.char)(unsafe.Pointer(&cArray[0])), C.int(n))
}

func NewRdmaContext(name string, port, index int, ibv_mtu int) (*rdmaContext, error) {
	var count C.int
	var ctx *C.struct_ibv_context
	var guid net.HardwareAddr
	var portAttr C.struct_ibv_port_attr
	var gid C.union_ibv_gid

	deviceList, err := C.ibv_get_device_list(&count)
	if err != nil {
		return nil, err
	}
	if deviceList == nil || count == 0 {
		return nil, errors.New("failed to get devices list")
	}

	defer C.ibv_free_device_list(deviceList)
	devicePtr := deviceList
	device := *devicePtr
	bufName := name
	for device != nil && ctx == nil {
		bufName = CCharArrayToString(device.name)
		// If a device name is specified, we check the name match
		if name != "" && bufName != name {
			continue
		}

		ctx = C.ibv_open_device(device)
		portC := C.uint8_t(port)
		indexC := C.int(index)
		
		errno, err := C.ibv_query_gid(ctx, portC, indexC, &gid)
		if errno != 0 || err != nil {
			return nil, err
		}
		guid = net.HardwareAddr(gid[8:])

		errno, err = C.___ibv_query_port(ctx, portC, &portAttr)
		if errno != 0 || err != nil {
			return nil, err
		}
		
		// next device
		prevDevicePtr := uintptr(unsafe.Pointer(devicePtr))
		sizeofPtr := unsafe.Sizeof(devicePtr)
		devicePtr = (**C.struct_ibv_device)(unsafe.Pointer(prevDevicePtr + sizeofPtr))
		device = *devicePtr
	}
	if ctx == nil {
		return nil, fmt.Errorf("failed to open device %s", name)
	}

	return &rdmaContext{
		Name:     bufName,
		ctx:      ctx,
		Port:     port,
		Guid:     guid,
		portAttr: portAttr,
		gid:      gid,
		IBV_MTU:  ibv_mtu,
	}, nil
}

func (c *rdmaContext) Close() error {
	errno := C.ibv_close_device(c.ctx)
	if errno != 0 {
		return errors.New("failed to close device")
	}
	c.ctx = nil
	return nil
}

func (c *rdmaContext) String() string {
	return fmt.Sprintf("rdmaContext: \n name: %s\n port: %d\n guid: %s\n ", c.Name, c.Port, c.Guid)
}
