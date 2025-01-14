package gordma

//#include <infiniband/verbs.h>
//#cgo linux LDFLAGS: -libverbs
//#include <stdlib.h>
import "C"
import (
	"errors"
	"fmt"
	"net"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

type RdmaContext struct {
	Name      string
	Port      int
	PortIndex int
	Guid      net.HardwareAddr
	ctx       *C.struct_ibv_context
	portAttr  C.struct_ibv_port_attr
	gid       C.union_ibv_gid
	IBV_MTU   int
	isClosed  bool
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

func NewRdmaContext(name string, port, index int, ibv_mtu int) (*RdmaContext, error) {
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
	for i := 0; i < int(count); i++ {
		device := *devicePtr
		bufName := CCharArrayToString(device.name)

		// Check device name if specified
		if name != "" && bufName != name {
			devicePtr = nextDevice(devicePtr)
			continue
		}

		// Open device
		ctx = C.ibv_open_device(device)
		if ctx == nil {
			devicePtr = nextDevice(devicePtr)
			continue
		}

		portC := C.uint8_t(port)
		indexC := C.int(index)

		// Query GID
		errno, err := C.ibv_query_gid(ctx, portC, indexC, &gid)
		if errno != 0 || err != nil {
			C.ibv_close_device(ctx)
			devicePtr = nextDevice(devicePtr)
			continue
		}
		guid = net.HardwareAddr(gid[8:])

		// Query port attributes
		errno, err = C.___ibv_query_port(ctx, portC, &portAttr)
		if errno != 0 || err != nil {
			C.ibv_close_device(ctx)
			devicePtr = nextDevice(devicePtr)
			continue
		}

		// Check if the port state is PORT_ACTIVE
		if portAttr.state != C.IBV_PORT_ACTIVE {
			fmt.Printf("Skipping port %d on device %s: not active\n", port, bufName)
			C.ibv_close_device(ctx)
			devicePtr = nextDevice(devicePtr)
			continue
		}

		return &RdmaContext{
			Name:      bufName,
			ctx:       ctx,
			Port:      port,
			PortIndex: index,
			Guid:      guid,
			portAttr:  portAttr,
			gid:       gid,
			IBV_MTU:   ibv_mtu,
		}, nil
	}

	return nil, fmt.Errorf("no active InfiniBand ports found on device %s", name)
}

// Move to the next device
func nextDevice(devicePtr **C.struct_ibv_device) **C.struct_ibv_device {
	return (**C.struct_ibv_device)(unsafe.Pointer(uintptr(unsafe.Pointer(devicePtr)) + unsafe.Sizeof(devicePtr)))
}

func (c *RdmaContext) Close() error {
	if c.isClosed {
		return fmt.Errorf("CTX is already closed")
	}
	errno := C.ibv_close_device(c.ctx)
	if errno != 0 {
		return errors.New("failed to close device")
	}
	c.ctx = nil
	c.isClosed = true
	return nil
}

func (c *RdmaContext) String() string {
	var builder strings.Builder
	for i, b := range c.gid {
		if i > 0 {
			builder.WriteString(":")
		}
		builder.WriteString(fmt.Sprintf("%02x", b))
	}
	
	return fmt.Sprintf(
		"rdmaContext: \n name: %s\n port: %d index: %d\n  mtu: %d\n guid: %s\n  gid: %s\n",
		c.Name,
		c.Port,
		c.PortIndex,
		c.IBV_MTU,
		c.Guid,
		builder.String())
}
