package gordma

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

type Type int

const (
    MemBuffer Type = 0
    MemNotice Type = 1
)

func NewErrorOrNil(name string, errno int32) error {
	if errno > 0 {
		return os.NewSyscallError(name, syscall.Errno(errno))
	}
	if errno < 0 {
		// generic error for functions that don't set errno
		return errors.New(name + ": failure")
	}
	return nil
}

// NetToHostShort converts a 16-bit integer from network to host byte order, aka "ntohs"
func NetToHostShort(i uint16) uint16 {
    data := make([]byte, 2)
    binary.BigEndian.PutUint16(data, i)
    return binary.LittleEndian.Uint16(data)
}

// NetToHostLong converts a 32-bit integer from network to host byte order, aka "ntohl"
func NetToHostLong(i uint32) uint32 {
    data := make([]byte, 4)
    binary.BigEndian.PutUint32(data, i)
    return binary.LittleEndian.Uint32(data)
}

// NetToHostLongLong converts a 64-bit integer from network to host byte order, aka "ntohll"
func NetToHostLongLong(i uint64) uint64 {
    data := make([]byte, 8)
    binary.BigEndian.PutUint64(data, i)
    return binary.LittleEndian.Uint64(data)
}

// HostToNetShort converts a 16-bit integer from host to network byte order, aka "htons"
func HostToNetShort(i uint16) uint16 {
    b := make([]byte, 2)
    binary.LittleEndian.PutUint16(b, i)
    return binary.BigEndian.Uint16(b)
}

// HostToNetLong converts a 32-bit integer from host to network byte order, aka "htonl"
func HostToNetLong(i uint32) uint32 {
    b := make([]byte, 4)
    binary.LittleEndian.PutUint32(b, i)
    return binary.BigEndian.Uint32(b)
}

// HostToNetLongLong converts a 64-bit integer from host to network byte order, aka "htonll"
func HostToNetLongLong(i uint64) uint64 {
    b := make([]byte, 8)
    binary.LittleEndian.PutUint64(b, i)
    return binary.BigEndian.Uint64(b)
}
