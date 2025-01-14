package gordma

//#include <infiniband/verbs.h>
import "C"
import "errors"

// MTU flag
const (
	IBV_MTU_256  = C.IBV_MTU_256
	IBV_MTU_512  = C.IBV_MTU_512
	IBV_MTU_1024 = C.IBV_MTU_1024
	IBV_MTU_2048 = C.IBV_MTU_2048
	IBV_MTU_4096 = C.IBV_MTU_4096
)

// access flag
const (
	IBV_ACCESS_LOCAL_WRITE   = C.IBV_ACCESS_LOCAL_WRITE
	IBV_ACCESS_REMOTE_WRITE  = C.IBV_ACCESS_REMOTE_WRITE
	IBV_ACCESS_REMOTE_READ   = C.IBV_ACCESS_REMOTE_READ
	IBV_ACCESS_REMOTE_ATOMIC = C.IBV_ACCESS_REMOTE_ATOMIC
)

// qp type
const (
	IBV_QPT_RC = C.IBV_QPT_RC
)

// wr action
const (
	IBV_WR_SEND          = C.IBV_WR_SEND
	IBV_WR_SEND_WITH_IMM = C.IBV_WR_SEND_WITH_IMM
	IBV_WR_RDMA_WRITE    = C.IBV_WR_RDMA_WRITE
	IBV_WR_RDMA_READ     = C.IBV_WR_RDMA_READ

	IBV_SEND_SIGNALED = C.IBV_SEND_SIGNALED
	IBV_SEND_INLINE   = C.IBV_SEND_INLINE
)

// interface states
const (
	IBV_PORT_ACTIVE = C.IBV_PORT_ACTIVE
)

var QPClosedErr = errors.New("qp already closed")
