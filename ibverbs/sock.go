//go:build linux
// +build linux

package ibverbs

//#include <infiniband/verbs.h>
import "C"

func Connect_qp_client(ctx *rdmaContext, qp *queuePair) error {

	return nil
}

func Connect_qp_server(ctx *rdmaContext, qp *queuePair) error {

	return nil
}
