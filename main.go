package main

import (
	"fmt"
	"gordma/ibverbs"
)

var is_server bool

func main() {
	c, err := ibverbs.NewRdmaContext("mlx_5", 1, 0)
	if err != nil {
		panic(err)
	}
	fmt.Println(c)
	pd, err := ibverbs.NewProtectDomain(c)
	fmt.Println("pd", pd, err)
	mr, err := ibverbs.NewMemoryRegion(pd, 1024, true)
	if err != nil {
		panic(err)
	}
	fmt.Println(mr, mr.RemoteKey())

	cq, err := ibverbs.NewCompletionQueue(c, 10)
	fmt.Println(cq, err)

	qp, err := ibverbs.NewQueuePair(c, pd, cq)

	if is_server {
		err = ibverbs.Connect_qp_server(c, qp)
	} else {
		err = ibverbs.Connect_qp_client(c, qp)
	}

	fmt.Println(qp, err)
	fmt.Println(qp.Qpn())

	fmt.Println("\n---------------- close ---------------")
	fmt.Println(qp.Close())
	fmt.Println(cq.Close())
	fmt.Println(mr.Close())
	fmt.Println(pd.Close())
	fmt.Println(c.Close())
}
