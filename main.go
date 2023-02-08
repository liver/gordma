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

	fmt.Println(qp, err)
	fmt.Println(qp.Qpn())

	if is_server {
		err = ibverbs.ConnectQpServer(c, qp)
	} else {
		err = ibverbs.ConnectQpClient(c, qp)
	}
	fmt.Println(err)

	fmt.Println("\n---------------- close ---------------")
	fmt.Println(qp.Close())
	fmt.Println(cq.Close())
	fmt.Println(mr.Close())
	fmt.Println(pd.Close())
	fmt.Println(c.Close())
}
