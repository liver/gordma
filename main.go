package main

import (
	"flag"
	"fmt"
	"gordma/ibverbs"
	"time"
)

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
	fmt.Println(mr, mr.RemoteKey(), mr.LocalKey())

	cq, err := ibverbs.NewCompletionQueue(c, 10)
	fmt.Println(cq, err)

	qp, err := ibverbs.NewQueuePair(c, pd, cq)

	fmt.Println(qp, err)
	fmt.Println(qp.Qpn())

	isServer := flag.Bool("s", false, "")
	flag.Parse()

	if *isServer {
		err = ibverbs.ConnectQpServer(c, qp, mr)
	} else {
		err = ibverbs.ConnectQpClient(c, qp, mr)
	}
	fmt.Println(qp.State(), mr.RemoteKey(), mr.RemoteAddr(), err)

	if *isServer {
		err = runServer(qp, mr)
	} else {
		err = runClient(qp, mr)
	}

	fmt.Println("\n---------------- close ---------------")
	fmt.Println(qp.Close())
	fmt.Println(cq.Close())
	fmt.Println(mr.Close())
	fmt.Println(pd.Close())
	fmt.Println(c.Close())
}

func runServer(qp *ibverbs.QueuePair, mr *ibverbs.MemoryRegion) error {
	wr := ibverbs.NewSendWorkRequest(mr)
	fmt.Printf("%d\n", &mr.Buf[0])
	mr.Buf[0] = 8
	qp.PostWrite(wr, mr.RemoteAddr(), mr.RemoteKey())
	return nil
}

func runClient(qp *ibverbs.QueuePair, mr *ibverbs.MemoryRegion) error {
	time.Sleep(1 * time.Second)
	fmt.Printf("%d\n", &mr.Buf[0])
	fmt.Println(mr.Buf[0])
	return nil
}
