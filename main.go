package main

import (
	"flag"
	"fmt"
	"gordma/ibverbs"
)

func main() {
	c, err := ibverbs.NewRdmaContext("", 1, 0)
	if err != nil {
		panic(err)
	}
	pd, err := ibverbs.NewProtectDomain(c)
	if err != nil {
		panic(err)
	}
	mr, err := ibverbs.NewMemoryRegion(pd, 4096)
	if err != nil {
		panic(err)
	}
	cq, err := ibverbs.NewCompletionQueue(c, 10)
	if err != nil {
		panic(err)
	}
	qp, err := ibverbs.NewQueuePair(c, pd, cq)
	if err != nil {
		panic(err)
	}

	isServer := flag.Bool("s", false, "")
	flag.Parse()

	server := "localhost"
	port := 8008

	if *isServer {
		err := ibverbs.ConnectQpServer(c, qp, mr, port)
		if err != nil {
			panic(err)
		}
	} else {
		err := ibverbs.ConnectQpClient(c, qp, mr, server, port)
		if err != nil {
			panic(err)
		}
	}

	if *isServer {
		err := runServer(qp, mr)
		if err != nil {
			fmt.Println(err)
		}
	} else {
		err := runClient(qp, mr)
		if err != nil {
			fmt.Println(err)
		}
	}

	qp.Close()
	cq.Close()
	mr.Close()
	pd.Close()
	c.Close()
}

func runServer(qp *ibverbs.QueuePair, mr *ibverbs.MemoryRegion) error {
	wr := ibverbs.NewSendWorkRequest(mr)
	localData := mr.Buffer()
	(*localData)[0] = 1
    (*localData)[1] = 2
	(*localData)[2] = 3
	if err := qp.PostWrite(wr, mr.RemoteAddr(), mr.RemoteKey()); err != nil {
		return fmt.Errorf("PostWrite failed: %v\n", err)
	}

	rw := ibverbs.NewReceiveWorkRequest(mr)
	if err := ibverbs.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
	}
	if err := qp.PostReceive(rw); err != nil {
		return fmt.Errorf("PostReceive failed: %v\n", err)
	}
	if err := ibverbs.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
    }
	fmt.Printf("from client: %d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2])

	return nil
}

func runClient(qp *ibverbs.QueuePair, mr *ibverbs.MemoryRegion) error {
	rwr := ibverbs.NewSendWorkRequest(mr)
	if err := qp.PostRead(rwr, mr.RemoteAddr(), mr.RemoteKey()); err != nil {
		return fmt.Errorf("PostRead failed: %v\n", err)
	}
	if err := ibverbs.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
    }
	fmt.Printf("from server: %d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2])

	swr := ibverbs.NewSendWorkRequest(mr)
	localData := mr.Buffer()
	(*localData)[0] = 4
    (*localData)[1] = 5
	(*localData)[2] = 6
	fmt.Printf("from client: %d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2])
	if err := qp.PostSend(swr); err != nil {
		return fmt.Errorf("PostSend failed: %v\n", err)
	}
	if err := ibverbs.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
    }
	
	return nil
}
