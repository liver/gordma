package main

import (
	"flag"
	"fmt"

	"github.com/liver/gordma"
)

func main() {
	c, err := gordma.NewRdmaContext("", 1, 0, gordma.IBV_MTU_4096)
	if err != nil {
		panic(err)
	}
	pd, err := gordma.NewProtectDomain(c)
	if err != nil {
		panic(err)
	}
	mr, err := gordma.NewMemoryRegion(pd, 4096)
	if err != nil {
		panic(err)
	}
	cq, err := gordma.NewCompletionQueue(c, 10)
	if err != nil {
		panic(err)
	}
	qp, err := gordma.NewQueuePair(c, pd, cq)
	if err != nil {
		panic(err)
	}

	isServer := flag.Bool("s", false, "")
	flag.Parse()

	server := "localhost"
	port := 8008

	if *isServer {
		err := gordma.ConnectQpServer(c, qp, mr, port)
		if err != nil {
			panic(err)
		}
	} else {
		err := gordma.ConnectQpClient(c, qp, mr, server, port)
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

func runServer(qp *gordma.QueuePair, mr *gordma.MemoryRegion) error {
	wr := gordma.NewSendWorkRequest(mr)
	localData := mr.Buffer()
	(*localData)[0] = 1
    (*localData)[1] = 2
	(*localData)[2] = 3
	if err := qp.PostWrite(wr, mr.RemoteAddr(), mr.RemoteKey()); err != nil {
		return fmt.Errorf("PostWrite failed: %v\n", err)
	}

	rw := gordma.NewReceiveWorkRequest(mr)
	if err := gordma.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
	}
	if err := qp.PostReceive(rw); err != nil {
		return fmt.Errorf("PostReceive failed: %v\n", err)
	}
	if err := gordma.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
    }
	fmt.Printf("from client: %d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2])

	return nil
}

func runClient(qp *gordma.QueuePair, mr *gordma.MemoryRegion) error {
	rwr := gordma.NewSendWorkRequest(mr)
	if err := qp.PostRead(rwr, mr.RemoteAddr(), mr.RemoteKey()); err != nil {
		return fmt.Errorf("PostRead failed: %v\n", err)
	}
	if err := gordma.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
    }
	fmt.Printf("from server: %d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2])

	swr := gordma.NewSendWorkRequest(mr)
	localData := mr.Buffer()
	(*localData)[0] = 4
    (*localData)[1] = 5
	(*localData)[2] = 6
	fmt.Printf("from client: %d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2])
	if err := qp.PostSend(swr); err != nil {
		return fmt.Errorf("PostSend failed: %v\n", err)
	}
	if err := gordma.WaitForCompletion(qp.CQ()); err != nil {
		return fmt.Errorf("WaitForCompletion failed: %v\n", err)
    }
	
	return nil
}
