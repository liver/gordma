package main

import (
	"flag"
	"fmt"
	"math/rand"

	"github.com/liver/gordma"
)

func main() {
	c, err := gordma.NewRdmaContext("", 1, 0, gordma.IBV_MTU_4096)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", c)
	pd, err := gordma.NewProtectDomain(c)
	if err != nil {
		panic(err)
	}
	mr, err := gordma.NewMemoryRegion(pd, 4096, 3)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", mr)
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
		portSelection := make(chan int, 1)
		go func (p chan int)  {
			fmt.Printf("rdma server port: %d\n", <-p)
		}(portSelection)
		err := gordma.ConnectQpServer(c, qp, mr, port, portSelection)
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
	fmt.Printf("%s\n", mr)

	rwr := gordma.NewReceiveWorkRequest(mr)
	defer rwr.Close()
	if err := qp.PostReceiveWithWait(rwr); err != nil {
		return fmt.Errorf("PostReceive failed: %v\n", err)
	}
	fmt.Printf("from client R: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])

	swr := gordma.NewSendWorkRequest(mr)
	defer swr.Close()
	localData := mr.Buffer()
	(*localData)[0] = byte(rand.Intn(9))
	(*localData)[1] = byte(rand.Intn(9))
	(*localData)[2] = byte(rand.Intn(9))
	(*localData)[3] = byte(rand.Intn(9))
	(*localData)[4] = byte(rand.Intn(9))
	fmt.Printf("from server W: %d%d%d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2], (*mr.Buffer())[3], (*mr.Buffer())[4])
	if err := qp.PostWriteWithWait(swr, gordma.MemBuffer); err != nil {
		return fmt.Errorf("PostWrite failed: %v\n", err)
	}

	localNotice := mr.Notice()
	(*localNotice)[0] = byte(randomInRange(2, 9))
	(*localNotice)[1] = byte(rand.Intn(9))
	(*localNotice)[2] = byte(rand.Intn(9))
	fmt.Printf("from server S: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])
	if err := qp.PostSendWithWait(swr); err != nil {
		return fmt.Errorf("PostSend failed: %v\n", err)
	}

	(*localNotice)[0] = 1
	fmt.Printf("from server I: %d\n", (*mr.Notice())[0])
	if err := qp.PostWriteImm(swr, gordma.MemNotice, 1); err != nil {
		return fmt.Errorf("PostWriteImm failed: %v\n", err)
	}

	return nil
}

func runClient(qp *gordma.QueuePair, mr *gordma.MemoryRegion) error {
	fmt.Printf("%s\n", mr)

	swr := gordma.NewSendWorkRequest(mr)
	defer swr.Close()
	localNotice := mr.Notice()
	(*localNotice)[0] = byte(randomInRange(2, 9))
	(*localNotice)[1] = byte(rand.Intn(9))
	(*localNotice)[2] = byte(rand.Intn(9))
	fmt.Printf("from client S: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])
	if err := qp.PostSendWithWait(swr); err != nil {
		return fmt.Errorf("PostSend failed: %v\n", err)
	}

	rwr := gordma.NewReceiveWorkRequest(mr)
	defer rwr.Close()
	if err := qp.PostReceiveWithWait(rwr); err != nil {
		return fmt.Errorf("PostReceive failed: %v\n", err)
	}
	fmt.Printf("from server R: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])

	fmt.Printf("from server W: %d%d%d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2], (*mr.Buffer())[3], (*mr.Buffer())[4])

	for {
		if (*mr.Notice())[0] == 1 {
			break
		}
		fmt.Printf("loop\n")
	}

	fmt.Printf("from server I: %d\n", (*mr.Notice())[0])

	return nil
}

func randomInRange(min, max int) int {
	return rand.Intn(max-min+1) + min
}
