package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"time"

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
	wr_id, err := qp.PostReceive(rwr)
	if err != nil {
		return fmt.Errorf("PostReceive failed: %v\n", err)
	}
	fmt.Printf("PostReceive wr_id:%d\n", wr_id)

	fmt.Printf("from client R: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])

	time.Sleep(time.Microsecond)
	fmt.Printf("from client I: %d\n", (*mr.Notice())[0])

	swr := gordma.NewSendWorkRequest(mr)
	defer swr.Close()
	localData := mr.Buffer()
	(*localData)[0] = byte(rand.Intn(9))
	(*localData)[1] = byte(rand.Intn(9))
	(*localData)[2] = byte(rand.Intn(9))
	(*localData)[3] = byte(rand.Intn(9))
	(*localData)[4] = byte(rand.Intn(9))
	fmt.Printf("from server W: %d%d%d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2], (*mr.Buffer())[3], (*mr.Buffer())[4])
	wr_id, err = qp.PostWrite(swr, gordma.MemBuffer)
	if err != nil {
		return fmt.Errorf("PostWrite failed: %v\n", err)
	}
	fmt.Printf("PostWrite wr_id:%d\n", wr_id)

	localNotice := mr.Notice()
	(*localNotice)[0] = byte(randomInRange(2, 9))
	(*localNotice)[1] = byte(rand.Intn(9))
	(*localNotice)[2] = byte(rand.Intn(9))
	fmt.Printf("from server S: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])
	
	time.Sleep(time.Second)
	wr_id, err = qp.PostSend(swr)
	if err != nil {
		return fmt.Errorf("PostSend failed: %v\n", err)
	}
	fmt.Printf("PostSend wr_id:%d\n", wr_id)

	(*localNotice)[0] = 1
	fmt.Printf("from server I: %d\n", (*mr.Notice())[0])
	wr_id, err = qp.PostWriteImm(swr, gordma.MemNotice, 1)
	if err != nil {
		return fmt.Errorf("PostWriteImm failed: %v\n", err)
	}
	fmt.Printf("PostWriteImm wr_id:%d\n", wr_id)

	cs, _ := qp.CompletionQueue.WaitForCompletionBusy(context.Background())
	fmt.Printf("WaitForCompletionBusy cs:%v\n", cs)

	return nil
}

func runClient(qp *gordma.QueuePair, mr *gordma.MemoryRegion) error {
	fmt.Printf("%s\n", mr)

	err := qp.CompletionQueue.Notify()
	if err != nil {
		return fmt.Errorf("Notify failed: %v\n", err)
	}
	swr := gordma.NewSendWorkRequest(mr)
	defer swr.Close()
	localNotice := mr.Notice()
	(*localNotice)[0] = byte(randomInRange(3, 9))
	(*localNotice)[1] = byte(rand.Intn(9))
	(*localNotice)[2] = byte(rand.Intn(9))
	fmt.Printf("from client S: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])
	wr_id, err := qp.PostSend(swr)
	if err != nil {
		return fmt.Errorf("PostSend failed: %v\n", err)
	}
	fmt.Printf("PostSend wr_id:%d\n", wr_id)

	cs, _ := qp.CompletionQueue.WaitForCompletion(context.Background())
	fmt.Printf("PostSend WaitForCompletion cs:%v\n", cs)

	(*localNotice)[0] = 2
	fmt.Printf("from client I: %d\n", (*mr.Notice())[0])
	wr_id, err = qp.PostWriteImm(swr, gordma.MemNotice, 1)
	if err != nil {
		return fmt.Errorf("PostWriteImm failed: %v\n", err)
	}
	fmt.Printf("PostWriteImm wr_id:%d\n", wr_id)

	rwr := gordma.NewReceiveWorkRequest(mr)
	defer rwr.Close()
	
	wr_id, err = qp.PostReceive(rwr)
	if err != nil {
		return fmt.Errorf("PostReceive failed: %v\n", err)
	}
	fmt.Printf("PostReceive wr_id:%d\n", wr_id)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cs, err = qp.CompletionQueue.WaitForCompletionId(ctx, wr_id)
	if err != nil {
		return fmt.Errorf("PostReceive WaitForCompletionId %d failed %v:%v", wr_id, cs, err)
	}
	fmt.Printf("PostReceive WaitForCompletionId %d cs:%v\n", wr_id, cs)

	fmt.Printf("from server R: %d%d%d\n", (*mr.Notice())[0], (*mr.Notice())[1], (*mr.Notice())[2])
	
	for {
		if (*mr.Notice())[0] == 1 {
			break
		}
	}

	cs, _ = qp.CompletionQueue.WaitForCompletionBusy(context.Background())
	fmt.Printf("Notice WaitForCompletionBusy cs:%v\n", cs)

	fmt.Printf("from server I: %d\n", (*mr.Notice())[0])
	fmt.Printf("from server W: %d%d%d%d%d\n", (*mr.Buffer())[0], (*mr.Buffer())[1], (*mr.Buffer())[2], (*mr.Buffer())[3], (*mr.Buffer())[4])

	return nil
}

func randomInRange(min, max int) int {
	return rand.Intn(max-min+1) + min
}
