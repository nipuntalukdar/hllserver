package main

import (
	"bufio"
	"flag"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nipuntalukdar/hllserver/hllthrift"
	"os"
	"sync/atomic"
	"time"
)

func copybuf(line []byte) []byte {
	ret := make([]byte, len(line))
	copy(ret, line)
	return ret
}

func get_client(addr string) (*hllthrift.HllServiceClient, *thrift.TBufferedTransport) {
	trans, err := thrift.NewTSocket(addr)
	if err != nil {
		panic(err)
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transport := thrift.NewTBufferedTransport(trans, 2048000)
	if err = transport.Open(); err != nil {
		panic(err)
	}
	client := hllthrift.NewHllServiceClientFactory(trans, protocolFactory)
	return client, transport
}

func addLog(chn chan *hllthrift.UpdateLogMValCmd, cl *hllthrift.HllServiceClient, counter *int32) {
	for {
		upd := <-chn
		status, err := cl.UpdateM(upd)
		if status != hllthrift.Status_SUCCESS || err != nil {
			panic("Failed")
		}
		atomic.AddInt32(counter, -1)
	}
}

func main() {
	ipAddr := flag.String("server", "127.0.0.1:9999", "server_addr:port")
	filepath := flag.String("file", "a.txt", "file containing a word in every line")
	logkey := flag.String("logkey", "key1", "log key name")
	flag.Parse()
	fmt.Println("Starting client")
	fp, err := os.Open(*filepath)
	if err != nil {
		panic(err)
	}
	rd := bufio.NewReader(fp)
	i := 0
	clients := make([]*hllthrift.HllServiceClient, 20)
	trans := make([]*thrift.TBufferedTransport, 20)
	for i < 20 {
		client, tr := get_client(*ipAddr)
		clients[i] = client
		trans[i] = tr
		i++
	}

	status, err := clients[0].AddLog(&hllthrift.AddLogCmd{*logkey, 0})
	if err != nil || status != hllthrift.Status_SUCCESS {
		panic("Failed")
	}

	var counter int32
	chn := make(chan *hllthrift.UpdateLogMValCmd, 16384)
	j := 0
	i = 0
	for i < 20 {
		go addLog(chn, clients[i], &counter)
		i++
	}

	i = 0
	var updm *hllthrift.UpdateLogMValCmd
	start := time.Now()
	updm = hllthrift.NewUpdateLogMValCmd()
	updm.Key = *logkey
	updm.Data = make([][]byte, 4096)
	for {
		line, _, err := rd.ReadLine()
		if err != nil || line == nil {
			break
		}
		updm.Data[i] = copybuf(line)
		if i&4095 == 4095 {
			atomic.AddInt32(&counter, 1)
			j += i
			i = 0
			chn <- updm
			updm = hllthrift.NewUpdateLogMValCmd()
			updm.Key = *logkey
			updm.Data = make([][]byte, 4096)
		} else {
			i++
		}
	}

	if i > 0 && updm != nil {
		atomic.AddInt32(&counter, 1)
		updm.Data = updm.Data[:i]
		chn <- updm
		j += i
	}
	fmt.Printf("Sent %d\n", j)
	for {
		val := atomic.LoadInt32(&counter)
		if val == 0 {
			break
		}
		time.Sleep(1000 * time.Millisecond)
		fmt.Printf("Val %d\n", val)
	}
	cr, err := clients[0].GetCardinality(*logkey)
	if err != nil || cr.Status != hllthrift.Status_SUCCESS {
		panic("Couldn't get cardinality")
	}
	fmt.Printf("Cardinality for %s: %d\n", cr.Key, cr.Cardinality)
	fmt.Printf("Time taken %s\n", time.Since(start))
	for _, tr := range trans {
		tr.Close()
	}
}
