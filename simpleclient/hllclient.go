package main

import (
	"bufio"
	"flag"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nipuntalukdar/hllserver/hllthrift"
	"os"
)

func copybuf(line []byte) []byte {
	ret := make([]byte, len(line))
	copy(ret, line)
	return ret
}

func main() {
	ipAddr := flag.String("server", "127.0.0.1:9999", "server_addr:port")
	filepath := flag.String("file", "a.txt", "file containing a word in every line")
	logkey := flag.String("logkey", "key1", "log key name")
	flag.Parse()

	trans, err := thrift.NewTSocket(*ipAddr)
	if err != nil {
		panic(err)
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transport := thrift.NewTBufferedTransport(trans, 2048000)
	defer transport.Close()
	if err = transport.Open(); err != nil {
		panic(err)
	}
	client := hllthrift.NewHllServiceClientFactory(trans, protocolFactory)
	status, err := client.AddLog(&hllthrift.AddLogCmd{*logkey, 20})
	if err != nil {
		panic(err)
	}
	if status != hllthrift.Status_SUCCESS {
		panic("Failed")
	}
	fp, err := os.Open(*filepath)
	if err != nil {
		panic(err)
	}
	rd := bufio.NewReader(fp)
	updm := hllthrift.NewUpdateLogMValCmd()
	updm.Key = *logkey
	updm.Data = make([][]byte, 8192)
	i := 0
	j := 0
	for {
		line, _, err := rd.ReadLine()
		if err != nil || line == nil {
			break
		}
		updm.Data[i] = copybuf(line)
		if i&8191 == 8191 {
			j += i
			i = 0
			status, err = client.UpdateM(updm)
			if err != nil || status != hllthrift.Status_SUCCESS {
				panic("Failed")
			}
		} else {
			i++
		}
	}
	if i > 0 {
		updm.Data = updm.Data[:i]
		status, err = client.UpdateM(updm)
		j += i
		if err != nil || status != hllthrift.Status_SUCCESS {
			panic("Failed")
		}
	}
	fmt.Printf("Sent %d\n", j)
	cr, err := client.GetCardinality(*logkey)
	if err != nil || cr.Status != hllthrift.Status_SUCCESS {
		panic("Couldn't get cardinality")
	}
	fmt.Printf("Cardinality for %s: %d\n", cr.Key, cr.Cardinality)

}
