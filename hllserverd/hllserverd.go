package main

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nipuntalukdar/hllserver/handlers/thrift"
	"github.com/nipuntalukdar/hllserver/hllthrift"
)

func main() {
	thandler, err := thandler.NewThriftHandler()
	if err != nil {
		panic("Could not initialize the thrift handler")
	}
	hllprocessor := hllthrift.NewHllServiceProcessor(thandler)
	ssock, err := thrift.NewTServerSocket("127.0.0.1:9999")
	if err != nil {
		panic("Couldn't create server socket for")
	}
	server := thrift.NewTSimpleServer4(hllprocessor, ssock,
		thrift.NewTBufferedTransportFactory(2048000), thrift.NewTBinaryProtocolFactoryDefault())

	fmt.Println("Starting the server....")
	server.Serve()
}
