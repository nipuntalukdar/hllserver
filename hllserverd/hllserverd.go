package main

import (
	"flag"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nipuntalukdar/hllserver/handlers/httphandler"
	"github.com/nipuntalukdar/hllserver/handlers/thrift"
	"github.com/nipuntalukdar/hllserver/hll"
	"github.com/nipuntalukdar/hllserver/hllthrift"
	"net/http"
	"sync"
	"time"
)

func main() {

	hlc := hll.NewHllContainer(1024)
	thandler, err := thandler.NewThriftHandler(hlc)
	if err != nil {
		panic("Could not initialize the thrift handler")
	}
	http_addr := flag.String("http", ":55123", "give http lister_address")
	thrift_port := flag.String("thrift", "127.0.0.1:55124", "thrift rpc address")
	flag.Parse()

	hllprocessor := hllthrift.NewHllServiceProcessor(thandler)
	ssock, err := thrift.NewTServerSocket(*thrift_port)
	if err != nil {
		panic("Couldn't create server socket for")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		server := thrift.NewTSimpleServer4(hllprocessor, ssock,
			thrift.NewTBufferedTransportFactory(2048000), thrift.NewTBinaryProtocolFactoryDefault())
		fmt.Println("Starting the thrift server")
		server.Serve()
	}()

	go func() {
		defer wg.Done()
		server := &http.Server{
			Addr:         *http_addr,
			ReadTimeout:  180 * time.Second,
			WriteTimeout: 180 * time.Second,
		}
		haddlogh := httphandler.NewHttpAddLogHandler(hlc)
		hdellogh := httphandler.NewHttpDelLogHandler(hlc)
		updllogh := httphandler.NewHttpUpdateLogHandler(hlc)
		cardinalh := httphandler.NewHttpGetCardinalityHandler(hlc)
		http.Handle("/addlogkey", haddlogh)
		http.Handle("/dellogkey", hdellogh)
		http.Handle("/updatelog", updllogh)
		http.Handle("/cardinality", cardinalh)

		fmt.Println("Http listener starting")
		server.ListenAndServe()
	}()

	wg.Wait()
}
