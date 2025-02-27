package main

import (
	"flag"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nipuntalukdar/hllserver/handlers/httphandler"
	"github.com/nipuntalukdar/hllserver/handlers/thrift"
	"github.com/nipuntalukdar/hllserver/hll"
	"github.com/nipuntalukdar/hllserver/hllogs"
	"github.com/nipuntalukdar/hllserver/hllstore"
	"github.com/nipuntalukdar/hllserver/hllthrift"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {

	http_addr := flag.String("http", ":55123", "give http lister_address")
	thrift_port := flag.String("thrift", "127.0.0.1:55124", "thrift rpc address")
	persistence := flag.Bool("persist", false, "should persist the hyperlogs in db?")
	persistdbdir := flag.String("db", "/tmp", "directory for hyperlog db")
	persitdbname := flag.String("dbfile", "hyperlogs.db", "hyperlogdb file")
	logfile := flag.String("logfile", "/tmp/hllogs.log", "log file path")
	logbackup := flag.Int("logbackup", 10, "maximum backup for logs")
	logsize := flag.Int("logfilesize", 2048000, "log rollover size")
	loglevel := flag.String("loglevel", "INFO", "logging level")
	flag.Parse()

	logmod := hllogs.InitLogger(*logbackup, *logsize, *logfile, *loglevel)
	logger := hllogs.GetLogger()
	logger.Info("Initialized logs")

	var store hllstore.HllStore
	if *persistence {
		store = hllstore.NewBoltStore(*persistdbdir, *persitdbname)
	}

	hlc := hll.NewHllContainer(1024, store)
	thandler, err := thandler.NewThriftHandler(hlc)
	if err != nil {
		logger.Fatal("Could not initialize the thrift handler")
	}

	hllprocessor := hllthrift.NewHllServiceProcessor(thandler)
	ssock, err := thrift.NewTServerSocket(*thrift_port)
	if err != nil {
		logger.Fatal("Couldn't create server socket for")
	}

	// Start the servers
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		server := thrift.NewTSimpleServer4(hllprocessor, ssock,
			thrift.NewTBufferedTransportFactory(2048000), thrift.NewTBinaryProtocolFactoryDefault())
		logger.Info("Starting the thrift server")
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
		updexpiryh := httphandler.NewHttpUpdateExpiryHandler(hlc)
		http.Handle("/addlogkey", haddlogh)
		http.Handle("/dellogkey", hdellogh)
		http.Handle("/updatelog", updllogh)
		http.Handle("/cardinality", cardinalh)
		http.Handle("/updexpiry", updexpiryh)

		logger.Info("Http listener starting")
		server.ListenAndServe()
	}()

	// signal handlers
	sigchan := make(chan os.Signal, 10)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	go func() {
		s := <-sigchan
		logger.Infof("Terminating hllserverd as signal:%v received", s)
		hlc.Shutdown()
		if store != nil {
			store.FlushAndStop()
		}
		logmod.Shutdown()
		time.Sleep(2 * time.Second)
		os.Exit(0)
	}()
	// Wait for http/thrift servers to stop
	wg.Wait()
	sigchan <- syscall.SIGTERM
	time.Sleep(2 * time.Second)
	os.Exit(0)
}
