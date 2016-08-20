package hllogs

import (
	"bufio"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/nipuntalukdar/hllserver/hutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	MAX_LOG_INTERVAL  = 2 * time.Second
	MAX_ROLLOVER_SIZE = 2 * 1024 * 1024 * 1024
)

var inited sync.Once

type LogWriter struct {
	logs chan []byte
}

func NewLogWriter(inp chan []byte) *LogWriter {
	return &LogWriter{inp}
}

func (mywriter *LogWriter) Write(p []byte) (n int, err error) {
	mywriter.logs <- p
	return len(p), nil
}

type Logger struct {
	events  chan []byte
	outfile string
	file    *os.File
	buffer  *bufio.Writer
	backups int
	outdir  string
	regxp   string
	writer  *LogWriter
	log     *logrus.Logger
	rolsize int
	fsize   int
	t       *time.Ticker
	shutd   chan bool
}

var (
	Log    *logrus.Logger
	logger *Logger
)

func (logger *Logger) RollOver() {
	files, err := filepath.Glob(logger.regxp)
	if err != nil {
		fmt.Printf("Some error during rollover %v\n", err)
		os.Exit(1)
		return
	}

	var index_nums []int
	num_name := make(map[int]string)
	for _, file := range files {
		index_name := file[len(logger.outfile)+1:]
		if index_num, err := strconv.Atoi(index_name); err == nil {
			index_nums = append(index_nums, index_num)
			num_name[index_num] = file
		}
	}
	sort.Ints(index_nums)
	to_be_deleted := len(index_nums) - logger.backups + 1
	i := to_be_deleted
	if to_be_deleted > 0 {
		for ; to_be_deleted > 0; to_be_deleted-- {
			err = os.Remove(num_name[index_nums[len(index_nums)-to_be_deleted]])
			if err != nil && !os.IsNotExist(err) {
				fmt.Printf("Some error in removing file\n")
				os.Exit(1)
			}
		}
		index_nums = index_nums[:len(index_nums)-i]
	}
	i = len(index_nums)
	for i > 0 {
		err = os.Rename(num_name[index_nums[i-1]], fmt.Sprintf("%s.%d", logger.outfile, i+1))
		if err != nil {
			fmt.Printf("Error in renaming file %v\n", err)
		}
		i--
	}
	logger.file.Close()
	err = os.Rename(logger.outfile, fmt.Sprintf("%s.%d", logger.outfile, 1))
	if err != nil {
		fmt.Printf("Error in renaming file %v\n", err)
	}
	file, fsize := openAndLockFile(logger.outfile)
	logger.fsize = int(fsize)
	logger.file = file
	logger.buffer = bufio.NewWriter(file)
}

func openAndLockFile(outfile string) (*os.File, int64) {
	file, err := os.Open(outfile)
	var fsize int64
	if err != nil {
		if os.IsNotExist(err) {
			fsize = 0
		} else {
			fmt.Printf("Failed to open file:%v, err:%v\n", file, err)
			os.Exit(1)
		}
	} else {
		fsize = hutil.GetFileSizeFile(file)
		file.Close()
	}
	if fsize < 0 {
		file.Close()
		fmt.Printf("Problem in getting file size\n")
		os.Exit(1)
	}
	file.Close()
	file, err = os.OpenFile(outfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Failed to open file:%v, err:%v\n", file, err)
		os.Exit(1)
	}
	fd := file.Fd()
	if syscall.Flock(int(fd), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		file.Close()
		fmt.Printf("Unable to lock log file\n")
		os.Exit(1)
	}

	return file, fsize
}

func NewLogger(max_back_up int, max_log_size int, outfile string,
	loglev string) *Logger {
	outdir := filepath.Dir(outfile)
	file, fsize := openAndLockFile(outfile)
	buffer := bufio.NewWriter(file)
	regexp := outfile + ".*"
	events := make(chan []byte, 40960)
	writer := NewLogWriter(events)
	logrloger := logrus.New()

	loglevel, err := logrus.ParseLevel(loglev)
	if err != nil {
		panic(err)
	}

	logrloger.Level = loglevel
	logrloger.Out = writer
	logrloger.Formatter = &logrus.TextFormatter{DisableColors: true,
		TimestampFormat: "2006-01-02T15:04:05"}
	t := time.NewTicker(MAX_LOG_INTERVAL)
	Log = logrloger
	return &Logger{events, outfile, file, buffer, max_back_up,
		outdir, regexp, writer, logrloger, max_log_size, int(fsize), t,
		make(chan bool, 8)}
}

func (logger *Logger) addMsg(msg []byte) {
	n, err := logger.buffer.Write(msg)
	if err != nil {
		fmt.Printf("Log write error, exiting %v", err)
		os.Exit(1)
	}
	logger.fsize += n
	if logger.fsize > logger.rolsize {
		logger.buffer.Flush()
		logger.RollOver()
	}
}

func (logger *Logger) logSyncer() {
	for {
		select {
		case msg := <-logger.events:
			logger.addMsg(msg)

		case _ = <-logger.t.C:
			logger.buffer.Flush()

		case _ = <-logger.shutd:
			logger.buffer.Flush()
			logger.file.Close()
			break
		}
	}
}

func (logger *Logger) Shutdown() {
	logger.shutd <- true
}

func InitLogger(maxb int, maxls int, outfile string, loglev string) *Logger {
	inited.Do(func() {
		logger = NewLogger(maxb, maxls, outfile, loglev)
		go logger.logSyncer()
	})
	return logger
}

func GetLogger() *logrus.Logger {
	return Log
}
