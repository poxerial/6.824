package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// Debugging
const Debug = true

const (
	RPCInfo = iota
	LeaderInfo
	CandidateInfo
	FollowerInfo
	TickerInfo
	UserInfo
	ApplyInfo
	PersistInfo
)

//	var loggersList = []string{
//		"RPCInfo",
//		"LeaderInfo",
//		"CandidateInfo",
//		"FollowerInfo",
//		"TickerInfo",
//		"UserInfo",
//	}
var loggers map[int]*log.Logger

// initialize logger
func init() {
	if Debug {
		tmpIO, err := os.OpenFile("log.txt", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalf("Can't open file.")
		}

		loggers = make(map[int]*log.Logger)

		loggers[RPCInfo] = log.New(tmpIO, "[RPCInfo]", log.LstdFlags)
		loggers[LeaderInfo] = log.New(tmpIO, "[LeaderInfo]", log.LstdFlags)
		//loggers[CandidateInfo] = log.New(tmpIO, "[CandidateInfo]", log.LstdFlags)
		//loggers[TickerInfo] = log.New(tmpIO, "[TickerInfo]", log.LstdFlags)
		loggers[FollowerInfo] = log.New(tmpIO, "[FollowerInfo]", log.LstdFlags)
		loggers[UserInfo] = log.New(tmpIO, "[UserInfo]", log.LstdFlags)
		loggers[ApplyInfo] = log.New(tmpIO, "[ApplyInfo]", log.LstdFlags)
		//loggers[PersistInfo] = log.New(tmpIO, "[PersistInfo]", log.LstdFlags)

		runtime.SetFinalizer(loggers[0], func(logger *log.Logger) {
			err := tmpIO.Close()
			if err != nil {
				log.Fatalf("Can't close file.")
			}
		})
	}
}

func DLog(info int, content ...interface{}) {
	logger, inList := loggers[info]
	if Debug && inList {
		logger.Println(content...)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(info ...interface{}) {
	if Debug {
		log.Println(info...)
	}
}

type DLock struct {
	lock  sync.RWMutex
	goid  int
	Rgoid map[int]bool
	mu    sync.Mutex
	init  bool
}

func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func (l *DLock) Lock() {
	l.lock.Lock()
	l.goid = goid()
}

func (l *DLock) Unlock() {
	l.goid = -1
	l.lock.Unlock()
}

func (l *DLock) RLock() {
	l.lock.RLock()
	l.mu.Lock()
	if !l.init {
		l.Rgoid = make(map[int]bool)
		l.init = true
	}
	l.Rgoid[goid()] = true
	l.mu.Unlock()
}

func (l *DLock) RUnlock() {
	l.mu.Lock()
	l.Rgoid[goid()] = false
	l.mu.Unlock()
	l.lock.RUnlock()
}

func Assert(expr bool, v interface{}) {
	if !expr {
		panic(v)
	}
}
