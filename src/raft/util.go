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
const Debug = false 

const (
	RPCInfo = iota
	Fatal
	LeaderInfo
	CandidateInfo
	FollowerInfo
	TickerInfo
	UserInfo
	ApplyInfo
	PersistInfo
	SnapshotInfo
	ServerInfo
	ClientInfo
	CondInfo
	TestInfo
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

		flags := log.Lmicroseconds

		//loggers[RPCInfo] = log.New(tmpIO, "[RPCInfo]", flags)
		loggers[LeaderInfo] = log.New(tmpIO, "[LeaderInfo]", flags)
		loggers[Fatal] = log.New(tmpIO, "Fatal:", log.LstdFlags)
		//loggers[CandidateInfo] = log.New(tmpIO, "[CandidateInfo]", flags)
		//loggers[TickerInfo] = log.New(tmpIO, "[TickerInfo]", flags)
		loggers[FollowerInfo] = log.New(tmpIO, "[FollowerInfo]", flags)
		loggers[UserInfo] = log.New(tmpIO, "[UserInfo]", flags)
		//loggers[ApplyInfo] = log.New(tmpIO, "[ApplyInfo]", flags)
		//loggers[PersistInfo] = log.New(tmpIO, "[PersistInfo]", flags)
		//loggers[SnapshotInfo] = log.New(tmpIO, "[SnapshotInfo]", flags)
		loggers[ServerInfo] = log.New(tmpIO, "[ServerInfo]", flags)
		loggers[CondInfo] = log.New(tmpIO, "[CondInfo]", flags)
		loggers[TestInfo] = log.New(tmpIO, "[TestInfo]", flags)
		loggers[ClientInfo] = log.New(tmpIO, "[ClientInfo]", flags)

		runtime.SetFinalizer(loggers[Fatal], func(logger *log.Logger) {
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

func GoID() int {
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
	l.goid = GoID()
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
	l.Rgoid[GoID()] = true
	l.mu.Unlock()
}

func (l *DLock) RUnlock() {
	l.mu.Lock()
	l.Rgoid[GoID()] = false
	l.mu.Unlock()
	l.lock.RUnlock()
}

func Assert(expr bool, v ...interface{}) {
	if !expr {
		logger, enabled := loggers[Fatal]
		if enabled {
			logger.Fatal(v...)
		} else {
			log.Fatal(v...)
		}
	}
}
