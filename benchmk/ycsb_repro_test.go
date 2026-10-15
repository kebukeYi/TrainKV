package benchmk

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimePprof "runtime/pprof"

	TrainKV "github.com/kebukeYi/TrainKV/v2"
	"github.com/kebukeYi/TrainKV/v2/common"
	"github.com/kebukeYi/TrainKV/v2/lsm"
)

// TestYCSBWorkloadAReopenRepro 复现: 纯写 load(新库) → Close → 重开 →
// workloada(50% 读 + 50% 更新), 观察写路径是否崩溃, 用于定位瓶颈;
// 用法: go test ./benchmk -run TestYCSBWorkloadAReopenRepro -cpuprofile=/tmp/repro_cpu.prof -blockprofile=/tmp/repro_block.prof -mutexprofile=/tmp/repro_mutex.prof -v
const reproDirA = "/tmp/golanddata/repro_ycsb_a"

var (
	reproNKeys   = 1000000
	reproValSize = 1024
	reproThreads = 16
)

func reproKey(i int) []byte {
	return []byte(fmt.Sprintf("user%08d", i))
}

type reproStats struct {
	ops      atomic.Int64
	totalUs  atomic.Int64 // 单 op 总耗时(us)累计
	totalWUs atomic.Int64 // update 中 Commit 段耗时(us)累计
	wOps     atomic.Int64
}

func reproWorker(phase int, db *TrainKV.TrainKV, st *reproStats, stop chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	val := make([]byte, reproValSize)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(phase)*100000))
	var (
		totalUs, totalWUs int64
		ops, wOps         int64
	)
	for {
		select {
		case <-stop:
			st.ops.Add(ops)
			st.totalUs.Add(totalUs)
			st.wOps.Add(wOps)
			st.totalWUs.Add(totalWUs)
			return
		default:
		}
		opStart := time.Now()
		if rnd.Intn(100) < 50 {
			// read op
			txn := db.NewTransaction(false)
			_, err := txn.Get(reproKey(rnd.Intn(reproNKeys)))
			if err != nil {
				panic(fmt.Sprintf("read err: %v", err))
			}
			txn.Discard()
		} else {
			// update op: 与 YCSB trainkv adapter 一致: 写事务内先读(RMW)再写同一 key;
			// DetectConflicts=true 下并发同 key RMW 会返回 ErrConflict, 按引擎建议重试;
			attempts := 0
			for {
				attempts++
				if attempts > 200 {
					panic("update retry limit exceeded")
				}
				txn := db.NewTransaction(true)
				key := reproKey(rnd.Intn(reproNKeys))
				entry, err := txn.Get(key)
				if err != nil {
					panic(fmt.Sprintf("update-get err: %v", err))
				}
				if len(entry.Value) == 0 {
					panic("empty value on update-get")
				}
				val[0] = byte(rnd.Intn(256))
				val[1] = byte(rnd.Intn(256))
				if err := txn.Set(key, val); err != nil {
					panic(fmt.Sprintf("set err: %v", err))
				}
				commitStart := time.Now()
				if _, err := txn.Commit(); err == common.ErrConflict {
					continue // 冲突重试(Commit 已内部 Discard)
				} else if err != nil {
					panic(fmt.Sprintf("commit err: %v", err))
				}
				totalWUs += time.Since(commitStart).Microseconds()
				wOps++
				break
			}
		}
		totalUs += time.Since(opStart).Microseconds()
		ops++
	}
}

func runReproPhase(name string, db *TrainKV.TrainKV, duration time.Duration) (readOps, writeOps, totalOps int64, readUsPerOp, writeUsPerOp float64) {
	st := &reproStats{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < reproThreads; i++ {
		wg.Add(1)
		go reproWorker(i, db, st, stop, &wg)
	}
	go func() {
		time.Sleep(duration / 2)
		f, err := os.Create("/tmp/repro_goro.txt")
		if err == nil {
			_ = runtimePprof.Lookup("goroutine").WriteTo(f, 2)
			f.Close()
		}
	}()
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	totalOps = st.ops.Load()
	wOps := st.wOps.Load()
	rOps := totalOps - wOps
	fmt.Printf("[%s] %d s: total=%d ops/s=%d reads=%d writes=%d readAvg=%dus writeAvg=%dus commitAvg=%dus\n",
		name, int(duration.Seconds()), totalOps, totalOps/int64(duration.Seconds()),
		rOps, wOps, st.totalUs.Load()/totalOps, (st.totalUs.Load()-st.totalWUs.Load())/max64(rOps, 1), st.totalWUs.Load()/max64(wOps, 1))
	return rOps, wOps, totalOps, 0, 0
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func TestYCSBWorkloadAReopenRepro(t *testing.T) {
	if _, err := os.Stat(reproDirA); err == nil {
		if err = os.RemoveAll(reproDirA); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(reproDirA, 0o755); err != nil {
		t.Fatal(err)
	}

	opt := lsm.GetDefaultOpt(reproDirA)
	opt.SyncWrites = false
	opt.NumCompactors = 4

	// ===== Phase 1: load(新库纯写) =====
	db, err, _ := TrainKV.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	loadStart := time.Now()
	loadDone := make(chan struct{})
	var loadWG sync.WaitGroup
	var written atomic.Int64
	for i := 0; i < reproThreads; i++ {
		loadWG.Add(1)
		go func(base int) {
			defer loadWG.Done()
			val := make([]byte, reproValSize)
			for j := base; j < reproNKeys; j += reproThreads {
				txn := db.NewTransaction(true)
				if err := txn.Set(reproKey(j), val); err != nil {
					panic(err)
				}
				if _, err := txn.Commit(); err != nil {
					panic(err)
				}
				written.Add(1)
			}
		}(i)
	}
	go func() {
		loadWG.Wait()
		close(loadDone)
	}()
	<-loadDone
	loadDur := time.Since(loadStart)
	fmt.Printf("[load] keys=%d in %v -> %d ops/s\n", written.Load(), loadDur.Round(time.Millisecond), int64(float64(written.Load())/loadDur.Seconds()))

	// ===== Close & Reopen =====
	closeStart := time.Now()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("[close] took %v\n", time.Since(closeStart).Round(time.Millisecond))
	openStart := time.Now()
	db, err, _ = TrainKV.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("[reopen] took %v\n", time.Since(openStart).Round(time.Millisecond))

	// ===== Phase 2: workloada(50/50) =====
	runReproPhase("workloadA", db, 30*time.Second)
}
