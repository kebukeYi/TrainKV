package benchmk

import (
	_ "net/http/pprof"
)

//
//func TestMain(m *testing.M) {
//	// 启动 pprof HTTP 服务
//	runtime.GOMAXPROCS(2)
//	// 启用 mutex 性能分析
//	runtime.SetMutexProfileFraction(1)
//	// 启用 block 性能分析
//	runtime.SetBlockProfileRate(1)
//
//	go func() {
//		// 加个日志提示，确认服务已开启
//		fmt.Println("🚀 pprof HTTP server started at http://localhost:6060/debug/pprof/")
//		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
//			fmt.Printf("pprof server error: %v\n", err)
//		}
//	}()
//
//	// 让 pprof 服务有 1 秒时间完全启动
//	time.Sleep(2 * time.Second)
//
//	// 1. 运行所有的基准测试/单元测试
//	code := m.Run()
//
//	// 2. 测试跑完了！但是进程不要退出！
//	fmt.Println("\n========================================")
//	fmt.Println("✅ 基准测试已执行完毕！")
//	fmt.Println("📊 pprof 服务依然在运行中...")
//	fmt.Println("🌐 请打开浏览器访问: http://localhost:6060/debug/pprof/")
//	fmt.Println("🕒 或者使用 go tool pprof 拉取数据：")
//	fmt.Println("   go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap")
//	fmt.Println("   go tool pprof -http=:8081 http://localhost:6060/debug/pprof/profile?seconds=30")
//	fmt.Println("========================================")
//	fmt.Println("按 Enter 键退出程序并停止服务...")
//
//	// 3. 关键点：阻塞在这里，等待用户按回车键
//	fmt.Scanln()
//
//	// 4. 按了回车才退出
//	os.Exit(code)
//}
