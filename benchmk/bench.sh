#!/bin/bash
set -e  # 遇到错误立即停止

# ==================== 配置区（按需修改） ====================
PKG="."                     # 包路径
BENCH="BenchmarkWriteTxnSet"        # 要跑的基准测试名字
FILE_PREFIX="WriteTxn"             # 输出文件前缀（不含特殊字符）
TIME="10s"                          # 压测时长（建议长一点，采样更准）
OUT_DIR="../bench_profile"                 # 输出文件夹
# ===========================================================

echo "🚀 开始运行基准测试: $BENCH"
echo "⏱️  测试时长: $TIME"
echo "📁 输出目录: $OUT_DIR"

# 1. 创建输出目录
mkdir -p "$OUT_DIR"

# 2. 执行 go test，一次性生成所有 profile 文件
# 关键点：1. 续行符 \ 后面绝对不能有任何字符（包括空格和 #）
#         2. 参数名是 -cpuprofile（不是 -cpprofile）
#         3. 所有参数都放在 go test 那一行内部
go test "$PKG" -run xxx -bench "$BENCH" -benchtime="$TIME" \
  -cpuprofile="$OUT_DIR/${FILE_PREFIX}_cpu.prof" \
  -memprofile="$OUT_DIR/${FILE_PREFIX}_mem.prof" \
  -memprofilerate=1 \
  -blockprofile="$OUT_DIR/${FILE_PREFIX}_block.prof" \
  -mutexprofile="$OUT_DIR/${FILE_PREFIX}_mutex.prof" \
  -trace="$OUT_DIR/${FILE_PREFIX}_trace.out" \
  -outputdir="$OUT_DIR"

echo ""
echo "✅ 所有 profile 文件已生成在 $OUT_DIR 目录下："
ls -lh "$OUT_DIR"

echo ""
echo "📊 一键分析命令（复制即用）："
echo "  CPU:     go tool pprof -http=:8080 $OUT_DIR/${FILE_PREFIX}_cpu.prof"
echo "  MEM:     go tool pprof -http=:8081 $OUT_DIR/${FILE_PREFIX}_mem.prof"
echo "  BLOCK:   go tool pprof -http=:8082 $OUT_DIR/${FILE_PREFIX}_block.prof"
echo "  MUTEX:   go tool pprof -http=:8083 $OUT_DIR/${FILE_PREFIX}_mutex.prof"
echo "  TRACE:   go tool trace -http=:8084 $OUT_DIR/${FILE_PREFIX}_trace.out"