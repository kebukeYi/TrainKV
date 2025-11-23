module github.com/kebukeYi/TrainKV

go 1.24.0

toolchain go1.24.10

require (
	github.com/cespare/xxhash/v2 v2.3.0 // 指定正式tag发布版本
	github.com/golang/protobuf v1.5.4
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.11.1
	golang.org/x/sys v0.38.0
	google.golang.org/protobuf v1.36.10 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect; indirect //间接依赖
	github.com/gofrs/flock v0.13.0
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect; 指定提交记录版本号;
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/kr/text v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
)
