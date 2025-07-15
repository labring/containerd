//go:build linux

package devbox

const (
	// 改插件默认的存储路径
	DefaultRootDir = "/var/lib/containerd/io.sealos.labring.devbox"
	// 该插件提供 grpc 服务的 socks 文件名，路径为 paths.Join(rootDir, SocksFileName)
	SocksFileName = "grpc.socks"
)
