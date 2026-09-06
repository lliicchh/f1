// Command account 账号服务，渠道校验、开号、多渠道绑定
//
// 启动要的环境变量见 deploy/s1.env。它和网关是仅有的两个需要 LOGIN_SECRET
// 的进程：这边签发，网关校验
package main

import (
	"github.com/gamedev/f1/internal/account"
	"github.com/gamedev/f1/pkg/node"
)

func main() {
	// Bootstrap 里会依次做 NODE_SEQ 越界校验、etcd 上的 nodeID 唯一性检查，
	// 以及 Redis 关键配置校验。任何一步不过就退出，而非降级，降级会造成生产环境
	// 重复 ID 或者数据丢失
	n, err := node.Bootstrap("account", "lobby")
	if err != nil {
		node.Fatal(err)
	}
	if err := n.Run(account.New()); err != nil {
		node.Fatal(err)
	}
}
