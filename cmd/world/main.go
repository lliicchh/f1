// Command world 世界 BOSS 和活动调度这类全局唯一逻辑，选主主备
//
// 启动要的环境变量见 deploy/s1.env，其中 SERVER_ID 和 NODE_SEQ 必须手动配
package main

import (
	"github.com/gamedev/f1/internal/world"
	"github.com/gamedev/f1/pkg/node"
)

func main() {
	// Bootstrap 里会依次做 NODE_SEQ 越界校验、etcd 上的 nodeID 唯一性检查，
	// 以及 Redis 关键配置校验。任何一步不过就退出，而非降级，降级会造成生产环境
	// 重复 ID 或者数据丢失
	n, err := node.Bootstrap("world", "world")
	if err != nil {
		node.Fatal(err)
	}
	if err := n.Run(world.New()); err != nil {
		node.Fatal(err)
	}
}
