// Command lobby 是Lobby：玩家对象常驻内存 + 非战斗玩法逻辑，分片独占（§2.1 / §2.2）
//
// 启动所需环境变量见 deploy/s1.env；其中 SERVER_ID 与 NODE_SEQ 必须手动配置（§3.2 / §3.3）。
package main

import (
	"github.com/gamedev/f1/internal/lobby"
	"github.com/gamedev/f1/pkg/node"
)

func main() {
	// Bootstrap 内部依次完成：
	//   §3.4 第一道 NODE_SEQ 越界校验 → 第二道 etcd nodeID 唯一性自检
	//   §6.4 Redis 关键配置校验（maxmemory-policy 必须 noeviction）
	// 任一步失败都直接退出，绝不降级 —— 降级就是在生产制造重复 ID 或丢档。
	n, err := node.Bootstrap("lobby", "lobby")
	if err != nil {
		node.Fatal(err)
	}
	if err := n.Run(lobby.New()); err != nil {
		node.Fatal(err)
	}
}
