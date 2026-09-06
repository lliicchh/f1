# 游戏服务器（slots + RPG）

文档在 [.doc/README.md](.doc/README.md)：架构、ID 与分片、刷盘与 fencing、
slots 与合规、部署运维、故障行为、代码约定，都在那一份里。

```bash
make test    # 单元 + 集成测试，内嵌 etcd / NATS / Redis，不需要 Docker
make build   # 构建全部服务
make up ENV_FILE=deploy/s1.env PROJECT=game-s1 TAG=v1.0.0
```
