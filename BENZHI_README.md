# StreamEngine

StreamEngine 是一个自建实时流处理引擎，提供事件接入、水位线推进、窗口与会话管理、增量聚合、状态后端、检查点快照与 sink 输出能力，并带有一个浏览器拓扑监控页面。

## 本地构建与运行

```bash
go build -mod=vendor -o bin/streamengine ./cmd/streamengine
./bin/streamengine -addr :8080 -web web
```

启动后打开 `http://localhost:8080/topology` 查看拓扑监控页面，`http://localhost:8080/health` 返回健康状态，`http://localhost:8080/metrics` 返回引擎指标。

## Docker 构建

```bash
./build_benzhi_docker.sh
docker run --rm -p 8080:8080 streamengine:benzhi
```

镜像使用离线 vendor 构建，`GOPROXY=off` 且不依赖外部网络。
