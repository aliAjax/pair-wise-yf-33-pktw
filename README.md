# 多站卫星地面站排程系统

使用标准库与 SQLite 实现的独立排程原型。系统维护卫星、地面站、天线、维护时段、可见窗口、租户配额和数据请求，并检查速率、数据量、截止时间、设备重叠、卫星同时接收、天气和租户配额。

## 运行

```bash
python3 app.py --db satellite_scheduling.db
```

默认监听 `127.0.0.1:8204`，首页 `/`，健康检查 `/health`。

身份头为 `X-User-Id`、`X-Role`；`requester` 还需 `X-Tenant`。角色：`viewer`、`requester`、`operator`、`commander`、`auditor`。

## 主要接口

- `POST /api/satellites`、`/api/stations`、`/api/antennas`、`/api/maintenance`、`/api/visibility-windows`、`/api/quotas`：资源配置。
- `POST /api/requests`：创建数据接收请求。
- `POST /api/requests/{id}/schedule`、`/reschedule`：排程或重排被抢占请求。
- `POST /api/schedules/{id}/start`、`/complete`、`/cancel`、`/preempt`：接收状态和紧急抢占。
- `POST /api/visibility-windows/{id}/change`：窗口变更处置。请求体带 `expected_revision` 做乐观并发：版本过期返回 409 `window_revision_conflict`，窗口、排程和请求保持上一版。提交前先按窗口版本与排程快照生成处置单（`commit:false` 可只预览），影响分四类：`retained` 保留、`compressed` 压缩到新窗口（必要时提速）、`preserve_received_data` 已接收数据原样保留、`preempted` 抢占；容量不足的未接收请求进入排队（请求状态 `queued`），按优先级（高者优先）仲裁。写入失败时事务回滚恢复提交前数值，并登记 `failed` 处置单。
- `GET /api/state`、`GET /api/schedules/{id}`、`GET /api/dispositions/{id}`：权限化状态查询，`/api/state` 含 `dispositions`（处置单与影响明细）和 `queue`（排队队列）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
