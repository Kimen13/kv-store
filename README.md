# 配置中心 KV Store 实验

本项目选择第 4 个场景「配置中心」。每个配置值是字符串。发布方支持按版本修改，读取方可以做线性一致读，也可以通过 Watch 和短期缓存减少重复读取。

## 运行

需要 Python 3.10 或更新版本。仅使用标准库，无需安装包，无需管理员权限。复制或解压本目录后，在该目录执行：

~~~sh
python experiments.py
~~~

实验会启动三个本机副本进程，运行九组断言，逐条打印 JSON，并在 results/ 中生成 results.json 与 results.csv。故障数据写入临时目录，进程和临时目录会在实验结束后回收。第三组会实际等待至少 10 秒。results/ 初始附有一次完整运行的结果，重新运行会覆盖该目录的同名文件。可指定新目录保存复跑数据：

~~~sh
python experiments.py --out rerun
python experiments.py --case 07_concurrent_cas --out demo
python experiments.py --help
~~~

若 Windows 的 python 命令不可用，可用 py -3。推荐先演示 07_concurrent_cas 与 09_lost_reply，再完整运行九项。

## 代码入口

| 文件 | 内容 |
|---|---|
| kvstore.py | SQLite 持久化、子进程 Pipe RPC、固定主节点协调器、版本与 Watch、缓存 |
| experiments.py | 九项故障与负载实验、通过条件、数据导出 |
| results/results.json | 一次实跑的完整输出 |
| results/results.csv | 同一次实跑的 CSV 记录 |

在独立 Python 脚本里用公开 API 时，将创建集群的代码放在 main guard 内，以便 spawn 启动子进程：

~~~python
from pathlib import Path
from tempfile import TemporaryDirectory
from kvstore import Cache, Cluster

if __name__ == "__main__":
    with TemporaryDirectory() as directory, Cluster(Path(directory)) as kv:
        first = kv.put("feature/pay", "off", "publish-1")
        print(kv.get("feature/pay"))
        cached = Cache(kv, "feature/pay", ttl=1)
        print(cached.get())
        kv.put("feature/pay", "on", "publish-2", expected=first["revision"])
        print(cached.poll())
        print(cached.get())
~~~

请求方负责为不同逻辑写生成不同的 request_id。超时重试时保持同一个 ID 和同样的参数。expected=0 表示仅当键不存在时执行 CAS。CAS 返回 ok=False 表示版本不匹配。

## 一致性与恢复

- A 是固定主节点，B、C 是保存复制日志的投票副本。协调器只有一个，串行处理 GET、PUT、CAS 和 Watch。A 与至少一个跟随节点需可通信。
- A 先保存一条预备日志。跟随副本持久化同一条记录后，A 用一个 SQLite 事务提交配置、幂等请求结果和提交位点。该提交构成写入的线性化点。A 只读已提交数据，强读与写入使用相同的锁。若提交前留下预备日志，下一次写先尝试完成它，调用方应按请求结果可能未知处理超时。
- log.idx 是全局递增的序号，成功修改时也成为该键的新版本。CAS 失败保留原键版本，仍持久化拒绝结果并推进全局游标。Watch 按游标补读历史，只产出已提交的目标键变更。
- Cache.poll 执行一次补读。Cache.follow(stop, interval=0.05) 返回可终止的轮询迭代器，断链时保留游标以便恢复。缓存只接收更高的键版本。缓存有效期从建立它的强读开始用单调时钟计时，Watch 更新不延长有效期。超期且强读失败时，缓存向调用方抛出 Unavailable。
- D 作为非投票学习副本从持久化日志尾续传。追平时表示日志已经同步。D 一直不参与多数决策，也一直不接收客户端请求。

## 实验对应关系

| # | 注入与观测 |
|---|---|
| 01 | 真正终止 B。A+C 仍能写，B 重启后补齐日志。 |
| 02 | 双向隔离 C。初次发送后各重发三次客户端命令和复制包。验证只修改一次。隔离 A 后验证拒绝读写。 |
| 03 | 旧包入队并延迟 10 秒。先交付新包和新事件，再交付旧包和旧事件。日志和键版本保持不变。 |
| 04 | 真正终止 A，使用相同 SQLite 文件启动新进程。验证原配置和去重结果、下一次写的版本。 |
| 05 | D 在复制 3/6 条后宕机。重启从 3 续传到 6，再追到 7。B、C 同时停机后，D 不形成多数派。 |
| 06 | 两套 1,000 次读的序列分别包含 60% 与 50% 热点读。每套重放两遍，对照强读次数、P50、P95。随后用 50 ms 轮询验证配置发布及通知。 |
| 07 | 两个线程基于同一个键版本竞争 CAS。恰有一个成功，同时核对值、版本和 Watch 事件。 |
| 08 | 注入墙上时钟回拨 60 秒。使用可控单调时钟跨过 1 秒 TTL，检查过期拒绝和恢复刷新。 |
| 09 | A 提交成功后丢弃响应。重启 A 后用相同请求 ID 重试，验证一次提交和一条变更。 |

## 实验边界

进程终止和磁盘恢复发生在实际的本机进程及独立 SQLite 文件上。链路故障发生在本地 Pipe RPC 的注入层。每项实验假设 A 的磁盘保留，固定主节点不会切换，副本非恶意。主节点停机期间以及不满足多数条件时，强读和写入均暂停。实验没有自动选主、磁盘永久丢失恢复、跨机网络部署、日志压缩或投票成员变更。

热点延迟来自本机串行纯读，缓存对照阶段不计后台 Watch 成本，也没有跨机性能结论。Watch 检测时间从提交请求发起测到轮询线程收到事件，包含写入与调度延迟。进程 PID、CAS 获胜线程和延迟随运行而变化。

## 原理资料

- SQLite 原子提交机制 <https://www.sqlite.org/atomiccommit.html>
- Raft 论文，对照自动选主与多数复制 <https://raft.github.io/raft.pdf>
- etcd 的 KV 与 Watch 语义 <https://etcd.io/docs/v3.6/learning/api_guarantees/>
- etcd 学习副本设计 <https://etcd.io/docs/v3.6/learning/design-learner/>
