# 普罗米修斯TSDB复制器 <!-- omit in toc -->
## prom-tsdb-copyer <!-- omit in toc -->

- [用途](#用途)
- [用法](#用法)
  - [举例](#举例)
- [本地构建](#本地构建)
- [注意事项](#注意事项)
- [QA](#qa)


## 用途
从 TSDB 里面复制一段时间内的指标出来，生成新的块。这些块可以上传到像 [Grafana Mimir][1] 或者 [Thanos][2] 这些基于对象存储的系统中，供使用者查询。

项目的目的是实现某些数据的永久存储，如果你没有在用上述系统，请用 [prom-migrator][3] （或类似的工具） 转移数据

## 用法
```sh
usage: prom-tsdb-copyer [<flags>] <command> [<args> ...]

普罗米修斯TSDB复制器

Flags:
  -h, --help     Show context-sensitive help (also try --help-long and --help-man).
  -V, --version  Show application version.

Commands:
  help [<command>...]
    Show help.

  copy --start-time=START-TIME --end-time=END-TIME [<flags>] <from> <toDir>
    从本地或者远程复制TSDB到本地

  compact [<flags>] <dirs>...
    根据间隔压缩本地TSDB
```
```sh
usage: prom-tsdb-copyer copy --start-time=START-TIME --end-time=END-TIME [<flags>] <from> <toDir>

从本地或者远程复制TSDB到本地

Flags:
  -h, --help                   Show context-sensitive help (also try --help-long and --help-man).
  -V, --version                Show application version.
  -S, --start-time=START-TIME  数据开始时间
  -E, --end-time=END-TIME      数据结束时间
  -Q, --query-split=1h         切分查询的时长
  -B, --block-split=24h        切分新块的时长
      --verify                 是否验证数据量
      --allow-out-of-order     是否允许乱序写入
      --thanos-metadata        是否追加Thanos的元数据
  -T, --multi-thread=-1        并行多少个复制(0=GOMAXPROCS)
  -l, --label-query=LABEL-QUERY ...  
                               查询label（k=v）
  -L, --label-append=LABEL-APPEND ...  
                               增加label（k=v）
      --commit-count=10240     分组提交指标写入的数量
      --tenant=TENANT          分租户用的Label
      --default-tenant=DEFAULT-TENANT  
                               默认租户值

Args:
  <from>   源TSDB文件夹/Remote Read 地址
  <toDir>  目标TSDB文件夹
```
```sh
usage: prom-tsdb-copyer compact [<flags>] <dirs>...

根据间隔压缩本地TSDB

Flags:
  -h, --help             Show context-sensitive help (also try --help-long and --help-man).
  -V, --version          Show application version.
  -B, --block-split=24h  切分新块的时长
  -T, --multi-thread=-1  并行多少个复制(0=GOMAXPROCS)

Args:
  <dirs>  源TSDB文件夹/Remote Read 地址
```
### 举例
```sh
# 本地拷贝某一天的内容
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' old_data/ new_data/
# 远程拷贝某一天的内容
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' http://prometheus:9090/api/v1/read new_data/
# 按查询内容拷贝
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -l job=nodes -l 'instance=~192\.168\.1\.\d+' -l 'hostname!=foonode' -l '__name__!~go.*' old_data/ new_data/
# 在拷贝的内容里加标签
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -L create_from=copyer -L storage=persistent old_data/ new_data/
# 并发拷贝（自动协程数量）
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -T0 old_data/ new_data/
# 按周分块
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -B 168h old_data/ new_data/
# 拷贝完以后通过meta.json验证指标数
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' --verify old_data/ new_data/
# 按标签 job 的值分租户
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' --tenant job old_data/ new_data/
# 按标签 groupname 的值分租户，没有值的扔到 nogroup 里面
prom-tsdb-copyer copy -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' --tenant groupname --default-tenant nogroup old_data/ new_data/
```

## 本地构建
```sh
make
```

## 注意事项
- 最大查询间隔为 `1h` ~~（你可以超，但TSDB也可以panic）~~
- 所有数据拷贝完成后，才调用 `Compactor` 对写入的数据进行打包，每个时间段生成一个块
- `RemoteRead` 读取远程数据时会对远程 `Prometheus` 产生大量负载，并发时需要注意远程内存消耗  
  大概会增加**1-2倍**的内存使用，

## QA
- Q: 为什么没有 RemoteWrite 支持  
  A: 因为目的是实现完全兼容 Prometheus 的历史记录保存，如果得用 RemoveWrite 写到某个系统中再用其他PromQL兼容系统读的话太麻烦  
    ~~加上本来就有 Thanos 环境，直接弹一个指向这些块的 Store 就完事了(也是为什么有加标签这个功能(去重.jpg))~~  
    但不排除以后会支持，看有没有需求，或者 Prometheus + RemoveRead 的架构管理起来麻不麻烦

[1]: https://github.com/grafana/mimir
[2]: https://github.com/thanos-io/thanos
[3]: https://github.com/timescale/promscale/tree/master/migration-tool/cmd/prom-migrator
