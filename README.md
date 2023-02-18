# 普罗米修斯TSDB复制器
## prom-tsdb-copyer

## 用途
从 TSDB 里面复制一段时间内的指标出来，生成新的块。这些块可以上传到像 [Grafana Mimir][1] 或者 [Thanos][2] 这些基于对象存储的系统中，供使用者查询。

项目的目的是实现某些数据的永久存储，如果你没有在用上述系统，请用 [prom-migrator][3] （或类似的工具） 转移数据

## 用法
```sh
usage: prom-tsdb-copyer --start-time=START-TIME --end-time=END-TIME [<flags>] <from> <toDir>

普罗米修斯TSDB复制器

Flags:
      --help                   Show context-sensitive help (also try --help-long and --help-man).
  -S, --start-time=START-TIME  数据开始时间
  -E, --end-time=END-TIME      数据结束时间
  -Q, --query-split=1h         切分新块的时长
  -B, --block-split=24h        切分新块的时长
      --verify                 是否验证数据量
  -T, --multi-thread=-1        并行多少个复制(0=GOMAXPROCS)
  -l, --label-query=LABEL-QUERY ...  
                               查询label（k=v）
  -L, --label-append=LABEL-APPEND ...  
                               增加label（k=v）

Args:
  <from>   源TSDB文件夹/Remote Read 地址
  <toDir>  目标TSDB文件夹
```
### 举例
```sh
# 本地拷贝某一天的内容
prom-tsdb-copyer -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' old_data/ new_data/
# 远程拷贝某一天的内容
prom-tsdb-copyer -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' http://prometheus:9090/api/v1/read new_data/
# 按查询内容拷贝
prom-tsdb-copyer -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -l job=nodes -l 'instance=~192\.168\.1\.\d+' -l 'hostname!=foonode' -l '__name__!~go.*' old_data/ new_data/
# 在拷贝的内容里加标签
prom-tsdb-copyer -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -L create_from=copyer -L storage=persistent old_data/ new_data/
# 并发拷贝（自动协程数量）
prom-tsdb-copyer -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -T0 old_data/ new_data/
# 按周分块
prom-tsdb-copyer -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' -B 168h old_data/ new_data/
# 拷贝完以后通过meta.json验证指标数
prom-tsdb-copyer -S '2023-02-17 00:00:00' -E '2023-02-18 00:00:00' --verify old_data/ new_data/
```

## 本地构建
```sh
make
```

## 注意事项
- 单次查询的最大时间区间为 **1小时** 这个结果是经过大量测试总结的，超过这个时间会导致一堆奇奇怪怪的问题  
  最常见的就是 `panic: out of bounds`。这个和源 TSDB 块的间隔有关，如果你的查询同时匹配到两个块，可能会导致返回了乱序的时序（即使 Querier.Select 传入了 sortSeries = true）  
  ~~（但你看[源码][4]这个选项是没有开的，本来数据就是顺序的）~~
- 1小时的单次查询区间性能是最高的，因为写入每个区间的每个序列都会进行一次commit以减少内存使用量  
  （否则再多内存都不够用）
- RemoteRead 读取远程数据时会对远程 Prometheus 产生大量负载，并发时需要注意远程内存消耗

[1]: https://github.com/grafana/mimir
[2]: https://github.com/thanos-io/thanos
[3]: https://github.com/timescale/promscale/tree/master/migration-tool/cmd/prom-migrator
[4]: local.go#L52