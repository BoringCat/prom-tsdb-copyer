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
usage: prom-tsdb-copyer [<flags>] <source> <target>

普罗米修斯TSDB复制器

Flags:
  -h, --help                Show context-sensitive help (also try --help-long and --help-man).
  -v, --version             Show application version.
      --from=FROM           数据开始时间
      --to=TO               数据结束时间
  -S, --query-duration=2h   切分查询的时长
  -B, --block-duration=24h  切分新块的时长
  -T, --write-thread=1      每个读取并行多少个写入（注意内存使用）(0=不限制)
  -l, --label-query=LABEL-QUERY ...  
                            查询label（k=v）
  -L, --label-append=LABEL-APPEND ...  
                            增加label（k=v）
      --gc-pre-series=GC-PRE-SERIES  
                            写入多少序列后GC
  -d, --debug               输出Debug日志到终端
      --show-metrics        输出监控指标到终端
      --gc-after-flush      写入完成后手动GC

Args:
  <source>  源TSDB文件夹
  <target>  目标TSDB文件夹
```
### 举例
```sh
# 过滤指标拷贝
prom-tsdb-copyer -l '__name__=up' old_data/ new_data/
# 拷贝某一天的内容
prom-tsdb-copyer --from='2023-02-17 00:00:00' --to='2023-02-18 00:00:00' old_data/ new_data/
# 按查询内容拷贝
prom-tsdb-copyer copy --from='2023-02-17 00:00:00'  --to='2023-02-18 00:00:00' -l job=nodes -l 'instance=~192\.168\.1\.\d+' -l 'hostname!=foonode' -l '__name__!~go.*' old_data/ new_data/
# 在拷贝的内容里加标签
prom-tsdb-copyer copy --from='2023-02-17 00:00:00'  --to='2023-02-18 00:00:00' -L create_from=copyer -L storage=persistent old_data/ new_data/
# 并发拷贝
prom-tsdb-copyer copy --from='2023-02-17 00:00:00'  --to='2023-02-18 00:00:00' -T4 old_data/ new_data/
# 按周分块
prom-tsdb-copyer copy --from='2023-02-17 00:00:00'  --to='2023-02-18 00:00:00' -B 168h old_data/ new_data/
```

## 本地构建
```sh
make
```

## 注意事项
- 按 切分查询的时长 分块，合理的方块可以有效降低内存使用量
  - 例如 50k个15秒 的序列，按2小时分块只需要700多M内存，按24小时分块要4.8G内存
- 所有数据拷贝完成后，才调用 `Compactor` 对写入的数据进行打包

## QA
- Q: 为什么新版完全移除了远程支持？  
  A: 它太慢了

[1]: https://github.com/grafana/mimir
[2]: https://github.com/thanos-io/thanos
[3]: https://github.com/timescale/promscale/tree/master/migration-tool/cmd/prom-migrator
