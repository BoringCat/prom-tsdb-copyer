# 普罗米修斯TSDB复制器 <!-- omit in toc -->
## prom-tsdb-copyer <!-- omit in toc -->

- [用途](#用途)
- [用法](#用法)
  - [举例](#举例)
- [注意事项](#注意事项)
- [本地构建](#本地构建)
- [QA](#qa)
- [新旧版本对比](#新旧版本对比)


## 用途
从 TSDB 里面复制一段时间内的指标出来，生成新的块。这些块可以上传到像 [Grafana Mimir][grafana-mimir] 或者 [Thanos][thanos] 这些基于对象存储的系统中，供使用者查询。  
例如，先挑选 `{user="foo"}` 的数据，再用 [`mimirtool` 推送到 foo 租户][mimirtool#backfill]

项目的目的是实现某些数据的永久存储，如果你没有在用上述系统，请用 [prom-migrator][prom-migrator] （或类似的工具） 转移数据

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

## 注意事项
- 按 切分查询的时长 分块，合理的方块可以有效降低内存使用量
  - 例如 50k个15秒 的序列，按2小时分块只需要700多M内存，按24小时分块要4.8G内存
- 所有数据拷贝完成后，才调用 `Compactor` 对写入的数据进行打包
- 并发只在分块逻辑中使用，压缩不进行并发操作

## 本地构建
```sh
make
```

## QA
- Q: 为什么新版完全移除了远程支持？  
  A: 它太慢了，并且远程读还需要转换数据类型

## 新旧版本对比
数据源: 有 1154916774 个指标的数据  
时间跨度: 2023-02-24T14:00:00.692+08:00 => 2023-03-01T02:00:00+08:00

新旧版本处理速率基本一致，但新版本降低了40%的CPU时间，以及75%的内存使用量

意味着新版本可以使用更多的并发，提升CPU利用率

<details>
<summary>旧版本</summary>

注: 使用 commit-count 配置来保证旧版本不会在写入中途进行Commit操作，保证逻辑一致性
```sh
/bin/time -v prom-tsdb-copyer-linux-amd64 copy ./_old_data/ ./_new_data \
  -S '0000-01-01 08:00:00' \
  -E '9999-12-31 23:59:59.999' \
  --commit-count=10485765156 \
>_test.log 2>&1
```

```yaml
User time (seconds):                         537.73
System time (seconds):                       32.58
Percent of CPU this job got:                 129%
Elapsed (wall clock) time (h:mm:ss or m:ss): 7:20.58
Average shared text size (kbytes):           0
Average unshared data size (kbytes):         0
Average stack size (kbytes):                 0
Average total size (kbytes):                 0
Maximum resident set size (kbytes):          3499336
Average resident set size (kbytes):          0
Major (requiring I/O) page faults:           46
Minor (reclaiming a frame) page faults:      1818527
Voluntary context switches:                  148016
Involuntary context switches:                37746
Swaps:                                       0
File system inputs:                          2602496
File system outputs:                         9757480
Socket messages sent:                        0
Socket messages received:                    0
Signals delivered:                           0
Page size (bytes):                           4096
Exit status:                                 0
```

</details>
<details>
<summary>新版本</summary>

```sh
/bin/time -v ./_dist/prom-tsdb-copyer ./_old_data/ ./_new_data/ \
  --show-metrics \
  --debug \
>_test2.log 2>&1
```
```yaml
User time (seconds):                         344.23
System time (seconds):                       19.01
Percent of CPU this job got:                 84%
Elapsed (wall clock) time (h:mm:ss or m:ss): 7:07.99
Average shared text size (kbytes):           0
Average unshared data size (kbytes):         0
Average stack size (kbytes):                 0
Average total size (kbytes):                 0
Maximum resident set size (kbytes):          813832
Average resident set size (kbytes):          0
Major (requiring I/O) page faults:           0
Minor (reclaiming a frame) page faults:      2241313
Voluntary context switches:                  134209
Involuntary context switches:                38977
Swaps:                                       0
File system inputs:                          64
File system outputs:                         9741208
Socket messages sent:                        0
Socket messages received:                    0
Signals delivered:                           0
Page size (bytes):                           4096
Exit status:                                 0
```

</details>

[grafana-mimir]: https://github.com/grafana/mimir
[thanos]: https://github.com/thanos-io/thanos
[prom-migrator]: https://github.com/timescale/promscale/tree/master/migration-tool/cmd/prom-migrator
[mimirtool#backfill]: https://grafana.com/docs/mimir/latest/manage/tools/mimirtool/#backfill
