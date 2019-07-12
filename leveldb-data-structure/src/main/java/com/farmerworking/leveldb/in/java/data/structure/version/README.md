# 	Version，VersionSet，Compaction，VersionEdit 个人理解



### Version

Version 负责保存 leveldb 磁盘 sstable 文件的 metadata 信息。leveldb 将 sstable 分为 7 个层级，每个层级可以有多个 sstable 文件，每个 sstable 文件的 metadata 包含如下信息：

1. fileNumber
2. fileSize
3. smallestKey
4. largestKey



因为 Version 包含所有 sstable 的 metadata 信息，所以，Version 需要履行以下职责：

1. 数据读取 —— iterator 遍历形式 + 指向型 get 形式
2. Compaction 相关：
   1. memtable compaction 输出层级判断 —— 是否 overlap，是否 over MaxGrandParentOverlapBytes
   2. seek compaction —— 更新每个 metadata 里面允许 seek 的次数，判断是否需要发起 seek compaction



### VersionBuilder

 leveldb 恢复的过程中需要读取多个 VersionEdit 并最后保存在 Version 当中

一种做法：

Version 1 + edit 1 —— Version 2 + edit 2 —— Version 3 + edit 3 …… —— Version final

上述做法可行，但是会产生很多不必要的中间态 Version，因此引入 VersionBuilder 得到以下处理流程：

new VersionBuilder(Version 1) + edit 1 + edit 2 + …… —— Version final



个人点评：

VersionBuilder 在实际实现上，和一般 Builder 并不一致。

一般 Builder build 的过程中只会修改自身的状态，然后根据自身的状态，在最后产出的阶段对外界状态进行变更

而 leveldb 原生 VersionBuilder 的实现，在 apply edit 阶段会立刻变更 VersionSet 的 compactPoint 状态，在 saveTo 阶段会产出 Version 的状态



### VersionEdit

MANIFEST-XXX 文件中包含一系列 VersionEdit 记录，用于 leveldb 恢复



VersionEdit 包含主要 8 个状态，这 8 个状态又可以根据恢复过程的使用方式分为 2 类：

1. 更新型 —— 值即是状态本身，覆盖式更新，恢复结束时的值就是 leveldb 该状态应该处于的状态
2. 指向操作型 —— 值是某项操作的参数，恢复过程中使用值作为参数，顺序执行一系列操作，使得执行对象达到理应的状态（leveldb 中这个执行对象是文件集合，这个集合删除和添加了哪些文件）

更新型：

1. logNumber
2. prevLogNumber
3. nextFileNumber
4. lastSequence
5. comparatorName
6. compactPointer



指向操作型：

1. deletedFiles
2. newFiles



#### logNumber

日志是保证数据持久化的重要实现手段。leveldb 在启动时会通过重放日志中记录的操作，保证不因为断电等原因产生数据丢失。

日志文件不断增加，既会占用磁盘空间，也会减慢 leveldb 重启时的启动速率

基于上面的原因，引入 logNumber 作为持久化状态之一。文件编号在 logNumber 之前的日志文件可以安全删除释放磁盘空间，日志重放从文件编号大于等于 logNumber 的日志文件开始



Todo: logNumber 状态被推进的时机



#### prevLogNumber

prevLogNumber 新版本 leveldb 已经不再使用。仅在 db 启动的时候关注，用于处理从历史 leveldb 恢复数据的场景



#### nextFileNumber

fileNumber 用于磁盘文件命名。需要持久化，从而 leveldb 重启后，新写入的文件不会和历史文件冲突



#### lastSequence

sequence 用于对每一个 Key-Value 键值对进行版本管理，Key 相同的情况下，sequence 越大，表明数据版本越新。所以 lastSequence 需要持久化，保证每个 sequence 最多仅被使用一次



#### comparatorName

leveldb 支持用户自定义 comparator 从而控制 Key 的排序规则。

虽然可以自定义 comparator 但并不允许中途变更 comparator。在 leveldb 重启恢复的过程中，会校验当前用户提供的 comparator 和历史使用的 comparator 名称是否一致



#### compactPoint

compactPoint 用于指导哪个文件参与 compaction 过程。

当某一个层级 sstable 文件大小太大的时候 （level 0 根据文件数）会触发 compaction，leveldb 会把每次 compaction 挑选的文件的最大键值保存下来作为下一次此类 compaction 挑选文件的起点。

也可以采用别的挑选机制，比如：

1. 随机挑选
2. 总是选第一个

对比上面列举的挑选机制，leveldb 的做法相当于把每个层级切分成若干个无交集的数据段，一段一段的进行 compaction。具体对比效果有待进一步验证，个人有如下猜测：

待验证：

1. 平均每次 compaction 的范围更大 —— 因为前后参与 compaction 的数据段无交集
2. 每个层次的文件大小更加均匀 —— leveldb 选择策略相当于从头到尾顺序的 compact
3. 上一次 compact 因为各种原因失败，下一次 compaction 文件挑选会选择完全不同的数据段进行



个人看法：compactPoint 不持久化也没关系，重启就从头开始挑选 compaction 文件即可，影响不大



#### deletedFiles && newFiles

每一次 Compaction，会有若干文件作为 compaction 过程的输入，然后会产出若干文件作为合并的结果。

作为输入的文件会被删除，作为输出的文件需要被创建。

deletedFiles 和 newFiles 作为 versionEdit 的内容的一部分，在恢复过程中，按顺序进行推演，可以得到 leveldb 当下磁盘文件的 metadata 信息

