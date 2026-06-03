# Yuanrong KvEventPublisher 阶段 1 详细设计

## 1. 阶段定位

本文档细化 `Yuanrong KvEventPublisher 需求设计文档` 中的阶段 1：publisher 基础能力。

阶段 1 只实现可独立编译、可独立单测的 publisher 基础设施，不接入 Yuanrong 的 Set/Delete/eviction 生命周期。生命周期接入在后续阶段完成。

阶段 1 目标：

1. 新增 worker object-cache 下的 `kv_event` 模块。
2. 支持配置构造、初始化、停止、状态统计。
3. 支持 key 解析：从 Yuanrong namespace URI 中提取 tenant 和真实 object key，并解析 rolling `seq_hash`。
4. 支持异步有界入队：`PublishStored()`、`PublishRemoved()`。
5. 支持后台线程批量编码 RFC #1527 风格事件，并通过 ZMQ PUB 发送。
6. 支持 disabled no-op、队列满 drop、非法 key skip、send 失败计数。
7. 提供单元测试覆盖 key parser、payload encoding、队列行为、disabled publisher。

阶段 1 非目标：

- 不修改 `WorkerOcServicePublishImpl`、`WorkerOcServiceCrudCommonApi`、`WorkerOcEvictionManager` 等业务生命周期代码。
- 不改变 KVClient/ObjectClient 行为。
- 不实现 replay endpoint。
- 不实现启动时全量对象扫描。
- 不新增 HTTP 管理接口。
- 不引入新的第三方依赖。

## 2. 性能原则

KvEventPublisher 是观测能力，不是主业务正确性的必要条件。阶段 1 必须遵守以下性能原则：

1. 阶段 1 直接参考 Mooncake PR #2214：业务线程只做 enabled 判断、构造 `PendingEvent`、有界队列入队。
2. 主流程不允许做 ZMQ send、MessagePack 编码、等待 ZMQ consumer、等待队列空间。
3. 队列满时直接 drop event，并通过 `GetStats()` 暴露。
4. `Enqueue()` 使用短临界区 `queue_mutex_` 保护 `std::deque`。锁竞争时业务线程可能短暂等待，这一点与 Mooncake PR #2214 保持一致。
5. publisher 的可靠性目标是 best-effort；阶段 1 优先保持实现与 Mooncake 对齐，不额外设计 Mooncake 之外的队列机制。

## 3. 设计输入

参考来源：

- Mooncake PR #2214 中 `KvEventPublisher` 的事件格式。
- Yuanrong 当前仓库 `/Users/kuan/Desktop/yuanrong-datasystem`。

相关现有代码：

| 领域 | 文件 | 结论 |
| --- | --- | --- |
| worker object-cache 构建 | `src/datasystem/worker/object_cache/CMakeLists.txt` | 可将新源文件加入 `WORKER_OC_SRCS`，依赖加入 `WORKER_OBJECT_CACHE_DEPEND_LIBS` |
| worker 总构建 | `src/datasystem/worker/CMakeLists.txt` | worker 已依赖 `worker_object_cache` 和 `common_rpc_zmq` |
| ZMQ 依赖 | `cmake/external_libs/libzmq.cmake` | ZeroMQ 已是项目现有第三方依赖 |
| ZMQ 封装 | `src/datasystem/common/rpc/zmq/zmq_context.h`、`zmq_socket_ref.h`、`zmq_message.h` | 可复用 `ZmqContext`、`ZmqSocketRef`、`ZmqMessage`，但需要扩展 `ZmqSocketType::PUB` |
| 配置 | `src/datasystem/common/util/gflag/common_gflag_define.cpp`、`common_gflags.h`、`common_gflags_validate.cpp` | 只新增一个 JSON 字符串配置；空字符串表示关闭 publisher |
| tenant key | `src/datasystem/common/iam/tenant_auth_manager.cpp` | 可复用 `ExtractTenantId()` 和 `ExtractRealObjectKey()` |
| 线程封装 | `src/datasystem/common/util/thread.h` | 后台线程使用 `datasystem::Thread` 并设置线程名 |
| 队列 | `src/datasystem/common/util/queue/blocking_queue.h` | 现有 `BlockingQueue` 没有容量上限，阶段 1 不直接复用 |
| MessagePack | `nlohmann_json` CMake/Bazel 依赖 | 可用 `nlohmann::json::to_msgpack()`，避免新增 `msgpack-c` |

Yuanrong 需要做的适配点：

- key parser 从 Yuanrong namespace URI 提取 tenant 和真实 object key。
- ZMQ 使用 Yuanrong 现有 `ZmqContext`、`ZmqSocketRef`、`ZmqMessage` 封装。
- 线程使用 `datasystem::Thread`。
- 配置使用 Yuanrong gflags 体系。
- `Enqueue()` 使用 `queue_mutex_ + std::deque + condition_variable`，队列满直接 drop，不等待队列空间。

## 4. 文件布局

新增生产代码：

```text
src/datasystem/worker/object_cache/kv_event/
  kv_event_config.h
  kv_event_publisher.h
  kv_event_publisher.cpp
```

新增测试代码：

```text
tests/ut/worker/object_cache/kv_event_publisher_test.cpp
```

需要更新的构建文件：

```text
src/datasystem/worker/object_cache/CMakeLists.txt
src/datasystem/worker/object_cache/BUILD.bazel
tests/ut/worker/BUILD.bazel
```

如果测试源码由 `tests/ut/CMakeLists.txt` 递归收集，则不需要单独改 CMake 测试列表；仍需确认新增测试会进入 `DS_UT_OBJECT_SRCS`。

## 5. 模块边界

### 5.1 `KvEventConfig`

定义在 `kv_event_config.h`：

```cpp
namespace datasystem::object_cache {

struct KvEventConfig {
    // Whether to enable KV event publishing. Disabled publisher is a no-op.
    bool enabled{false};

    // ZMQ PUB bind endpoint, for example "tcp://0.0.0.0:5557".
    // Required when enabled is true.
    std::string bindEndpoint;

    // Model name in the event envelope. Empty string is encoded as null.
    std::string modelName;

    // Backend identity in the event envelope. Phase 1 reads it from config;
    // later worker integration may default it from worker address.
    std::string backendId;

    // Tenant id used when object key does not carry a tenant.
    std::string tenantId{"default"};

    // Hash namespace salt in the event envelope. Empty string is encoded as null.
    std::string additionalSalt;

    // LoRA name in the event envelope. Empty string is encoded as null.
    std::string loraName;

    // KV block size in the event envelope. 0 is encoded as null.
    uint32_t blockSize{0};

    // Data parallel rank in the event envelope and payload wrapper.
    uint32_t dpRank{0};

    // Whether to emit vLLM/SGLang legacy compatibility fields:
    // type, block_hashes, parent_block_hash.
    // This only affects extra event fields and does not change the ZMQ protocol.
    bool emitLegacyCompatFields{true};

    // Bounded pending-event queue capacity. Additional events are dropped.
    uint32_t queueCapacity{65536};
};

}
```

设计说明：

- 使用 Yuanrong C++ 命名风格，字段用 lowerCamelCase。
- `backendId` 在阶段 1 只从配置传入。后续 worker 接入时可由 `worker_address` 兜底。
- `blockSize=0` 时，payload 中 `block_size` 编码为 `null`。
- `emitLegacyCompatFields` 默认 `true`，与 Mooncake PR #2214 保持一致。
- `emitLegacyCompatFields=true` 时额外输出 legacy 兼容字段：`type`、`block_hashes`、`parent_block_hash`。
- `emitLegacyCompatFields=false` 时只输出标准字段，不输出上述 legacy 字段。
- `emitLegacyCompatFields` 只影响 event object 中的附加字段，不影响 publisher 是否启动、不影响内部队列、不影响 ZMQ 三帧协议。
- batch size 使用 cpp 内部常量 `kMaxBatchSize`，与 Mooncake 保持一致，阶段 1 不暴露到 `KvEventConfig`。
- ZMQ SNDHWM 使用 ZMQ 默认行为，阶段 1 不暴露到 `KvEventConfig`。
- shutdown drain timeout 不作为配置项，阶段 1 按 Mooncake 风格在析构中尽力 drain 已入队事件。

### 5.2 `KvEventPublisher`

定义在 `kv_event_publisher.h`：

```cpp
class KvEventPublisher {
public:
    explicit KvEventPublisher(KvEventConfig config);
    ~KvEventPublisher();

    KvEventPublisher(const KvEventPublisher &) = delete;
    KvEventPublisher &operator=(const KvEventPublisher &) = delete;

    bool Enabled() const;

    void PublishStored(const std::string &objectKey, const std::string &medium);
    void PublishRemoved(const std::string &objectKey, const std::string &medium);

    struct Stats {
        uint64_t publishedBatches{0};
        uint64_t publishedEvents{0};
        uint64_t droppedEvents{0};
        uint64_t skippedUnparsedKeys{0};
    };
    Stats GetStats() const;

    struct ParsedKey {
        std::string tenantId;
        std::string realObjectKey;
        uint64_t seqHash{0};
    };
    static std::optional<ParsedKey> ParseKey(const std::string &objectKey,
                                             const std::string &tenantId);

private:
    enum class EventKind { STORED, REMOVED };
    struct PendingEvent {
        EventKind kind;
        std::string objectKey;
        std::string medium;
    };

    void Enqueue(PendingEvent event);
    void WorkerLoop();
    void PublishBatch(const std::vector<PendingEvent> &batch);
};
```

接口约束：

- 构造函数负责 ZMQ 初始化、bind 和后台线程启动。
- 构造失败时记录日志并将 publisher 保持 disabled，不向业务路径返回错误。
- 析构函数负责停止后台线程并释放 ZMQ 资源。
- `PublishStored/PublishRemoved` 是 fire-and-forget，不返回业务错误。
- `Enqueue()` 对齐 Mooncake 的私有方法命名和阶段 1 语义：持有短临界区队列锁，队列满直接 drop 并更新 stats。
- `Enabled()` 返回内部最终配置状态；初始化失败时返回 false。

### 5.3 内部有界队列

不复用现有 `BlockingQueue`，因为它没有容量限制，也没有适合 drain 的批量 pop 接口。阶段 1 直接参考 Mooncake PR #2214，在 `KvEventPublisher` 内部实现私有有界队列。

推荐结构：

```cpp
mutable std::mutex queueMutex_;
std::deque<PendingEvent> queue_;
std::condition_variable queueCv_;
std::atomic<bool> stop_{false};
```

入队规则：

- `PublishStored/PublishRemoved` 只做 enabled/accepting 判断并构造 `PendingEvent`，然后调用私有 `Enqueue()`。
- `Enqueue()` 使用 `std::lock_guard<std::mutex>` 持有 `queueMutex_`。
- 持锁后检查 `queue_.size() >= queueCapacity`。
- 队列满时直接丢弃新事件，`droppedEvents` 加一，不等待队列空间。
- 队列未满时 `queue_.push_back(std::move(event))`。
- 入队成功后释放锁，再调用 `queueCv_.notify_one()`。
- 不在业务线程做 key parser、MessagePack 编码、ZMQ send。
- 不在业务线程输出逐条日志；drop 只更新原子计数，通过 `GetStats()` 查询。

出队规则：

- 后台线程使用同一个 `queueMutex_` 和 `queueCv_` 等待队列非空。
- 后台线程每轮最多从 `queue_` 中 `pop_front()` 取内部常量 `kMaxBatchSize` 个事件。
- shutdown 时参考 Mooncake 风格尽力 drain 已入队事件，不额外暴露 drain timeout 配置。

这个设计与 Mooncake PR #2214 一致：队列是有界的，队列满时 drop，不等待 ZMQ consumer，也不等待队列空间；但 `Enqueue()` 获取 `queueMutex_` 时可能因为锁竞争短暂等待。

热路径伪代码：

```cpp
void KvEventPublisher::PublishStored(const std::string &objectKey, const std::string &medium)
{
    if (!config_.enabled) {
        return;
    }
    Enqueue(PendingEvent{EventKind::STORED, objectKey, medium});
}

void KvEventPublisher::Enqueue(PendingEvent event)
{
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (queue_.size() >= config_.queueCapacity) {
            droppedEvents_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        queue_.push_back(std::move(event));
    }
    queueCv_.notify_one();
}
```

注意：上面保留 `medium` 为 string 是为了和阶段 2 调用点保持简单。实现时可以进一步改成 `enum class Medium { CPU, DISK }`，减少热路径字符串拷贝。

## 6. 配置设计

阶段 1 只新增一个 gflag：

```cpp
DS_DEFINE_string(kv_events_config, "", "KV event publisher JSON config. Empty means disabled.");
```

在 `common_gflags.h` 中声明这个 flag。

语义：

- `kv_events_config == ""`：不启用 KvEventPublisher，`KvEventConfig.enabled=false`。
- `kv_events_config != ""`：按 JSON 解析配置，`KvEventConfig.enabled=true`。
- JSON 字段使用 Mooncake 配置里的 snake_case 名称，解析后映射到 Yuanrong `KvEventConfig` 的 lowerCamel 字段。

JSON 示例：

```json
{
  "bind_endpoint": "tcp://0.0.0.0:5557",
  "model_name": "llama-3.1-8b",
  "backend_id": "datasystem-worker-10.0.0.1:31501",
  "tenant_id": "default",
  "additional_salt": "",
  "lora_name": "",
  "block_size": 64,
  "dp_rank": 0,
  "emit_legacy_compat_fields": true,
  "queue_capacity": 65536
}
```

字段映射：

| JSON 字段 | `KvEventConfig` 字段 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `bind_endpoint` | `bindEndpoint` | 无 | 非空 JSON 配置中必填 |
| `model_name` | `modelName` | `""` | 空字符串编码为 `null` |
| `backend_id` | `backendId` | 无 | 非空 JSON 配置中必填 |
| `tenant_id` | `tenantId` | `"default"` | 无法从 namespace URI 提取 tenant 时使用 |
| `additional_salt` | `additionalSalt` | `""` | 空字符串编码为 `null` |
| `lora_name` | `loraName` | `""` | 空字符串编码为 `null` |
| `block_size` | `blockSize` | `0` | 0 编码为 `null` |
| `dp_rank` | `dpRank` | `0` | data parallel rank |
| `emit_legacy_compat_fields` | `emitLegacyCompatFields` | `true` | 是否额外输出 vLLM/SGLang legacy 兼容字段 |
| `queue_capacity` | `queueCapacity` | `65536` | 有界队列容量，必须大于 0 |

配置构造函数建议：

```cpp
KvEventConfig BuildKvEventConfigFromJsonString(const std::string &jsonConfig);
```

解析规则：

- 输入为空字符串时返回 disabled config。
- 输入非空但不是合法 JSON 时记录 ERROR，返回 disabled config。
- `bind_endpoint` 或 `backend_id` 缺失/为空时记录 ERROR，返回 disabled config。
- `queue_capacity == 0` 时记录 ERROR，返回 disabled config。
- 未出现的可选字段使用上表默认值。
- 不新增额外字段；阶段 1 只解析上表字段。

## 7. ZMQ 设计

### 7.1 复用现有封装

复用 Yuanrong 现有 ZMQ 封装：

- `ZmqContext`
- `ZmqSocketRef`
- `ZmqMessage`

需要对 `ZmqSocketType` 增加：

```cpp
PUB = ZMQ_PUB
```

原因：

- 避免直接散落 `zmq_ctx_new`、`zmq_socket`、`zmq_msg_close` 等 C API。
- 统一使用 `Status` 返回错误。
- 现有 `ZmqSocketRef::Set(sockopt::ZmqSndhwm, ...)`、`Set(sockopt::ZmqLinger, ...)`、`Bind()`、`SendMsg()` 足够覆盖阶段 1。

### 7.2 初始化顺序

构造函数初始化顺序：

1. 如果 `config.enabled=false`，直接返回，publisher 保持 disabled。
2. 校验 `bindEndpoint` 非空。
3. 校验或确认 `backendId` 非空。阶段 1 不做 `worker_address` 回退。
4. 创建 `ZmqContext`，调用 `Init()`。
5. 创建 `PUB` socket，包装为 `ZmqSocketRef`。
6. 设置：
   - `ZMQ_LINGER = 0`
7. 不额外设置 `ZMQ_SNDHWM`，使用 ZMQ 默认行为。
8. `Bind(config.bindEndpoint)`。
9. 启动后台线程，线程名 `KvEventPub`。

初始化失败策略：

- 记录 ERROR。
- 释放已创建的 ZMQ 资源。
- 将内部 `config_.enabled` 置为 false。
- 不启动后台线程。
- publisher 初始化失败不阻止 worker 启动。

### 7.3 发送三帧

ZMQ 帧格式与总设计文档保持一致：

```text
frame 1: empty topic
frame 2: big-endian uint64 publisher sequence
frame 3: msgpack payload: [timestamp_ms, [events...], dp_rank]
```

发送实现上，frame 1 和 frame 2 调用 `SendMsg` 时需要带 `ZMQ_SNDMORE`，frame 3 不带 `ZMQ_SNDMORE`。`ZMQ_SNDMORE` 是 ZMQ 发送标志，不是 frame 内容的一部分。

`sequence` 使用 helper：

```cpp
static uint64_t HostToBigEndian64(uint64_t value);
```

实现优先使用平台可用的 `htobe64` 或 `__builtin_bswap64`，封装在 cpp 内部，不在业务代码手写字节反转。

发送失败策略：

- 任一帧发送失败，本 batch 计为 send failed。
- 不重试，避免后台线程被慢 consumer 或异常 socket 阻塞。
- 按限频日志输出失败原因。

## 8. MessagePack 编码设计

阶段 1 使用 `nlohmann::json` 构造 payload，再调用 `nlohmann::json::to_msgpack()`。

payload 逻辑结构：

```json
[
  1770000000000,
  [
    {
      "event_id": 1,
      "timestamp": 1770000000000,
      "event_type": "stored",
      "type": "BlockStored",
      "model_name": null,
      "block_size": null,
      "additional_salt": null,
      "lora_name": null,
      "tenant_id": "default",
      "backend_id": "worker-0",
      "medium": "cpu",
      "dp_rank": 0,
      "seq_hashes": [42],
      "block_hashes": [42],
      "base_block_idx": null,
      "parent_hash": null,
      "token_ids": null,
      "parent_block_hash": null
    }
  ],
  0
]
```

编码逻辑直接放在 `KvEventPublisher::PublishBatch()` 中，不新增 public/test-only encoder API，也不额外设计独立 encoder 类。实现时可以在 cpp 内使用匿名 namespace helper 函数降低重复代码。

字段编码规则：

- 空字符串字段编码为 `nullptr`：
  - `model_name`
  - `additional_salt`
  - `lora_name`
- `block_size == 0` 编码为 `nullptr`。
- `dp_rank` 总是编码为数值。
- `seq_hashes` 总是单元素数组。
- legacy 开关关闭时不输出：
  - `type`
  - `block_hashes`
  - `parent_block_hash`

## 9. Key Parser 详细设计

输入：`objectKey`，即 Yuanrong worker object table 中看到的 key。

vLLM Ascend 的 Yuanrong backend 通过 `Backend.put(keys, addrs, sizes)` 写入 Yuanrong。当前路径是：

```text
ChunkedTokenDatabase.process_tokens()
  -> PoolKey / LayerPoolKey
  -> key.to_string()
  -> YuanrongBackend.put()
  -> YuanrongHelper.normalize_keys()
  -> HeteroClient.mset_d2h()
  -> Yuanrong worker tenant namespace
```

因此 publisher 看到的 key 不是直接来自 vLLM 的 `block_hashes`，而是 Yuanrong tenant namespace 包裹后的、可能经过 vLLM Ascend `normalize_keys()` 处理的 object key。

输出：

```cpp
struct ParsedKey {
    std::string tenantId;
    std::string realObjectKey;
    uint64_t seqHash;
};
```

流程：

1. `parsedTenantId = TenantAuthManager::ExtractTenantId(objectKey)`。
2. 如果 `parsedTenantId.empty()`，使用配置中的 `tenantId`。
3. `realObjectKey = TenantAuthManager::ExtractRealObjectKey(objectKey)`。
4. 如果 `realObjectKey` 末尾匹配 `__` + 16 位十六进制字符，先去掉该后缀。这个后缀来自 vLLM Ascend `YuanrongHelper.normalize_keys()`，用于标识被替换或截断过的原始 key。
5. 从 `realObjectKey` 中提取候选 hash 字符串：
   - 如果 `realObjectKey` 本身是纯十进制 `u64` 或 `0x` / `0X` 前缀十六进制 `u64`，候选值就是整个 `realObjectKey`。
   - 如果是 vLLM Ascend `PoolKey.to_string()`，格式为：

     ```text
     <model>@pcp<pcp>@dcp<dcp>@head_or_tp_rank:<rank>@pp_rank:<pp>@group:<group>@cache_role:<role>@cache_family:<family>@<chunk_hash>
     ```

     候选值是最后一个 `@` 后的 `<chunk_hash>`。
   - 如果是 vLLM Ascend `LayerPoolKey.to_string()`，格式为：

     ```text
     <model>@pcp<pcp>@dcp<dcp>@head_or_tp_rank:<rank>@group:<group>@cache_role:<role>@cache_family:<family>@<chunk_hash>@<layer_id>
     ```

     候选值是倒数第二个 `@` 后的 `<chunk_hash>`；最后一个字段是 layer id，不参与 `seq_hash`。
6. 解析候选 hash 字符串：
   - 如果以 `0x` 或 `0X` 开头，用 base 16。
   - 如果候选值来自整个 `realObjectKey` 且不带 `0x`，只按 base 10 解析。
   - 如果候选值来自 vLLM Ascend `PoolKey` / `LayerPoolKey` 的 `<chunk_hash>` 字段，且不带 `0x`、全部是十六进制字符、长度不超过 16，可以按 base 16 解析。这是为了兼容 vLLM Ascend 对 bytes block hash 调用 `h.hex()` 后形成的短 hex 字符串。
   - 使用 `std::stoull` 并检查 `idx == candidate.size()`。
   - 捕获异常，失败返回 `std::nullopt`。

注意：

- Yuanrong 的 `ExtractTenantId()` 对无分隔符 key 会返回 `DEFAULT_TENANT_ID`。阶段 1 不额外改变该行为。
- Yuanrong tenant namespace 分隔符是 `$`，不是 `/`。例如 `tenant-a$12345`。
- vLLM Ascend `normalize_keys()` 不会把原始 key 原样保留下来：非法字符会被替换为 `_`，并追加 `__<sha256(original)[:16]>`；如果 key 超过 Yuanrong 255 字符限制，还会截断前缀。
- 如果截断导致 `<chunk_hash>` 丢失，publisher 无法可靠恢复 `seq_hash`，该事件应跳过并增加 `skippedUnparsedKeys`。
- 如果候选 hash 是超过 64 bit 的长 hex 字符串，阶段 1 不截断、不 hash 二次转换，直接跳过。是否取低 64 位或使用其他转换，需要和 vLLM/Dynamo 的 rolling hash 语义单独对齐。
- `seqHash=0` 不是非法值；只要 key 是合法 u64 就允许。

解析示例：

| 输入 namespace URI | 中间结果 | 期望 |
| --- | --- | --- |
| `12345` | tenant=`default`，candidate=`12345` | `seqHash=12345` |
| `tenant-a$12345` | tenant=`tenant-a`，real=`12345`，candidate=`12345` | `seqHash=12345` |
| `tenant-a$0x2a` | tenant=`tenant-a`，real=`0x2a`，candidate=`0x2a` | `seqHash=42` |
| `tenant-a$llama@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@group:0@cache_role:kv@cache_family:default@0x75bcd15` | vLLM Ascend `PoolKey`，candidate=`0x75bcd15` | `seqHash=123456789` |
| `tenant-a$llama@pcp0@dcp0@head_or_tp_rank:0@group:0@cache_role:kv@cache_family:default@0x75bcd15@17` | vLLM Ascend `LayerPoolKey`，candidate=`0x75bcd15`，layer=`17` | `seqHash=123456789` |
| `tenant-a$Qwen_Qwen3@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@group:0@cache_role:kv@cache_family:default@0x2a__0123456789abcdef` | 先去掉 normalize suffix，candidate=`0x2a` | `seqHash=42` |
| `tenant-a$llama@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@group:0@cache_role:kv@cache_family:default@6830` | candidate=`6830`，无 `0x` 但是短 hex | `seqHash=26672` |
| `tenant-a$not-a-hash` | candidate=`not-a-hash` | parse failed |
| `tenant-a$123abc` | 整个 `realObjectKey` 是候选值，不启用 PoolKey 短 hex 规则 | parse failed |
| 空字符串 | 无 candidate | parse failed |

## 10. 后台线程流程

后台线程是 `KvEventPublisher` 的 worker thread。它的作用是把业务线程已经放入 `queue_` 的轻量 `PendingEvent` 转换成真正的 ZMQ KV event message。

业务线程只负责：

```text
PublishStored/PublishRemoved -> Enqueue(PendingEvent)
```

后台线程负责较重的工作：

```text
queue_ -> parse key -> build event map -> pack msgpack -> send ZMQ frames
```

这样可以避免在 Set/Delete/eviction 等业务路径里做 MessagePack 编码和 ZMQ IO。

主循环：

```text
while not stopping:
  1. wait until queue_ is not empty or stop_ is true
  2. hold queueMutex_ and move up to kMaxBatchSize PendingEvent from queue_ into local batch
  3. release queueMutex_
  4. for each PendingEvent in local batch:
       - parse tenant / real object key / seq_hash
       - skip invalid key and increase skippedUnparsedKeys
       - assign event_id from nextEventId_
       - build stored/removed event fields
  5. if no valid event remains, continue
  6. build frame 3 payload: [timestamp_ms, [events...], dp_rank]
  7. send ZMQ frame 1 empty topic
  8. send ZMQ frame 2 big-endian publisher sequence from nextZmqSequence_
  9. send ZMQ frame 3 msgpack payload
  10. update publishedBatches / publishedEvents

on stopping:
  best-effort drain queued events
```

锁边界：

- `queueMutex_` 只保护 `queue_` 的 pop/push。
- key parser、event 构造、MessagePack 编码、ZMQ send 都在释放 `queueMutex_` 后执行。
- 因此后台线程不会在持有队列锁时做网络 IO 或编码。

事件 ID 分配：

- 只给成功解析的 event 分配 `event_id`。
- 非法 key 被 skip 时不消耗 `event_id`。
- `nextEventId_` 初始为 1。
- `nextZmqSequence_` 初始为 1。

批次统计：

- `publishedEvents` 在三帧发送成功后增加本 batch event 数。
- `publishedBatches` 在三帧发送成功后加一。
- `droppedEvents` 在队列满 drop 时增加。
- `skippedUnparsedKeys` 在 key 解析失败时增加。

## 11. 状态与错误处理

### disabled

`config.enabled=false`：

- `Enabled()` 返回 false。
- `PublishStored/PublishRemoved` 直接返回。
- `GetStats()` 可返回全 0。

### 初始化失败

初始化失败包括：

- bind endpoint 为空。
- backend id 为空。
- ZMQ context 初始化失败。
- ZMQ socket 创建失败。
- 设置 socket option 失败。
- bind 失败。

处理：

- 记录 ERROR。
- 已创建资源必须释放。
- `config_.enabled=false`。
- 不启动后台线程。
- `Enabled()` 返回 false。

### 入队失败

- 队列满：drop 新事件，`droppedEvents` 加一。
- publisher disabled 或 stopping：直接忽略，不计 dropped。
- 入队失败不返回业务错误，不输出逐条 ERROR 日志。

### 解析失败

- `skippedUnparsedKeys` 加一。
- 按限频日志输出，不输出完整大量 key；建议只输出截断后的 key。

### 编码失败

`nlohmann::json::to_msgpack()` 理论上不应因正常字段失败，但仍需 catch `std::exception`：

- 记录限频 ERROR。
- 丢弃本 batch，后续 batch 继续处理。
- 不额外增加 Mooncake 未定义的 Stats 字段。

### 发送失败

- 本 batch 不重试。
- 记录限频 ERROR。
- 后续 batch 继续发送。

## 12. 资源生命周期

成员建议：

```cpp
KvEventConfig config_;
std::unique_ptr<ZmqContext> zmqContext_;
ZmqSocketRef zmqSocket_;
Thread worker_;

mutable std::mutex queueMutex_;
std::deque<PendingEvent> queue_;
std::condition_variable queueCv_;

std::atomic<bool> stop_{false};
std::atomic<uint64_t> nextEventId_{1};
std::atomic<uint64_t> nextZmqSequence_{1};
```

生命周期：

1. 构造：保存 config；如果 enabled，则初始化 ZMQ 并启动后台线程。
2. 初始化失败：记录错误、释放资源、将 publisher 置为 disabled。
3. 运行：请求线程只入队，后台线程负责编码发送。
4. 析构：
   - `stop_=true`
   - `queueCv_.notify_all()`
   - join worker thread
   - close socket
   - close context

线程安全要求：

- `Publish*()` 可与析构并发的场景由 worker 生命周期保证不发生；后续接入时应先停止业务入口，再析构 publisher。
- `Publish*()` 通过 `config_.enabled` 判断是否入队，并通过 `queueMutex_` 保护队列状态。
- `Publish*()` 不等待队列空间，不调用 ZMQ，不做 MessagePack 编码；但与 Mooncake 一致，获取 `queueMutex_` 时可能短暂等待。
- 只有后台线程访问 ZMQ socket。

## 13. 构建集成

### CMake

`src/datasystem/worker/object_cache/CMakeLists.txt`：

- `WORKER_OC_SRCS` 增加：

```text
kv_event/kv_event_publisher.cpp
```

- `WORKER_OBJECT_CACHE_DEPEND_LIBS` 增加：

```text
nlohmann_json::nlohmann_json
common_rpc_zmq
```

`common_rpc_zmq` 已存在于依赖中，仍需确认新增文件可以包含 `zmq_context.h`、`zmq_socket_ref.h`、`zmq_message.h`。

### Bazel

`src/datasystem/worker/object_cache/BUILD.bazel`：

- 新增或扩展 `worker_object_cache` 相关 target 的 srcs/hdrs。
- deps 增加：

```text
"@nlohmann_json//:json"
"//src/datasystem/common/rpc/zmq:zmq_socket"
```

具体 ZMQ dep 以现有 target 分层为准；如果 `common_rpc_zmq` 在 Bazel 中已有聚合 target，应优先依赖聚合 target。

`tests/ut/worker/BUILD.bazel`：

- 增加 `kv_event_publisher_test.cpp`。
- deps 增加 `@nlohmann_json//:json`，用于 round-trip 解码。

## 14. 单元测试详细设计

测试文件：`tests/ut/worker/object_cache/kv_event_publisher_test.cpp`

### 14.1 Key parser

覆盖：

- decimal key。
- hex key。
- Yuanrong `$` tenant namespace。
- vLLM Ascend `PoolKey.to_string()`。
- vLLM Ascend `LayerPoolKey.to_string()`。
- vLLM Ascend `normalize_keys()` 后缀 `__<16 hex>`。
- invalid key。
- empty key。
- overflow key。

### 14.2 Disabled no-op

构造：

```cpp
KvEventConfig config;
config.enabled = false;
KvEventPublisher publisher(config);
publisher.PublishStored("42", "cpu");
publisher.PublishRemoved("42", "cpu");
EXPECT_FALSE(publisher.Enabled());
EXPECT_EQ(publisher.GetStats().droppedEvents, 0);
```

### 14.3 Queue capacity

阶段 1 不引入 fake sender 或 test-only enqueue API

推荐：

- 使用 `bindEndpoint="inproc://..."` 或 `tcp://127.0.0.1:<free_port>` 启动真实 ZMQ PUB。
- `queueCapacity=1`。
- disabled publisher 下验证 publish 不入队。
- 增加容量测试，验证队列满后新事件直接 drop，`droppedEvents` 增加。

### 14.4 Publish 热路径边界检查

验证 `PublishStored/PublishRemoved` 这两个业务线程会调用的方法足够轻量。

要防止的错误实现：

- 在 `PublishStored()` 里直接解析 key。
- 在 `PublishStored()` 里直接构造 JSON/MessagePack。
- 在 `PublishStored()` 里直接调用 ZMQ send。
- 队列满时等待后台线程消费，而不是直接 drop。

阶段 1 可以用轻量 UT、mock wrapper 或 benchmark 做边界检查。测试目标是确认 publish 路径只负责判断开关和入队，重工作仍在 `WorkerLoop()/PublishBatch()` 中执行。

验证点：

- disabled publisher 的 `PublishStored()` 只检查 enabled 后返回。
- enabled publisher 的 `PublishStored()` 在队列未满时只做短临界区入队和 notify。
- 队列满时直接 drop。
- 不在 publish 路径调用 MessagePack 编码或 `ZmqSocketRef::SendMsg()`。

### 14.5 Payload encoding round-trip

阶段 1 不单独设计 `KvEventEncoder` 类，编码逻辑直接放在 `KvEventPublisher::PublishBatch()` 中，与 Mooncake 保持一致。测试用 `nlohmann::json::from_msgpack()` 解码收到的 payload，断言：

- payload 是 3 元素 array。
- `event_type=stored/removed`。
- `seq_hashes` 为单元素数组。
- `block_size=0` 时为 null。
- legacy 开关打开/关闭字段差异正确。

### 14.6 ZMQ wire smoke test

可选单测或后续集成测试：

- publisher bind `tcp://127.0.0.1:<free_port>`。
- 测试中创建 SUB socket 连接。
- 等待订阅建立后 publish 一个合法事件。
- SUB 接收三帧并解码 payload。

这个测试可能受 PUB/SUB slow joiner 影响，不作为阶段 1 必选单元测试。阶段 1 必选的是 encoder round-trip。

## 15. 推荐类拆分

为避免 `KvEventPublisher` 过大，阶段 1 推荐拆成：

| 类 | 文件 | 职责 |
| --- | --- | --- |
| `KvEventConfig` | `kv_event_config.h` | 配置结构 |
| `KvEventPublisher` | `kv_event_publisher.h/cpp` | 队列、线程、ZMQ 生命周期 |

不建议阶段 1 引入过多抽象：

- 不单独引入 plugin-style sender。
- 不新增通用 bounded queue 到 `common/util/queue`，除非后续多个模块复用。
- 不把 key parser 放到 common 层，先保持 worker object-cache 私有。

## 16. 与后续阶段的接口预留

阶段 2/3 接生命周期时，只应使用以下 public API：

```cpp
publisher->PublishStored(objectKey, "cpu");
publisher->PublishRemoved(objectKey, "cpu");
publisher->PublishStored(objectKey, "disk");
publisher->PublishRemoved(objectKey, "disk");
```

为了后续避免在各业务路径判断 publisher 是否为空，可提供一个轻量 helper：

```cpp
inline void PublishKvStored(KvEventPublisher *publisher,
                            const std::string &objectKey,
                            const std::string &medium)
{
    if (publisher != nullptr) {
        publisher->PublishStored(objectKey, medium);
    }
}
```

但这个 helper 可以留到阶段 2，再结合 `WorkerOCServiceImpl` 的持有方式设计。

## 17. 验收标准

阶段 1 完成后应满足：

1. 新模块可以被 worker object-cache target 编译。
2. `enable=false` 时 publisher 为 no-op。
3. 构造函数能成功 bind 一个合法 ZMQ PUB endpoint。
4. 构造函数对空 endpoint、空 backend id、bind 失败记录错误并将 publisher 置为 disabled。
5. `PublishStored/PublishRemoved` 在 enabled 状态只做有界队列入队，不做编码和 ZMQ send。
6. 后台线程能从队列取事件、解析 key、编码 payload、发送三帧。
7. key parser 单测通过。
8. payload round-trip 单测通过。
9. queue drop / skipped key stats 可被单测验证。
10. 析构时不泄漏线程和 ZMQ socket。
11. 队列满时 publish 直接 drop，不等待队列空间、后台线程和 consumer。
12. 单测或 benchmark 能证明 publish 热路径没有调用 MessagePack 编码和 ZMQ send。

## 18. 明确推迟到后续阶段的问题

- `KvEventPublisher` 由哪个 worker 对象持有。
- `backend_id` 是否默认取 `worker_address`。
- 业务生命周期调用点和锁内/锁外调用位置。
- `stored(cpu)`、`removed(cpu)`、`stored(disk)`、`removed(disk)` 的完整触发矩阵实现。
- ZMQ SUB 集成测试是否放到 UT、ST 或单独测试 target。
