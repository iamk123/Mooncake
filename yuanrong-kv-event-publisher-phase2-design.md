# Yuanrong KvEventPublisher 阶段 2 详细设计

## 1. 阶段定位

本文档细化 `yuanrong-kv-event-publisher-design.md` 中的阶段 2：`stored(cpu)` 和 `removed(cpu)` 生命周期接入。

阶段 2 的前置条件是阶段 1 已完成：

- `KvEventPublisher` 已能按 Mooncake PR #2214 的模型工作。
- publisher 已由 worker 启动时创建，并在 worker 退出时析构。
- public API 只使用：
  - `PublishStored(namespaceUri, medium)`
  - `PublishRemoved(namespaceUri, medium)`
  - `GetStats()`

阶段 2 只接入 worker 本地内存介质，也就是 `medium="cpu"`。

阶段 2 目标：

1. Set/Put 成功后发布 `stored(cpu)`。
2. 对象从 worker object table 删除成功后发布 `removed(cpu)`。
3. eviction 删除或释放内存成功后发布 `removed(cpu)`。
4. 不接入 disk/L2/spill 生命周期。
5. 不额外设计 Mooncake 之外的新 publisher、事件总线、队列、metrics endpoint 或 replay 机制。

阶段 2 非目标：

- 不发布 `stored(disk)` / `removed(disk)`。
- 不修改 MessagePack/ZMQ wire format。
- 不改变 Set/Delete/eviction 的原有返回语义。
- 不为了保证事件发布成功而回滚业务操作。
- 不在业务路径做编码或 ZMQ send。
- 不新增新的全局单例或额外异步队列。

## 2. 基本原则

阶段 2 的接入原则：

1. 只在状态变化已经成功后调用 publisher。
2. publisher 调用失败或事件被 drop 不影响原业务请求。
3. 只调用阶段 1 已有 `KvEventPublisher` API。
4. 调用点只做轻量 `PublishStored/PublishRemoved` 入队，不做 key parse、MessagePack 编码或 ZMQ send。
5. 阶段 2 不改变 Yuanrong 原有锁顺序和错误处理路径。
6. 尽量在函数即将返回 `Status::OK()` 前发布，避免业务后续失败但事件已发出。

## 3. Publisher 传递方式

阶段 2 需要让现有 worker object-cache 组件拿到阶段 1 创建的 `KvEventPublisher`。

推荐复用 Yuanrong 当前依赖传递方式：

- `WorkerOCServiceImpl` 持有阶段 1 创建的 `std::shared_ptr<KvEventPublisher>`。
- `WorkerOcServiceCrudParam` 增加一个 publisher 指针字段。
- `WorkerOcServiceCrudCommonApi` 保存该指针，供 `PublishObject()` / `ClearObject()` 所在继承类直接使用。
- `WorkerOcEvictionManager` 通过已有 setter 风格增加 `SetKvEventPublisher()`。

不引入：

- 全局 singleton。
- 事件 dispatcher。
- 新 thread pool。
- 新队列。

建议字段：

```cpp
std::shared_ptr<KvEventPublisher> kvEventPublisher;
```

在 `WorkerOcServiceCrudCommonApi` 中保存为：

```cpp
std::shared_ptr<KvEventPublisher> kvEventPublisher_{nullptr};
```

在 `WorkerOcEvictionManager` 中保存为：

```cpp
std::weak_ptr<KvEventPublisher> kvEventPublisher_;
```

使用 `weak_ptr` 的原因是 eviction manager 生命周期由现有 worker 管理，避免因反向引用延长 publisher 生命周期。

调用时只做空指针判断：

```cpp
if (kvEventPublisher_ != nullptr) {
    kvEventPublisher_->PublishStored(objectKey, "cpu");
}
```

`WorkerOcEvictionManager` 中：

```cpp
if (auto publisher = kvEventPublisher_.lock()) {
    publisher->PublishRemoved(objectKey, "cpu");
}
```

## 4. 触发点总览

阶段 2 只接 cpu 介质相关路径：

| 场景 | 代码位置 | 事件 | 发布时机 |
| --- | --- | --- | --- |
| Set/Put 后本 worker 内存可读 | `WorkerOcServicePublishImpl::PublishObject()` | `stored(cpu)` | 所有可能失败的 publish 收尾逻辑完成后，返回 OK 前 |
| MultiPublish 后本 worker 内存可读 | `WorkerOcServiceMultiPublishImpl::UpdateObjectAfterCreatingMeta()` | `stored(cpu)` | 单个 object 状态设置为 `CacheInvalid=false` 且收尾处理完成后 |
| Clear/Delete 删除 object table 条目 | `WorkerOcServiceCrudCommonApi::ClearObject()` | `removed(cpu)` | `objectTable_->Erase()` 成功后 |
| Eviction 删除或释放内存 | `WorkerOcEvictionManager::EvictObject()` | `removed(cpu)` | `Action::DELETE` erase 成功后；`Action::FREE_MEMORY` free 成功后 |

阶段 2 不接：

- `SaveBinaryObjectToPersistence()`。
- `AsyncSendManager::AfterSendToRemote()`。
- `WorkerOcEvictionManager::SpillImpl()`。
- `DeleteObjectFromDisk()`。
- `DeleteL2CacheEvictableObject()`。
- metadata recovery / migrate 路径。

这些属于阶段 3/4。

## 5. Set/Put：发布 `stored(cpu)`

### 5.1 单对象 PublishObject

目标函数：

```text
src/datasystem/worker/object_cache/service/worker_oc_service_publish_impl.cpp
WorkerOcServicePublishImpl::PublishObject()
```

当前关键流程：

```text
RequestingToMaster()
SaveBinaryObjectToMemory()
SaveBinaryObjectToPersistence()  // write-through 场景
NotifyPendingGetRequest()
SetLifeState()
SetPrimaryCopy(true)
SetCacheInvalid(false)
SetIncompleted(false)
DeleteObjectFromDisk()           // 如果之前 spilled
evictionManager_->Add(objectKey)
return OK
```

`stored(cpu)` 应在函数最终成功路径发布。推荐放在：

```cpp
evictionManager_->Add(objectKey);
if (kvEventPublisher_ != nullptr) {
    kvEventPublisher_->PublishStored(objectKey, "cpu");
}
return Status::OK();
```

原因：

- `SetCacheInvalid(false)` 表示本地内存副本可读。
- 但 `SetCacheInvalid(false)` 后还有 `DeleteObjectFromDisk()` 可能失败。
- 如果在 `DeleteObjectFromDisk()` 前发布，可能出现业务返回失败但 indexer 已收到 `stored(cpu)`。
- 因此应在所有可能导致 `PublishObject()` 返回错误的收尾操作之后发布。

不在以下位置发布：

- 不在 `PrepareForPublish()` 发布，此时对象还未成功发布。
- 不在 `RequestingToMaster()` 成功后发布，此时内存数据和本地状态还未完全更新。
- 不在 `SaveBinaryObjectToMemory()` 后立即发布，因为后续 write-through L2 或状态更新仍可能失败。
- 不在 `SetCacheInvalid(false)` 立刻发布，因为后续 spill 文件清理可能失败。

`medium` 固定为：

```cpp
"cpu"
```

### 5.2 MultiPublishObject

Yuanrong 当前还有 multi publish 路径，会在成功创建 metadata 后批量更新本地 object 状态：

```text
src/datasystem/worker/object_cache/service/worker_oc_service_multi_publish_impl.cpp
WorkerOcServiceMultiPublishImpl::UpdateObjectAfterCreatingMeta()
```

当前关键流程：

```text
for each key:
  SetCreateTime()
  SetTtlSecond()
  SaveBinaryObjectToPersistence()     // write-through 场景，失败当前只记录/rollback persistence
  asyncSendManager_->Add()            // write-back 场景
  SetNeedToDelete(false)
  SetLifeState(OBJECT_PUBLISHED)
  SetPrimaryCopy(true)
  SetCacheInvalid(false)
  SetIncompleted(false)
  DeleteObjectFromDisk()              // 如果之前 spilled，当前路径 LOG_IF_ERROR
async notify pending get request
```

如果 multi publish 是对外 Set/Put 成功路径的一部分，阶段 2 应在该函数中对每个本地状态已成功更新的 key 发布 `stored(cpu)`。推荐放在每个对象状态更新和可选 `DeleteObjectFromDisk()` 调用之后：

```cpp
if (kvEventPublisher_ != nullptr) {
    kvEventPublisher_->PublishStored(keys[i], "cpu");
}
```

说明：

- 这不是新增机制，只是覆盖 Yuanrong 已有的另一个 Set/Put 成功路径。
- 不在异步 notify pending get request 的 thread pool 中发布，避免事件发布时间被异步通知调度影响。
- `DeleteObjectFromDisk()` 在当前 multi publish 路径中使用 `LOG_IF_ERROR`，不会改变函数返回；因此 `stored(cpu)` 可在该调用之后发布。
- 阶段 2 仍不发布 `removed(disk)`，即使这里删除了 spill 文件。

## 6. Clear/Delete：发布 `removed(cpu)`

目标函数：

```text
src/datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.cpp
WorkerOcServiceCrudCommonApi::ClearObject()
```

当前关键流程：

```text
check caller holds object write lock
if entry exists:
  if spilled: DeleteObjectFromDisk()
  if write-back L2: asyncSendManager_->Remove()
objectTable_->Erase(objectKey, entry)
METRIC_INC(WORKER_OBJECT_ERASE_TOTAL)
evictionManager_->Erase(objectKey)
return OK
```

推荐在 `objectTable_->Erase()` 成功后发布 `removed(cpu)`：

```cpp
const bool hadCpuCopy = entry.Get() != nullptr && !entry->stateInfo.IsCacheInvalid();

RETURN_IF_NOT_OK_APPEND_MSG(objectTable_->Erase(objectKey, entry),
                            FormatString("Failed to erase object %s from object table", objectKey));
METRIC_INC(metrics::KvMetricId::WORKER_OBJECT_ERASE_TOTAL);
evictionManager_->Erase(objectKey);
if (hadCpuCopy && kvEventPublisher_ != nullptr) {
    kvEventPublisher_->PublishRemoved(objectKey, "cpu");
}
return Status::OK();
```

`hadCpuCopy` 在 erase 前计算，因为 erase 后 entry 状态不应再作为判断依据。

判断建议：

```cpp
entry.Get() != nullptr && !entry->stateInfo.IsCacheInvalid()
```

说明：

- `entry.Get() == nullptr` 表示没有实际对象条目，不发布。
- `CacheInvalid=true` 表示当前 worker 内存副本不可读，不发布 `removed(cpu)`。
- 阶段 2 不在这里发布 `removed(disk)`，即使 `ClearObject()` 中删除了 spill 文件。disk 事件留到阶段 3。

发布时机选择：

- 必须在 `objectTable_->Erase()` 成功之后。
- 如果 `DeleteObjectFromDisk()` 失败导致函数提前返回，不发布。
- 如果 `objectTable_->Erase()` 失败，不发布。

## 7. Eviction：发布 `removed(cpu)`

目标函数：

```text
src/datasystem/worker/object_cache/worker_oc_eviction_manager.cpp
WorkerOcEvictionManager::EvictObject()
```

阶段 2 只处理两个 action：

- `Action::DELETE`
- `Action::FREE_MEMORY`

### 7.1 `Action::DELETE`

当前关键流程：

```text
memEvictionList_.Erase(objectKey)
version = entry.Get()->GetCreateTime()
objectTable_->Erase(objectKey, entry)
SubmitAsyncMasterTask(...) 或记录 deletedObjects
return OK
```

推荐发布点：

```cpp
const bool hadCpuCopy = entry.Get() != nullptr && !entry->stateInfo.IsCacheInvalid();
RETURN_IF_NOT_OK(objectTable_->Erase(objectKey, entry));
...
if (hadCpuCopy) {
    PublishRemovedCpu(objectKey);
}
```

发布应在 `objectTable_->Erase()` 成功后。`SubmitAsyncMasterTask()` 是 master metadata 清理，不代表本 worker 内存副本是否仍存在；本地 erase 成功后即可发布 `removed(cpu)`。

### 7.2 `Action::FREE_MEMORY`

当前关键流程：

```text
entry->FreeResources()
return OK
```

推荐发布点：

```cpp
const bool hadCpuCopy = entry.Get() != nullptr && !entry->stateInfo.IsCacheInvalid();
RETURN_IF_NOT_OK(entry->FreeResources());
if (hadCpuCopy) {
    PublishRemovedCpu(objectKey);
}
```

说明：

- `FREE_MEMORY` 不一定删除 metadata，但本地内存资源已释放。
- 对 indexer 来说，本 worker 的 `cpu` tier 已不可用，因此发布 `removed(cpu)`。
- 不发布 `removed(disk)`，即使对象后续仍可从 L2 或 spill 恢复。

### 7.3 不处理的 action

阶段 2 不在这些 action 中新增事件：

| Action | 原因 |
| --- | --- |
| `SPILL` | 会涉及 `stored(disk)`，属于阶段 3 |
| `MIGRATE` | 迁移源/目标事件属于阶段 4 |
| `END_LIFE` | 可能涉及 disk/L2 删除，阶段 3 再统一处理 |
| `RETAIN` / `UNKNOWN` | 不表示 cpu tier 状态成功变化 |

## 8. 锁与调用位置

阶段 2 不改变现有锁策略。

需要注意：

- `ClearObject()` 当前要求 caller 已持有对象写锁。
- `PublishObject()` 也在上层 `PublishObjectWithLock()` 中持有对象写锁。
- `EvictObject()` 调用时对象也处于 eviction 路径控制下。

阶段 2 调用 `PublishStored/PublishRemoved` 只做轻量入队，不做编码和 ZMQ IO。为了与阶段 1/Mooncake 设计一致，可以在现有锁内调用；但必须避免在这些位置做任何 payload 编码或 socket send。

如果后续压测发现队列锁影响尾延迟，再作为独立优化处理；阶段 2 不额外设计队列替换方案。

## 9. 重复与漏发控制

阶段 2 使用“成功路径单点发布”降低重复：

| 事件 | 发布单点 |
| --- | --- |
| `stored(cpu)` | `PublishObject()` 成功返回前 |
| `stored(cpu)` by multi publish | `UpdateObjectAfterCreatingMeta()` 中单 key 状态更新完成后 |
| `removed(cpu)` by clear/delete | `ClearObject()` erase 成功后 |
| `removed(cpu)` by eviction | `EvictObject(Action::DELETE/FREE_MEMORY)` 成功后 |

注意事项：

- 不在 `DeleteAllCopyWithLock()` / `DeleteObjectFromNotification()` 中重复发布，因为这些路径会调用 `ClearObject()`。
- 不在 `WorkerOcServiceClearDataFlow::ClearObject()` 中重复发布，因为它也会走 delete proc 的 `ClearObject()`。
- 不在 `WorkerOcServicePublishImpl::PublishObjectWithLock()` 中重复发布，因为它会调用 `PublishObject()`。
- 不在 `WorkerOcServiceMultiPublishImpl::SendToMasterAndUpdateObject()` 中重复发布，因为它会调用 `UpdateObjectAfterCreatingMeta()`。
- 不在 `evictionManager_->Erase()` 中发布，它只是 eviction list 操作，不等价于 cpu 副本生命周期变化。

阶段 2 不解决启动前已有对象的 snapshot/replay 问题。

## 10. 测试设计

### 10.1 单元测试

建议新增或扩展 object-cache worker 单测。阶段 2 不为测试新增 fake publisher 接口；优先使用真实 publisher + ZMQ SUB，或复用 Yuanrong 现有 injection point 控制失败路径。

覆盖点：

1. `PublishObject()` 成功后收到一次 `stored(cpu)`。
2. `PublishObject()` 在 `RequestingToMaster()` / `SaveBinaryObjectToMemory()` / `SaveBinaryObjectToPersistence()` 任一失败时不发布 `stored(cpu)`。
3. `UpdateObjectAfterCreatingMeta()` 对每个成功更新为可读状态的 key 收到一次 `stored(cpu)`。
4. `ClearObject()` erase 成功后收到 `removed(cpu)`。
5. `ClearObject()` 在 `DeleteObjectFromDisk()` 或 `objectTable_->Erase()` 失败时不发布 `removed(cpu)`。
6. `EvictObject(Action::DELETE)` erase 成功后收到 `removed(cpu)`。
7. `EvictObject(Action::FREE_MEMORY)` free 成功后收到 `removed(cpu)`。
8. `EvictObject(Action::SPILL/MIGRATE/END_LIFE)` 阶段 2 不发布 cpu event。

### 10.2 集成测试

建议使用真实 publisher + ZMQ SUB：

1. 启动 worker，设置非空 `kv_events_config`。
2. SUB 连接 JSON 中的 `bind_endpoint`。
3. Set 一个可解析 rolling hash key。
4. 验证收到：

```text
event_type = stored
medium = cpu
seq_hashes = [parsed_key]
```

5. Delete 同一个 key。
6. 验证收到：

```text
event_type = removed
medium = cpu
seq_hashes = [parsed_key]
```

7. 压测队列满场景，确认业务请求不因 publisher 失败而失败。

### 10.3 不测内容

阶段 2 不验证：

- `stored(disk)`。
- `removed(disk)`。
- spill 成功后的 disk/cpu 组合事件。
- L2 write-through/write-back 持久化事件。
- recovery/migration 补发事件。

这些属于阶段 3/4。

## 11. 验收标准

阶段 2 完成后应满足：

1. `WorkerOCServiceImpl` 能将阶段 1 publisher 传递到 publish/delete/eviction 相关组件。
2. Set/Put 成功后发布 `stored(cpu)`。
3. MultiPublish 成功后对每个成功 key 发布 `stored(cpu)`。
4. Set/Put 失败不发布 `stored(cpu)`。
5. Delete/Clear 成功删除 object table 条目后发布 `removed(cpu)`。
6. Delete/Clear 失败不发布 `removed(cpu)`。
7. Eviction `DELETE` 成功后发布 `removed(cpu)`。
8. Eviction `FREE_MEMORY` 成功后发布 `removed(cpu)`。
9. 阶段 2 不发布任何 `disk` 事件。
10. 阶段 2 不改变原业务 Status 返回值。
11. 事件发布路径不执行 MessagePack 编码或 ZMQ send，只调用 `PublishStored/PublishRemoved`。

## 12. 推迟到后续阶段

- write-through L2 成功后的 `stored(disk)`。
- write-back L2 异步成功后的 `stored(disk)`。
- spill 成功后的 `stored(disk)` + `removed(cpu)` 组合。
- spill 文件或 L2 删除成功后的 `removed(disk)`。
- metadata recovery / migrate 后的补发事件。
- 启动时已有对象的 snapshot/replay。
