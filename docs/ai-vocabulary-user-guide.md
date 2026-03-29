# AI Vocabulary User Guide

## 1. 适用对象

适合这些角色使用：

- 知识库运营
- 术语管理员
- 规则维护人员
- Topic 配置人员

这个功能的目标不是自动发布规则，而是：

从已入库文档中自动提取候选术语，供人工审核后进入词库。

## 2. 使用前提

开始前请确认两件事：

1. 数据已经完成导入  
   文档已经完成解析、清洗、切块、向量化，并写入索引。

2. 当前数据集有可用内容  
   如果数据集还没有 chunk 和向量，AI 自动提取不会有结果。

## 3. 操作流程

### 第 1 步：选择数据集

进入 AI Vocabulary 页面后，先选择：

- `Dataset`
- `Prompt Version`
- `Model`

一般情况下：

- `Prompt Version` 使用默认版本
- `Model` 使用默认模型

只有在效果不理想时，才需要调整。

### 第 2 步：生成样本

点击：

- `Generate Sample`

这一步的作用是：

从当前数据集里抽取一批代表性文本，作为本次 AI 提取的固定输入。

重点参数：

- `sample_size`
  这次想抽多少条样本

- `similarity_threshold`
  样本之间的差异阈值

- `max_chunks_per_doc`
  每篇文档最多允许贡献几个样本

建议：

- 如果想覆盖更多文档，优先使用较小的 `max_chunks_per_doc`，例如 `1` 或 `2`
- 如果想从重点长文中多抽一些术语，可以适当调大

样本生成成功后，系统会保存一个新的 `sample version`。

### 第 3 步：检查样本

进入 `Sample Versions` 页面，打开刚生成的样本版本。

重点看：

- 样本数量是否正常
- 是否覆盖了不同文档
- `sample_content` 是否可读
- 是否存在明显 OCR 噪声、目录、页眉页脚

如果样本本身质量不好，不建议直接跑 AI。
应该重新生成样本，而不是继续执行提取。

### 第 4 步：创建提取任务

点击：

- `Create Run`

系统会把这次任务和以下条件绑定：

- 当前数据集
- 当前 `sample version`
- 当前 `prompt version`
- 当前模型

这一步相当于创建一批固定条件下的 AI 抽词任务。

### 第 5 步：执行提取

点击：

- `Execute Run`

建议在 UI 中优先使用异步执行。

执行后，可以在 `Runs` 列表页或 `Run Detail` 页面查看进度。

重点关注：

- `status`
- `processed_samples`
- `total_samples`
- `total_terms`
- `last_progress_message`
- `last_heartbeat_at`

只要：

- `processed_samples` 在增长
- `last_progress_message` 在变化

说明任务仍在正常执行。

### 第 6 步：查看候选词

提取完成后，进入 `Candidate Review` 页面。

这里显示的是 AI 提取出的候选术语。  
这些候选已经进入候选层，但还没有正式生效。

重点看：

- 术语是否有业务价值
- evidence 是否足够支持术语
- 是否适合用于 Topic / Rule 构建
- 是否为噪声词、泛词、重复词

### 第 7 步：查看证据

打开某个候选词详情，可以看到：

- 来源 run
- 来源 sample version
- 来源文档
- evidence 内容
- 多条 evidence

审核时不要只看词本身，一定要看 evidence。

### 第 8 步：审核处理

对每个候选词做处理：

- 合适：`Publish`
- 不合适：`Reject`
- 暂时保留：`CANDIDATE`

建议审核原则：

- 能直接用于规则构建的词优先发布
- 仅是主题词、描述词、泛化词时要谨慎
- evidence 弱或语义不完整的词优先拒绝

## 4. 结果评估建议

不要只看“抽出来多少词”，建议重点看：

1. 是否补到了 NLP 难以稳定抽出的完整短语
2. 是否减少了碎片词
3. evidence 是否足够硬
4. 审核通过率是否合理
5. 发布后的词是否真能用于 Topic / Rule

## 5. 常见问题

### 为什么 sample 看起来总来自少数文档？

系统已经加入文档覆盖控制。
如果你仍觉得集中在少数文档，可以进一步降低：

- `max_chunks_per_doc`

### 为什么任务一直显示 RUNNING？

重点看：

- `processed_samples`
- `last_progress_message`
- `last_heartbeat_at`

如果这些字段持续变化，说明任务仍在执行中。

### 为什么有些词看起来合理，却不建议发布？

因为这个系统生产的是候选术语，不是自动生效术语。
是否发布，取决于它能否真正用于规则构建。

### 为什么 AI 没覆盖所有词？

因为 sample 的目标不是全覆盖，而是高质量覆盖。
全量覆盖仍然主要依赖 NLP 候选和后续多轮补样。

## 6. 推荐日常使用方式

对运营和术语管理员，建议采用下面的节奏：

1. 新数据入库后，先生成一版 sample
2. 跑一版 AI extract
3. 先审核高置信度词
4. 再看高 `evidence_count` 的词
5. 批量发布明显正确的词
6. 拒绝噪声词
7. 如果结果不好，再调整 sample 和 prompt

## 7. 一句话流程

选择数据集 -> 生成样本 -> 检查样本 -> 创建 Run -> 执行 Run -> 查看候选 -> 审核发布
