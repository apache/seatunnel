# 使用 Merge Queue 合并 Pull Request

SeaTunnel 对目标分支为 `dev` 的 Pull Request 启用了 [GitHub Merge Queue](https://docs.github.com/zh/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/merging-a-pull-request-with-a-merge-queue)。队列会在合并前，基于最新的 `dev` 再次验证 PR。

## 将 Pull Request 加入队列

1. 确认 PR 已获得所需批准，并且 PR 自身的 `Build` 成功。
2. 选择 **Merge when ready**。
3. GitHub 会创建名称类似 `gh-readonly-queue/dev/pr-<number>-<sha>` 的临时分支。
4. `Merge Queue` workflow 会对这个临时提交运行必需的 `Build`。
5. 构建成功后，GitHub 会将 PR squash merge 到 `dev`。

队列构建首先使用 `ci` profile 编译标准 Maven reactor，然后使用 `benchmark` profile 编译 `seatunnel-benchmarks`。两个命令都使用 `-DskipTests`：测试不会执行，但 main 和 test 源码仍会编译。

## 查看 Pull Request 被移出队列的原因

必需检查失败或超时，或者临时提交与 `dev` 冲突时，GitHub 会将 PR 移出队列，并在 PR timeline 中记录原因。

1. 打开 PR，在 timeline 中找到被移出 Merge Queue 的事件。
2. 通过失败的 `Build` 或 **Details** 链接进入 workflow run。
3. 如果 timeline 中没有 run 链接，进入 `apache/seatunnel` 的 **Actions**，选择 **Merge Queue**，查找分支名包含 `pr-<PR编号>-` 的 run。
4. 打开 `Build` job，并展开失败的步骤：
   - `Compile main and test sources`
   - `Compile benchmark main and test sources`
5. 在日志中搜索第一个 Maven `[ERROR]` 或 `BUILD FAILURE`。最后的 `Process completed with exit code 1` 只表示执行失败，真正的原因通常在它之前。

如果网页日志被截断或不方便搜索，可以在 run 页面选择 **Download log archive**。也可以使用 GitHub CLI 只查看失败日志：

```shell
gh run view <run-id> --repo apache/seatunnel --log-failed
```

## 判断失败类型

| 日志特征 | 可能原因 | 处理方式 |
| --- | --- | --- |
| `COMPILATION ERROR`、`cannot find symbol`、`incompatible types` 或 `method ... cannot be applied` | 当前队列提交无法与最新 `dev` 一起编译 | 修复代码或对 PR 执行 rebase |
| `Could not transfer artifact`、`Connection reset`、`Read timed out` 或 HTTP 403/429/5xx | Maven 仓库或网络故障 | 确认没有编译错误后重新入队 |
| Job 达到 30 分钟限制，或者在 Maven 步骤仍在运行时被取消 | Runner、依赖下载或构建耗时异常 | 查看最后执行的步骤；重复超时时应先排查，不要直接重新入队 |
| 没有触发 `Merge Queue` run，或者必需的 `Build` 一直 pending 直到队列超时 | 队列事件、Runner 调度或状态上报异常 | 检查是否生成 `merge_group` run、required check 是否关联到该 run，并保留 PR 和 run 链接以便进一步排查 |
| PR timeline 提示与目标分支冲突或分支保护失败 | 临时提交不再满足合并要求 | 更新或 rebase PR，并重新完成必需检查 |

## 修复并重新入队

- 如果是代码或兼容性问题，请提交修复或基于最新 `dev` 执行 rebase。等待 PR 检查和所需批准再次通过后，选择 **Merge when ready**。
- 如果已经确认是临时基础设施故障，可以不修改代码，直接再次选择 **Merge when ready**。
- 不要对原因不明的失败反复重新入队。向社区求助时，请提供 PR URL、workflow run URL、失败步骤和第一个有效错误。
- 重新运行旧的 workflow run 不会让已被移出的 PR 重新进入队列；处理完失败原因后，需要回到 PR 页面重新入队。

如果较早的队列条目失败，GitHub 会排除该条目并重新构建后续临时 merge group。除非后续 PR 自身的检查也失败，否则不需要人工处理。

## 紧急 Bypass

Ruleset 为具有仓库 `Write` 角色的用户提供仅限 Pull Request 合并的 bypass，不允许通过直接 push 绕过规则。

:::warning 仅用于 Merge Queue 紧急故障

只有 Merge Queue 本身不可用或运行异常时才能使用 bypass。不要用它跳过正常排队、必需的批准、失败的 `Build` 或编译问题。

队列仍在运行时，bypass 会改变正常的合并顺序保证：

- 如果 bypass PR 先进入 `dev`，基于旧 `dev` 创建的 merge group 会失效。GitHub 必须重新构建这些 merge group，正在运行的 CI 也会被浪费。
- 如果队列中的 PR 先进入 `dev`，bypass PR 随后可能在没有针对最终组合执行 merge-group build 的情况下合并。普通 PR 的 `Build` 可能基于较旧的 `dev`，因此未经验证的代码组合可能进入 `dev`。

以上情况不会覆盖或丢失已有提交，但会浪费 CI 资源，并失去 Merge Queue 正常提供的最终组合验证。

使用 bypass 前：

1. 检查 merge queue 和正在运行的 `Merge Queue` workflow；只要条件允许，应等待队列空闲。
2. 确认 PR 已获得所需批准，并且普通 Pull Request 的 `Build` 已成功。
3. 在 PR 中说明 bypass 原因，并附上队列故障或服务异常的链接。

使用 bypass 后：

1. 使用相同的 Maven 编译命令验证新的 `dev` 提交，或者确认等价的 post-merge build 已成功。
2. 关注队列中其他 PR 是否自动重新构建 merge group，以及是否出现失败。
3. 在 dev 邮件列表报告队列异常；需要仓库级恢复时联系 ASF Infra。

:::
