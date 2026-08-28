# Merge Pull Requests with Merge Queue

SeaTunnel uses [GitHub Merge Queue](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/merging-a-pull-request-with-a-merge-queue) for pull requests targeting `dev`. The queue validates a pull request against the latest `dev` commit immediately before merge.

## Add a Pull Request to the Queue

1. Confirm that the pull request has the required approval and a successful pull-request `Build`.
2. Select **Merge when ready**.
3. GitHub creates a temporary branch named like `gh-readonly-queue/dev/pr-<number>-<sha>`.
4. The `Merge Queue` workflow runs the required `Build` against that temporary commit.
5. If the build succeeds, GitHub squash-merges the pull request into `dev`.

The queue build compiles the standard Maven reactor with the `ci` profile and then compiles `seatunnel-benchmarks` with the `benchmark` profile. Both commands use `-DskipTests`: tests are not executed, but main and test sources are compiled.

## Find Why a Pull Request Left the Queue

When a required check fails, times out, or the temporary commit conflicts with `dev`, GitHub removes the pull request from the queue and records the reason in the pull request timeline.

1. Open the pull request and find the merge-queue removal event in the timeline.
2. Follow the failed `Build` or **Details** link to the workflow run.
3. If the timeline does not provide a run link, open **Actions** in `apache/seatunnel`, select **Merge Queue**, and find the run whose branch contains `pr-<pull-request-number>-`.
4. Open the `Build` job and expand the failed step:
   - `Compile main and test sources`
   - `Compile benchmark main and test sources`
5. Search the log for the first Maven `[ERROR]` or `BUILD FAILURE`. The final `Process completed with exit code 1` line only reports the result; the useful cause normally appears earlier.

If the browser view is truncated or difficult to search, use **Download log archive** from the run page. With GitHub CLI, print only failed logs with:

```shell
gh run view <run-id> --repo apache/seatunnel --log-failed
```

## Classify the Failure

| Log signal | Likely cause | Next action |
| --- | --- | --- |
| `COMPILATION ERROR`, `cannot find symbol`, `incompatible types`, or `method ... cannot be applied` | The queued commit does not compile with the latest `dev` | Fix the code or rebase the pull request |
| `Could not transfer artifact`, `Connection reset`, `Read timed out`, or an HTTP 403/429/5xx response | Maven repository or network failure | Confirm that no compilation error occurred, then requeue |
| The job reaches the 30-minute limit or is cancelled while a Maven step is still running | Runner, dependency download, or unexpectedly slow build | Check the last active step; investigate repeated timeouts before requeueing |
| No `Merge Queue` run starts, or the required `Build` remains pending until the queue timeout | Queue event, runner scheduling, or status-reporting problem | Check whether a `merge_group` run was created and whether the required check is linked to that run; preserve the pull request and run links for further investigation |
| The pull request timeline reports a base-branch conflict or branch-protection failure | The temporary commit no longer satisfies merge requirements | Update or rebase the pull request and complete the required checks again |

## Recover and Requeue

- For a code or compatibility failure, push the fix or rebase onto the latest `dev`. Wait for the pull-request checks and any required approval to pass again, then select **Merge when ready**.
- For a confirmed temporary infrastructure failure, select **Merge when ready** again without changing the code.
- Do not repeatedly requeue an unknown failure. Save the PR URL, workflow run URL, failed step, and the first relevant error when asking the community for help.
- Re-running an old workflow run does not add an ejected pull request back to the queue; requeue it from the pull request after the cause has been handled.

If an earlier queue entry fails, GitHub rebuilds later temporary groups without that entry. No manual action is required for those later pull requests unless their own checks fail.

## Last-resort Recovery When Merge Queue Remains Unavailable

When Merge Queue itself fails and the troubleshooting and recovery steps above do not restore its ability to queue or merge an otherwise mergeable pull request, ASF Infra may use the `apache/root` team bypass as a last-resort recovery path.

The bypass mode is `always`, so it applies to both pull-request merges and direct pushes for this ruleset; separate branch-protection rules are evaluated independently.

:::warning Use only when normal recovery has failed

This bypass is not an alternative merge path. It must not be used to skip normal queue waiting, required approval, a failed `Build`, or a compilation problem.

Preserve the pull request and workflow-run links when contacting ASF Infra. Whenever possible, recovery should use a reviewed pull request with a successful normal `Build`; direct push should remain a last resort.

Bypassing Merge Queue removes its exact-combination validation and may make active merge groups stale. After recovery, verify the new `dev` commit with the same Maven compile commands or an equivalent post-merge build, and monitor queued pull requests for automatic rebuilds or failures.

:::
