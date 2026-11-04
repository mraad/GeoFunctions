- Reported two background Maven builds as green off `EXIT=0` when the exit code came from the
  backgrounded wrapper, not Maven — both had actually printed `BUILD FAILURE`. `${PIPESTATUS[0]}`
  after a `| tail` is unreliable through the background runner. Rule: judge a Maven run by
  grepping its log for `BUILD SUCCESS`/`BUILD FAILURE`, never by a captured exit code alone.
- 2026-11-04: chained `git push -q ... | tail -1; gh pr merge` - the push was rejected but the merge ran, so PR #31 merged without its review-fix commit. Rule: run the push alone and check its exit status before replying to review comments or merging.
