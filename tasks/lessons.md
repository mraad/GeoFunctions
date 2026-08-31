- Reported two background Maven builds as green off `EXIT=0` when the exit code came from the
  backgrounded wrapper, not Maven — both had actually printed `BUILD FAILURE`. `${PIPESTATUS[0]}`
  after a `| tail` is unreliable through the background runner. Rule: judge a Maven run by
  grepping its log for `BUILD SUCCESS`/`BUILD FAILURE`, never by a captured exit code alone.
