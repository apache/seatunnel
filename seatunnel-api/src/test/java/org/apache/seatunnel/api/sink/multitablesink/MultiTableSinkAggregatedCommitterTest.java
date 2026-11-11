    void testSharedAggregatedCommitterIsDrivenOnceForAllAliases() throws Exception {
        String table1 = "catalog.db.table1";
        String table2 = "catalog.db.table2";
        RecordingAggregatedCommitter sharedCommitter = new RecordingAggregatedCommitter();
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        aggCommitters.put(table1, sharedCommitter);
        aggCommitters.put(table2, sharedCommitter);
        MultiTableSinkAggregatedCommitter committer =
                new MultiTableSinkAggregatedCommitter(aggCommitters);
        MultiTableCommitInfo commitInfo = new MultiTableCommitInfo(new ConcurrentHashMap<>());
        commitInfo.getCommitInfo().put(SinkIdentifier.of(table1, 0), "t1-c0");
        commitInfo.getCommitInfo().put(SinkIdentifier.of(table2, 0), "t2-c0");

        committer.init();
        MultiTableAggregatedCommitInfo combined =
                committer.combine(Collections.singletonList(commitInfo));
        committer.commit(Collections.singletonList(combined));
        committer.abort(Collections.singletonList(combined));
        committer.close();

        Assertions.assertEquals(1, sharedCommitter.initCount);
        Assertions.assertEquals(1, sharedCommitter.combineCount);
        Assertions.assertEquals(1, sharedCommitter.commitCount);
        Assertions.assertEquals(1, sharedCommitter.abortCount);
        Assertions.assertEquals(1, sharedCommitter.closeCount);
        Assertions.assertEquals(2, sharedCommitter.combined.size());
        Assertions.assertTrue(
                sharedCommitter.combined.containsAll(Arrays.asList("t1-c0", "t2-c0")));
        Assertions.assertEquals(1, combined.getCommitInfo().size());
    }

    private static class RecordingAggregatedCommitter
            implements SinkAggregatedCommitter<Object, Object> {

        private final List<Object> combined = new ArrayList<>();
        private int initCount;
        private int combineCount;
        private int commitCount;
        private int abortCount;
        private int closeCount;

        @Override
        public void init() {
            initCount++;
        }

        @Override
        public List<Object> commit(List<Object> aggregatedCommitInfo) {
            commitCount++;
            return Collections.emptyList();
        }

        @Override
        public Object combine(List<Object> commitInfos) {
            combineCount++;
            combined.addAll(commitInfos);
            return "combined";
        }

        @Override
        public void abort(List<Object> aggregatedCommitInfo) {
            abortCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

        void testRestoreCommitRoutesToTableRestoreCommit() throws IOException {
        List<String> calls = new ArrayList<>();
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        aggCommitters.put(
                "table1",
                new SinkAggregatedCommitter<Object, Object>() {
                    @Override
                    public List<Object> commit(List<Object> aggregatedCommitInfo) {
                        calls.add("commit" + aggregatedCommitInfo);
                        return Collections.emptyList();
                    }

                    @Override
                    public List<Object> restoreCommit(List<Object> aggregatedCommitInfo) {
                        calls.add("restoreCommit" + aggregatedCommitInfo);
                        return Collections.emptyList();
                    }

                    @Override
                    public Object combine(List<Object> commitInfos) {
                        return null;
                    }

                    @Override
                    public void abort(List<Object> aggregatedCommitInfo) {}

                    @Override
                    public void close() {}
                });
        MultiTableSinkAggregatedCommitter committer =
                new MultiTableSinkAggregatedCommitter(aggCommitters);
        Map<String, Object> info = new HashMap<>();
        info.put("table1", "xid-1");
        List<MultiTableAggregatedCommitInfo> infos =
                Collections.singletonList(new MultiTableAggregatedCommitInfo(info));

        committer.restoreCommit(infos);
        committer.commit(infos);

        Assertions.assertIterableEquals(
                Arrays.asList("restoreCommit[xid-1]", "commit[xid-1]"), calls);
    }
}
