# SAI indexes written by Apache Cassandra 5.0

SSTables (format `nb`/big) and their SAI files (`SAI+aa+...`, Apache's on-disk version "aa") written by Apache
Cassandra's `cassandra-5.0` branch at fb7f1b7aee, for the tests of
`org.apache.cassandra.index.sai.disk.apache` and `org.apache.cassandra.index.sai.disk.format.ApacheSAIFilesTest`.

| Directory  | Schema (keyspace omitted) | Indexes |
|------------|---------------------------|---------|
| `skinny`   | `(pk int PRIMARY KEY, s text, n int, v vector<float, 3>)` | `skinny_s`, `skinny_n`, `skinny_v` |
| `wide`     | `(pk int, ck int, st int static, s text, n bigint, PRIMARY KEY (pk, ck))` | `wide_s`, `wide_n`, `wide_st` |
| `types`    | `(pk int PRIMARY KEY, i int, bi bigint, si smallint, ti tinyint, f float, d double, dec decimal, vi varint, dt date, tm time, ts timestamp, u uuid, tu timeuuid, ip inet, b boolean, t text, a ascii)` | `types_<column>` for every column; several segments in the first sstable; nulls, empty strings, deleted and expired rows |
| `descs`    | `(pk int, cb bigint, ci inet, cv varint, cd decimal, cn int, ct text, v int, PRIMARY KEY (pk, cb, ci, cv, cd, cn, ct)) WITH CLUSTERING ORDER BY (cb DESC, ci DESC, cv DESC, cd DESC, cn DESC, ct DESC)` | `descs_<column>`; several segments in the first sstable |
| `analyzed` | `(pk int PRIMARY KEY, t_ci text, t_norm text, t_ascii text, t_all text, a_ci ascii)` | `case_sensitive: false`, `normalize: true`, `ascii: true`, all three, and `case_sensitive: false` on ascii |
| `colls`    | `(pk int PRIMARY KEY, l list<int>, st set<text>, m map<text, int>, fm frozen<map<text, int>>, fl frozen<list<text>>)` | values of `l` and `st`, keys, values and entries of `m`, full `fm` and `fl` |
| `statics`  | `(pk1 int, pk2 text, ck int, st text static, sti int static, v int, PRIMARY KEY ((pk1, pk2), ck))` | every column; some partitions only have a static row |
| `vectors`  | `(pk int PRIMARY KEY, v vector<float, 8>, cat int)` | `vectors_v` (with product quantization; two segments in the first sstable), `vectors_cat` |
| `tombs`    | `(pk int, ck int, s text, n int, st set<text>, v int, PRIMARY KEY (pk, ck))` | `tombs_s`, `tombs_n`, `tombs_st`; the second sstable has row, range, partition, cell and collection deletions, expired cells and updates of rows of the first one |
| `vectors2` | `(pk int PRIMARY KEY, ve vector<float, 8>, vd vector<float, 8>)` | `vectors2_ve` (EUCLIDEAN), `vectors2_vd` (DOT_PRODUCT), with product quantization, on vectors not of unit length; vectors deleted and replaced in the memtables before the flushes |
| `mismatch` | `(pk int PRIMARY KEY, s text, v int, x text)` | `mismatch_s`; the first row (by token) only has a deleted `s`, so the row ids of Apache's index are one less than those of an index built here |
| `negative` | `(pk int PRIMARY KEY, st set<text>, v int)` | `negative_st`; the second sstable deletes the set of pk 1 (whose row stays alive) |
| `dot`      | `(pk int PRIMARY KEY, v vector<float, 8>)` | `dot_v` (DOT_PRODUCT, with product quantization), 4000 gaussian vectors, the first 200 of them scaled by 10 |

The several segments come from lowering `V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER` while building the indexes of an
existing sstable.

The `generator` directory has the source of the tests that wrote them, at the same version:
- `GenerateApacheSAIDataTest`: `skinny` and `wide`;
- `GenerateApacheSAIDataDeletionsTest`: `tombs` and `vectors2`;
- `GenerateApacheSAIDataMismatchTest`: `mismatch`, and `GenerateApacheSAIDataNegativeDotTest`: `negative` and `dot`
  (both into `/tmp/apache-sai-gen/data/<name>`);
- `GenerateApacheSAIDataExtendedTest`: the others.

To regenerate, copy them to `test/unit/org/apache/cassandra/index/sai/` of an Apache Cassandra 5.0 checkout (with the
`.java` extension), build it with `ant jar build-test`, list the test in a file and run
`ant testclasslist -Dtest.classlistfile=<file> -Dtest.classlistprefix=unit -Dtest.jvm.args="-Dsai.out=<dir>"`.
