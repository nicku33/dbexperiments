
## Shared KV store without installing a service

If you had multiple processes trying to share a KV store for caching purposes and you
didn't want to have to set up Redis or a shared database, or any other process,
what can be done.

I tried out some approaches:
 * the python dbm as as single file with file locks
 * dbm, but sharded across the first 8, then 16 bits of the keyspace
 * sqllite3: standard filelocks and the new WAL method

For each: I loaded 20,000 keys, then had 8 processes check, then put keys if missing, 50k times each.

Approach 1: A single dbm file, opened each time read or write is required, with a file lock guard around it.
This started at 1.8 ms, but with contention got up to 4 ms. The file footprint got to 12M

Approach 2: Split into 256 dbm files. This had decent performance at about 1ms and file size stayed the smea.

Approach 3: Split into 2^16 dbm files. This had better performance of 0.7ms but the file size on disk bloated with the ~64k files
due to minimum blocks. du -sh reports about 1GB on disk. This would get amortized with more keys however.

Approach 4: Use SQLLite with a uniqueness constraint, which uses file system locks for write operations. Performance started at 1ms then down to 0.5  as the
locks are at the block level, so the likelyhood of concurrent access to the same block got lower as we wrote more. File size was the smallest
at 6MB as sqlite is highly compact.

Approach 5: Use SQLite in it's WAL mode. In WAL mode, recent transactions are written to a seperate file, and then compacted
at about 1-4 MB in size. This allows concurrent read and writes since writes go to the WAL file. This had the best perf at 0.6 ms
and on disk footprint was 8MB. 

So overall, SQLite with WAL wins, and as a bonus, you have a full database with table semantics, which can execute complex queries, and has the sqlite CLI.



