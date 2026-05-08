# Data Housekeeper (Java 8)

This program recursively traverses a root directory (the current working directory), finds files whose **last modified time** is older than `retainDays`, and deletes them using `threadCount` worker threads.

## Build

From the project directory:

```bash
mvn package
```

## Run

```bash
java -jar target/data-housekeeper-1.0-SNAPSHOT.jar [retainDays] [threadCount]
```

Defaults:
- `retainDays=14`
- `threadCount=4`

Examples:

```bash
java -jar target/data-housekeeper-1.0-SNAPSHOT.jar
java -jar target/data-housekeeper-1.0-SNAPSHOT.jar 30
java -jar target/data-housekeeper-1.0-SNAPSHOT.jar 30 8
```

## Logging

A log file is written to the root directory (current working directory) with a name like:

`data-housekeeper-YYYYMMDD-HHmmss.log`

Log lines include:
- `RunStart=...` and `RunEnd=...`
- For each deletion attempt that is older than the cutoff:
  - `DeleteAt=...`
  - `action=deleted|not_found|age_check_error`
  - `path=...`
  - `relPath=...`

## File processing order and multithreading

All regular files are collected and sorted by filename (case-insensitive), with a deterministic tie-breaker using the file’s relative path.

The sorted list is then split into contiguous filename ranges and processed in parallel. This keeps each worker’s chunk in sorted order; deletions across different chunks may interleave.

