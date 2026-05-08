package com.mytestapp.housekeeping;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Recursively traverses files under the working directory and deletes files older than retainDays.
 *
 * Work is deterministically split across a fixed thread pool by processing filename-sorted chunks.
 */
public class DataHousekeeper {

  private static final DateTimeFormatter LOG_TS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  public static void main(String[] args) {
    if (args == null || args.length != 2) {
      System.err.println("Usage: java -jar data-housekeeper.jar <retainDays> <threadCount>");
      System.err.println("Example: java -jar data-housekeeper.jar 30 8");
      System.exit(2);
      return;
    }

    final int retainDays;
    final int threadCount;
    try {
      retainDays = Integer.parseInt(args[0]);
      threadCount = Integer.parseInt(args[1]);
    } catch (NumberFormatException e) {
      System.err.println("retainDays and threadCount must be integers.");
      System.exit(2);
      return;
    }

    if (retainDays < 0) {
      System.err.println("retainDays must be >= 0");
      System.exit(2);
      return;
    }
    if (threadCount <= 0) {
      System.err.println("threadCount must be > 0");
      System.exit(2);
      return;
    }

    final Path root = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();

    final Instant runStart = Instant.now();
    final String logFileName = "data-housekeeper-" + DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
        .withZone(ZoneId.systemDefault()).format(runStart) + ".log";
    final Path logPath = root.resolve(logFileName);

    try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logPath.toFile(), false))) {
      synchronized (logWriter) {
        logWriter.write("RunStart=" + LOG_TS.format(runStart));
        logWriter.newLine();
        logWriter.write("Root=" + root);
        logWriter.newLine();
        logWriter.write("retainDays=" + retainDays + ", threadCount=" + threadCount);
        logWriter.newLine();
      }

      // Compute cutoff: strictly older means timestamp < cutoffInstant.
      final Instant cutoff = runStart.minus(retainDays, ChronoUnit.DAYS);

      List<Path> allFiles = listAllFiles(root);
      if (allFiles.isEmpty()) {
        synchronized (logWriter) {
          logWriter.write("RunEnd=" + LOG_TS.format(Instant.now()));
          logWriter.newLine();
        }
        return;
      }

      // Sort by filename (then tie-break deterministically by relative path).
      final List<Path> sorted = new ArrayList<>(allFiles);
      final Comparator<Path> cmp = Comparator
          .comparing((Path p) -> p.getFileName().toString(), String.CASE_INSENSITIVE_ORDER)
          .thenComparing(p -> root.relativize(p).toString(), String.CASE_INSENSITIVE_ORDER)
          .thenComparing(p -> p.toString(), String.CASE_INSENSITIVE_ORDER);
      Collections.sort(sorted, cmp);

      final int workers = Math.min(threadCount, sorted.size());
      final ExecutorService pool = Executors.newFixedThreadPool(workers);

      // Deterministically split the sorted list into contiguous index ranges.
      for (int i = 0; i < workers; i++) {
        final int from = (i * sorted.size()) / workers;
        final int to = ((i + 1) * sorted.size()) / workers;
        if (from >= to) {
          continue;
        }
        final List<Path> chunk = sorted.subList(from, to);
        pool.submit(new DeletionWorker(chunk, root, cutoff, logWriter));
      }

      pool.shutdown();
      pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

      synchronized (logWriter) {
        logWriter.write("RunEnd=" + LOG_TS.format(Instant.now()));
        logWriter.newLine();
      }
    } catch (Exception e) {
      System.err.println("Failed to run data housekeeper: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static List<Path> listAllFiles(Path root) {
    List<Path> result = new ArrayList<>();
    try {
      // Files.walk does not follow symbolic links by default; avoid cycles on Windows.
      // If a filesystem throws during traversal, we skip that sub-tree.
      try (Stream<Path> stream = Files.walk(root)) {
        stream.forEach(p -> {
          if (Files.isRegularFile(p)) {
            result.add(p);
          }
        });
      }
    } catch (IOException e) {
      // Best-effort traversal; return what we have.
    }
    return result;
  }

  private static final class DeletionWorker implements Runnable {
    private final List<Path> files;
    private final Path root;
    private final Instant cutoff;
    private final BufferedWriter logWriter;

    private DeletionWorker(List<Path> files, Path root, Instant cutoff, BufferedWriter logWriter) {
      this.files = files;
      this.root = root;
      this.cutoff = cutoff;
      this.logWriter = logWriter;
    }

    @Override
    public void run() {
      for (Path p : files) {
        try {
          if (isOlderThanCutoffByLastModified(p, cutoff)) {
            deleteAndLog(p);
          }
        } catch (Exception e) {
          logFailure(p, "age_check_error", e);
        }
      }
    }

    private boolean isOlderThanCutoffByLastModified(Path p, Instant cutoffInstant) throws IOException {
      BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
      // As per requirement, use lastModifiedTime for the age check.
      Instant fileTime = attrs.lastModifiedTime().toInstant();
      return fileTime.isBefore(cutoffInstant);
    }

    private void deleteAndLog(Path p) throws IOException {
      Instant ts = Instant.now();
      boolean deleted = Files.deleteIfExists(p);
      String action = deleted ? "deleted" : "not_found";
      String rel = root.relativize(p).toString();
      synchronized (logWriter) {
        logWriter.write("DeleteAt=" + LOG_TS.format(ts));
        logWriter.write(", action=" + action);
        logWriter.write(", path=" + p.toString());
        logWriter.write(", relPath=" + rel);
        logWriter.newLine();
        logWriter.flush(); // Keep log durable even if the process is interrupted.
      }
    }

    private void logFailure(Path p, String kind, Exception e) {
      Instant ts = Instant.now();
      synchronized (logWriter) {
        try {
          logWriter.write("DeleteAt=" + LOG_TS.format(ts));
          logWriter.write(", action=" + kind);
          logWriter.write(", path=" + p.toString());
          logWriter.write(", error=" + safeMessage(e));
          logWriter.newLine();
          logWriter.flush();
        } catch (IOException ignored) {
          // If logging fails, we can't do much; swallow to keep other workers running.
        }
      }
    }

    private String safeMessage(Exception e) {
      String msg = (e == null || e.getMessage() == null) ? e.getClass().getSimpleName() : e.getMessage();
      msg = msg.replace('\n', ' ').replace('\r', ' ');
      return msg.length() > 300 ? msg.substring(0, 300) : msg;
    }
  }
}

