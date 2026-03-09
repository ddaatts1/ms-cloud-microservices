package com.micro.order_service.scape;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrademarkCrawler {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ===== CONFIG =====
    private static final int    START_YEAR        = 2020;
    private static final int    END_YEAR          = 2026;
    private static final int    END_SEQ_LAST_YEAR = 6167;
    private static final int    THREAD_COUNT      = 8;
    private static final long   DELAY_MS          = 1000;    // khong delay
    private static final int    MAX_RETRY_ROUNDS  = 100;  // retry tối đa 100 lần

    // Delay theo số lần retry: tăng dần, tối đa 60s
    // lần 1=5s, 2=10s, 3=15s, ... mỗi lần +5s, tối đa 60s
    private static final long MAX_RETRY_DELAY_MS = 2_000; // retry chi delay 1s

    // ===== FILES =====
    private static final String OUTPUT_FILE     = "trademark.txt";
    private static final String CHECKPOINT_FILE = "checkpoint.txt";

    // ===== DELAY (global giữa các request bình thường) =====
    private static final Object DELAY_LOCK    = new Object();
    private static long         lastRequestAt = 0;

    // ===== RETRY QUEUE: lưu cả ID lẫn thời điểm được phép gọi lại =====
    private static final ConcurrentHashMap<String, Integer> retryCount    = new ConcurrentHashMap<>();
    // Dùng DelayQueue để ID chỉ sẵn sàng sau khi hết thời gian chờ
    private static final DelayQueue<DelayedId>              retryQueue    = new DelayQueue<>();

    // ===== STATS =====
    private static final AtomicInteger totalSuccess = new AtomicInteger(0);
    private static final AtomicInteger totalSkipped = new AtomicInteger(0);
    private static final AtomicInteger totalRetried = new AtomicInteger(0);
    private static final AtomicInteger totalGaveUp  = new AtomicInteger(0);

    // ===== WRITER =====
    private static final BlockingQueue<String> writeQueue      = new LinkedBlockingQueue<>();
    private static final BlockingQueue<String> checkpointQueue = new LinkedBlockingQueue<>();
    private static volatile boolean writerRunning = true;

    // ===== DelayedId: wrapper để dùng với DelayQueue =====
    static class DelayedId implements Delayed {
        final String id;
        final long readyAt; // System.currentTimeMillis() khi được phép xử lý

        DelayedId(String id, long delayMs) {
            this.id      = id;
            this.readyAt = System.currentTimeMillis() + delayMs;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(readyAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(readyAt, ((DelayedId) o).readyAt);
        }
    }

    // =========================================================

    private static void log(String level, String message) {
        System.out.println("[" + LocalDateTime.now().format(FORMATTER) + "] "
                + level + " - " + message);
    }

    private static void waitDelay() throws InterruptedException {
        synchronized (DELAY_LOCK) {
            long now     = System.currentTimeMillis();
            long elapsed = now - lastRequestAt;
            long wait    = DELAY_MS - elapsed;
            if (wait > 0) Thread.sleep(wait);
            lastRequestAt = System.currentTimeMillis();
        }
    }

    private static Set<String> loadCheckpoint() {
        Set<String> done = Collections.newSetFromMap(new ConcurrentHashMap<>());
        File f = new File(CHECKPOINT_FILE);
        if (!f.exists()) return done;
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) done.add(line);
            }
            log("INFO", "Resume: bo qua " + done.size() + " IDs da xu ly");
        } catch (IOException e) {
            log("WARN", "Khong doc duoc checkpoint: " + e.getMessage());
        }
        return done;
    }

    private static Thread startWriterThread() throws IOException {
        File outFile  = new File(OUTPUT_FILE);
        File ckptFile = new File(CHECKPOINT_FILE);
        log("INFO", "Output     : " + outFile.getAbsolutePath());
        log("INFO", "Checkpoint : " + ckptFile.getAbsolutePath());

        BufferedWriter outWriter  = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(outFile,  true), "UTF-8"));
        BufferedWriter ckptWriter = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(ckptFile, true), "UTF-8"));

        Thread t = new Thread(() -> {
            try {
                while (writerRunning || !writeQueue.isEmpty() || !checkpointQueue.isEmpty()) {
                    String result = writeQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (result != null) { outWriter.write(result); outWriter.newLine(); outWriter.flush(); }
                    String ckpt = checkpointQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (ckpt != null) { ckptWriter.write(ckpt); ckptWriter.newLine(); ckptWriter.flush(); }
                }
            } catch (Exception e) {
                log("ERROR", "Writer error: " + e.getMessage());
            } finally {
                try { outWriter.flush();  outWriter.close();  } catch (IOException ignored) {}
                try { ckptWriter.flush(); ckptWriter.close(); } catch (IOException ignored) {}
                log("INFO", "Writer closed. success=" + totalSuccess.get()
                        + " | skipped=" + totalSkipped.get()
                        + " | gave_up=" + totalGaveUp.get());
            }
        }, "file-writer-thread");
        t.setDaemon(false);
        t.start();
        return t;
    }

    private static void startStatsMonitor() {
        new Thread(() -> {
            while (writerRunning) {
                try { Thread.sleep(10_000); } catch (InterruptedException e) { break; }
                log("STATS", "success=" + totalSuccess.get()
                        + " | skipped=" + totalSkipped.get()
                        + " | retry_queue=" + retryQueue.size()
                        + " | gave_up=" + totalGaveUp.get());
            }
        }, "stats-monitor").start();
    }

    // =========================================================
    // MAIN CRAWL
    // =========================================================

    public static void crawlRange() throws Exception {
        Set<String> done    = loadCheckpoint();
        Thread writerThread = startWriterThread();
        startStatsMonitor();

        log("INFO", "===== START CRAWL | year=" + START_YEAR + "-" + END_YEAR
                + " | threads=" + THREAD_COUNT + " | delay=" + DELAY_MS + "ms =====");

        // Vong chinh: submit ID vao executor
        final int TEST_LIMIT = 100; // doi thanh -1 de chay full
        int submitted = 0;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        outer:
        for (int year = START_YEAR; year <= END_YEAR; year++) {
            int maxSeq = (year == END_YEAR) ? END_SEQ_LAST_YEAR : 99999;
            for (int seq = 1; seq <= maxSeq; seq++) {
                final String id = String.format("VN4%d%05d", year, seq);
                if (done.contains(id)) continue;
                executor.submit(() -> processId(id));
                if (TEST_LIMIT > 0 && ++submitted >= TEST_LIMIT) break outer;
            }
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        // Vòng retry: poll từ DelayQueue (chỉ lấy khi hết delay)
        while (!retryQueue.isEmpty()) {
            log("INFO", "===== RETRY ROUND | " + retryQueue.size() + " IDs cho retry =====");

            // Drain những cái đã sẵn sàng (hết delay)
            // Những cái chưa đến giờ sẽ chờ
            List<DelayedId> batch = new ArrayList<>();
            retryQueue.drainTo(batch); // chỉ lấy cái getDelay() <= 0

            if (batch.isEmpty()) {
                // Chưa có cái nào sẵn sàng, chờ cái gần nhất
                DelayedId next = retryQueue.take(); // block đến khi có
                batch.add(next);
                retryQueue.drainTo(batch); // lấy thêm nếu có
            }

            log("INFO", "Chay lai " + batch.size() + " IDs...");
            ExecutorService retryExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
            for (DelayedId item : batch) retryExecutor.submit(() -> processId(item.id));
            retryExecutor.shutdown();
            retryExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }

        writerRunning = false;
        writerThread.join();

        log("INFO", "===== END CRAWL | success=" + totalSuccess.get()
                + " | skipped=" + totalSkipped.get()
                + " | retried=" + totalRetried.get()
                + " | gave_up=" + totalGaveUp.get() + " =====");
    }

    // =========================================================
    // PROCESS 1 ID
    // =========================================================

    private static void processId(String id) {
        String urlString =
                "https://wipopublish.ipvietnam.gov.vn/wopublish-search/public/ajax/detail/trademarks?id=" + id;

        HttpURLConnection conn = null;
        try {
            waitDelay();
            log("INFO", "GET " + id);

            conn = (HttpURLConnection) new URL(urlString).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(10_000);
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            conn.setRequestProperty("Accept", "text/html");

            int code = conn.getResponseCode();

            if (code == 200) {
                StringBuilder sb = new StringBuilder();
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), "UTF-8"))) {
                    String line;
                    while ((line = in.readLine()) != null) sb.append(line);
                }
                String trademark = extractTrademark(sb.toString());
                if (trademark != null && !trademark.isEmpty()) {
                    log("INFO", id + " -> " + trademark);
                    writeQueue.put(id + " | " + trademark);
                    totalSuccess.incrementAndGet();
                    checkpointQueue.put(id); // OK → checkpoint
                } else {
                    // 200 nhưng không có trademark → retry
                    scheduleRetry(id, code, "200 khong co nhan hieu | " + urlString);
                }

            } else {
                // Mọi HTTP khác (404, 500, 503...) → retry
                scheduleRetry(id, code, urlString);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            scheduleRetry(id, -1, urlString + " | " + e.getMessage());
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    // =========================================================
    // SCHEDULE RETRY với delay tăng dần
    // =========================================================

    private static void scheduleRetry(String id, int httpCode, String detail) {
        int count = retryCount.merge(id, 1, Integer::sum);

        if (count > MAX_RETRY_ROUNDS) {
            log("ERROR", id + " | Gave up sau " + MAX_RETRY_ROUNDS
                    + " retries | last HTTP=" + httpCode + " | " + detail);
            totalGaveUp.incrementAndGet();
            try { checkpointQueue.put(id); } catch (InterruptedException ignored) {}
        } else {
            // Delay tăng dần mỗi 5s, tối đa 60s
            // lan 1=5s, lan 2=10s, lan 3=15s, ..., lan 12+=60s
            long delayMs = MAX_RETRY_DELAY_MS; // luon 1s
            log("WARN", id + " | HTTP " + httpCode
                    + " -> retry lan " + count + "/" + MAX_RETRY_ROUNDS
                    + " sau " + (delayMs / 1000) + "s | " + detail);
            totalRetried.incrementAndGet();
            retryQueue.offer(new DelayedId(id, delayMs));
        }
    }

    private static String extractTrademark(String html) {
        Matcher m = Pattern.compile(
                "\\(541\\) Nh\u00e3n hi\u1ec7u</div>\\s*<div class=\"col-md-4 product-form-details\">(.*?)</div>",
                Pattern.DOTALL).matcher(html);
        if (m.find()) {
            return m.group(1)
                    .replaceAll("<[^>]*>", "")
                    .replace("(VI)", "")
                    .trim();
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        crawlRange();
    }
}