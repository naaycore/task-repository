package com.prodigious.Scheduler;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Scheduler {
    private final AtomicBoolean accepting;
    private final AtomicBoolean stopping;

    private final AtomicLong ids;

    private final PriorityQueue<Task> taskHeap;
    private final BlockingQueue<Task> readyQueue;
    private final ConcurrentMap<Long, AtomicBoolean> cancels;

    private final ReentrantLock lock;
    private final Condition timerSignal;

    private final ExecutorService workers;
    private final int workerCount;
    private final Thread timerThread;


    public Scheduler(int workerCount) {
        stopping = new AtomicBoolean();
        accepting = new AtomicBoolean();

        ids = new AtomicLong(0L);

        taskHeap = new PriorityQueue<>();
        readyQueue = new LinkedBlockingQueue<>();
        cancels = new ConcurrentHashMap<>();

        lock = new ReentrantLock();
        timerSignal = lock.newCondition();

        this.workerCount = workerCount;

        workers = Executors.newFixedThreadPool(this.workerCount);
        timerThread = new Thread(this::timerLoop);
    }

    public <T> CompletableFuture<T> submit(Callable<T> callable) {
        if (!accepting.get()) {
            return CompletableFuture.failedFuture(new RejectedExecutionException(
                    "Scheduler cannot accept tasks"));
        }

        CompletableFuture<T> completableFuture = new CompletableFuture<>();

        Runnable runnable = () -> {
            try {
                T t = callable.call();
                completableFuture.complete(t);
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
        };

        scheduleInternal(
                ids.incrementAndGet(),
                runnable,
                0L,
                0L,
                false,
                null
        );

        return completableFuture;
    }

    public TaskHandle scheduleAt(
            Runnable runnable,
            LocalDateTime dateTime
    ) {
        if (!accepting.get()) {
            throw new RejectedExecutionException(
                    "Scheduler cannot accept tasks");
        }

        long sec = dateTime.toEpochSecond(ZoneOffset.UTC);
        long deltaSec = sec - LocalDateTime
                .now()
                .toEpochSecond(ZoneOffset.UTC);
        long runAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(deltaSec);

        return scheduleInternal(
                ids.incrementAndGet(),
                runnable,
                runAt,
                0L,
                false,
                null
        );
    }

    public TaskHandle scheduleFixedRate(
            Duration initialDelay,
            Duration period,
            Runnable runnable
    ) {
        long id = ids.incrementAndGet();

        return scheduleInternal(
                id,
                runnable,
                initialDelay.toNanos() + System.nanoTime(),
                period.toNanos(),
                true,
                PeriodMode.FIXED_RATE
        );

    }

    private void signalChanged() {
        lock.lock();
        try {
            timerSignal.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean cancel(TaskHandle handle) {
        if (handle == null) {
            return false;
        }
        AtomicBoolean flag = cancels.get(handle.getId());
        if (flag == null) {
            return false;
        }
        boolean prev = flag.getAndSet(true);
        if (!prev) {
            signalChanged();
        }
        return !prev;

    }

    private TaskHandle scheduleInternal(
            long id,
            Runnable runnable,
            long runAt,
            long periodicNano,
            boolean isPeriodic,
            PeriodMode periodMode
    ) {
        if (!cancels.containsKey(id)) {
            AtomicBoolean flag = new AtomicBoolean(false);
            cancels.put(id, flag);
        }

        Task task;
        if (isPeriodic) {
            task = new PeriodicTask(
                    id,
                    runnable,
                    runAt,
                    periodicNano,
                    periodMode,
                    cancels.get(id),
                    false
            );
        } else {
            task = new Task(id, runnable, runAt, cancels.get(id));
        }

        TaskHandle handle = new TaskHandle(id);

        if (runAt <= System.nanoTime()) {
            readyQueue.offer(task);
            return handle;
        }

        requeueTask(task);
        return handle;
    }

    public void start() {
        accepting.set(true);
        stopping.set(false);
        for (int i = 0; i < workerCount; i++) {
            workers.submit(this::workerLoop);
        }
        timerThread.start();
    }

    public void shutdown() throws InterruptedException {
        accepting.set(false);
        stopping.set(true);
        timerThread.join();
        workers.shutdown();
    }

    private void workerLoop() {
        while (true) {
            if (stopping.get()) {
                if (readyQueue.isEmpty()) {
                    return;
                }
            }

            if (readyQueue.isEmpty()) {
                continue;
            }

            Task task = readyQueue.poll();

            if (task == null) {
                continue;
            }

            if (task.getCancelled().get()) {
                cleanupTask(task);
                continue;
            }

            long runTime = System.nanoTime();

            try {
                task.getRunnable().run();
            } catch (Exception e) {
                log.error("Error while running Task {} ", task.getId());
            }
            if (!(task instanceof PeriodicTask periodicTask)) {
                cleanupTask(task);
                continue;
            }

            if (periodicTask.isPeriodicCancelled()) {
                cleanupTask(periodicTask);
                continue;
            }

            long nextRunAt;

            if (periodicTask.getPeriodMode() == PeriodMode.FIXED_RATE) {
                nextRunAt = periodicTask.getRunAt() + periodicTask.getPeriodNano();
            } else {
                nextRunAt = runTime + periodicTask.getPeriodNano();
            }

            periodicTask.setRunAt(nextRunAt);

            requeueTask(periodicTask);
        }
    }

    private void requeueTask(Task task) {
        if (task instanceof PeriodicTask && ((PeriodicTask) task).isPeriodicCancelled()) {
            cleanupTask(task);
            return;
        }
        if (task.getCancelled().get()) {
            cleanupTask(task);
            return;
        }
        lock.lock();
        try {
            taskHeap.offer(task);
            if (taskHeap.peek() == task) {
                timerSignal.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    private void timerLoop() {
        while (true) {
            lock.lock();
            try {
                if (stopping.get()) {
                    if (taskHeap.isEmpty()) {
                        return;
                    }
                }

                if (taskHeap.isEmpty()) {
                    timerSignal.await();
                }

                Task headTask = taskHeap.peek();

                if (headTask == null) {
                    taskHeap.poll();
                    continue;
                }

                if (headTask.getCancelled().get()) {
                    taskHeap.poll();
                    cleanupTask(headTask);
                    continue;
                }

                long waitNanos = headTask.getRunAt() - System.nanoTime();

                if (waitNanos > 0) {
                    timerSignal.awaitNanos(waitNanos);
                    continue;
                }

                taskHeap.poll();

                readyQueue.offer(headTask);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    private void cleanupTask(Task task) {
        if (!(task instanceof PeriodicTask) || task
                .getCancelled()
                .get() || !accepting.get()) {
            cancels.remove(task.getId());
            return;
        }
        if ((((PeriodicTask) task).isPeriodicCancelled())) {
            cancels.remove(task.getId());
        }
    }

    static void main() throws ExecutionException, InterruptedException {
        Scheduler scheduler = new Scheduler(1);
        scheduler.start();
        CompletableFuture<Integer> cf = scheduler.submit(() -> 20 + 10);
        scheduler.scheduleAt(
                () -> IO.println("hello World"),
                LocalDateTime.now().plusMinutes(1)
        );
        IO.println(cf.get());

        TaskHandle handle = scheduler.scheduleFixedRate(
                Duration.ofMillis(300),
                Duration.ofMillis(200),
                () -> IO.println("tick " + System.currentTimeMillis())
        );

        Thread cancelThread = new Thread(() -> scheduler.cancel(handle));

        Thread.sleep(2000);
        cancelThread.start();
        cancelThread.join();

        Runtime
                .getRuntime()
                .addShutdownHook(new Thread(() -> {
                    try {
                        scheduler.shutdown();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }
}
