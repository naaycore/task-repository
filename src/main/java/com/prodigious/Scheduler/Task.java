package com.prodigious.Scheduler;

import java.util.concurrent.atomic.AtomicBoolean;

public class Task implements Comparable<Task> {
    private final Long id;
    private final Runnable runnable;
    private Long runAt;
    private AtomicBoolean cancelled;

    public Task(long id, Runnable runnable, long runAt, AtomicBoolean cancelled) {
        this.id = id;
        this.runnable = runnable;
        this.runAt = runAt;
        this.cancelled = cancelled;
    }

    public Long getId() {
        return id;
    }

    public Runnable getRunnable() {
        return runnable;
    }

    public Long getRunAt() {
        return runAt;
    }

    public void setRunAt(long runAt) {
        this.runAt = runAt;
    }

    public AtomicBoolean getCancelled() {
        return cancelled;
    }

    public void setCancelled(AtomicBoolean cancelled) {
        this.cancelled = cancelled;
    }

    @Override
    public int compareTo(Task other) {
        int c = this.runAt.compareTo(other.runAt);
        if (c != 0) {
            return c;
        }
        return this.id.compareTo(other.id);
    }
}
