package com.prodigious.Scheduler;


import lombok.Getter;

import java.util.concurrent.atomic.AtomicBoolean;

@Getter
public class PeriodicTask extends Task {
    private final long periodNano;
    private final PeriodMode periodMode;
    private boolean periodicCancelled;

    public PeriodicTask(
            long id,
            Runnable runnable,
            long runAt,
            long periodNano,
            PeriodMode periodMode,
            AtomicBoolean cancelled,
            boolean periodicCancelled
    ) {
        super(id, runnable, runAt, cancelled);

        this.periodNano = periodNano;
        this.periodMode = periodMode;
        this.periodicCancelled = periodicCancelled;
    }

}
