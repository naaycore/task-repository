package com.prodigious.Scheduler;


import lombok.Getter;

import java.util.concurrent.atomic.AtomicBoolean;

@Getter
public class PeriodicTask extends Task {
    private final long periodNano;
    private final PeriodMode periodMode;

    public PeriodicTask(
            long id,
            Runnable runnable,
            long runAt,
            long periodNano,
            PeriodMode periodMode,
            AtomicBoolean cancelled
    ) {
        super(id, runnable, runAt, cancelled);

        this.periodNano = periodNano;
        this.periodMode = periodMode;
    }

}
