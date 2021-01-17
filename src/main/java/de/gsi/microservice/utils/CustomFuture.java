package de.gsi.microservice.utils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jetbrains.annotations.NotNull;

public class CustomFuture<T> implements Future<T> {
    private static final String FUTURE_HAS_BEEN_CANCELLED = "future has been cancelled";
    protected final Lock lock = new ReentrantLock();
    protected final Condition processorNotifyCondition = lock.newCondition();
    protected final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean requestCancel = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private T reply = null;

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        cancelled.set(true);
        if (running.getAndSet(false)) {
            notifyListener();
            return !requestCancel.getAndSet(true);
        }
        return false;
    }

    @Override
    public T get() throws ExecutionException, InterruptedException {
        try {
            return get(0, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new ExecutionException("TimeoutException should not occur here", e);
        }
    }

    @Override
    public T get(final long timeout, @NotNull final TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        if (cancelled.get()) {
            throw new CancellationException(FUTURE_HAS_BEEN_CANCELLED);
        }
        if (isDone()) {
            return reply;
        }
        lock.lock();
        try {
            while (!isDone()) {
                if (timeout > 0) {
                    if (!processorNotifyCondition.await(timeout, unit)) {
                        throw new TimeoutException();
                    }
                } else {
                    processorNotifyCondition.await();
                }
                if (cancelled.get()) {
                    throw new CancellationException(FUTURE_HAS_BEEN_CANCELLED);
                }
            }
        } finally {
            lock.unlock();
        }
        return reply;
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public boolean isDone() {
        return (reply != null && !running.get());
    }

    /**
     * set reply and notify potential listeners
     * @param newValue the new value to be notified
     * @throws IllegalStateException in case this method has been already called or Future has been cancelled
     */
    public void setReply(final T newValue) {
        if (running.getAndSet(false)) {
            this.reply = newValue;
        } else {
            throw new IllegalStateException("future is not running anymore (either cancelled or already notified)");
        }
        notifyListener();
    }

    private void notifyListener() {
        lock.lock();
        try {
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}