package io.opencmw.utils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CustomFuture<T> implements Future<T> {
    private static final String FUTURE_HAS_BEEN_CANCELLED = "future has been cancelled";
    protected final Lock lock = new ReentrantLock();
    protected final Condition processorNotifyCondition = lock.newCondition();
    protected final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicBoolean requestCancel = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private T reply;
    private Throwable exception;

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        if (done.getAndSet(true)) {
            return false;
        }
        cancelled.set(true);
        notifyListener();
        return !requestCancel.getAndSet(true);
    }

    @Override
    public T get() throws ExecutionException, InterruptedException {
        try {
            return get(0, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            // cannot normally occur -- need this because we re-use 'get(...)' to avoid code duplication
            throw new ExecutionException("TimeoutException should not occur here", e);
        }
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public T get(final long timeout, final TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        if (cancelled.get()) {
            throw new CancellationException(FUTURE_HAS_BEEN_CANCELLED);
        }

        if (isDone()) {
            if (exception == null) {
                return reply;
            }
            throw new ExecutionException(exception);
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
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return reply;
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public boolean isDone() {
        return done.get();
    }

    /**
     * set reply and notify potential listeners
     * @param newValue the new value to be notified
     * @throws IllegalStateException in case this method has been already called or Future has been cancelled
     */
    public void setReply(final T newValue) {
        if (done.getAndSet(true)) {
            throw new IllegalStateException("future is not running anymore (either cancelled or already notified)");
        }
        this.reply = newValue;
        notifyListener();
    }

    public void setException(final Throwable exception) {
        if (done.getAndSet(true)) {
            throw new IllegalStateException("future is not running anymore (either cancelled or already notified)");
        }
        this.exception = exception;
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