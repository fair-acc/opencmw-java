package io.opencmw.utils;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

import java.net.ProtocolException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

class CustomFutureTests {
    @Test
    void testWithoutWaiting() throws ExecutionException, InterruptedException {
        final CustomFuture<String> future = new CustomFuture<>();

        assertFalse(future.done.get(), "future is active");
        assertFalse(future.isCancelled());
        future.setReply("TestString");

        assertEquals("TestString", future.get());
        assertFalse(future.isCancelled());
    }

    @Test
    void testWithWaiting() {
        final CustomFuture<String> future = new CustomFuture<>();
        assertFalse(future.done.get(), "future is active");
        assertFalse(future.isCancelled());

        final AtomicReference<String> result = new AtomicReference<>();
        final AtomicBoolean run = new AtomicBoolean(false);
        new Thread(() -> {
            run.set(true);
            try {
                result.set(future.get());
                assertEquals("TestString", future.get());
                assertEquals("TestString", result.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException("unexpected exception", e);
            }
            run.set(false);
        }).start();
        await().alias("wait for thread to start").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(true));
        future.setReply("TestString");
        await().alias("wait for thread to finish").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(false));

        assertEquals("TestString", result.get());
        assertFalse(future.isCancelled());
    }

    @Test
    void testWithExecutionException() {
        final CustomFuture<String> future = new CustomFuture<>();
        assertFalse(future.done.get(), "future is active");
        assertFalse(future.isCancelled());
        future.setException(new ProtocolException("specific exception"));

        assertThrows(ExecutionException.class, future::get);

        assertThrows(IllegalStateException.class, () -> future.setException(new ProtocolException("specific exception")));
    }

    @Test
    void testWithExecutionExceptionWhileWaiting() {
        final CustomFuture<String> future = new CustomFuture<>();
        assertFalse(future.done.get(), "future is active");

        new Thread(() -> {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            future.setException(new ProtocolException("specific exception"));
        }).start();
        assertThrows(ExecutionException.class, () -> future.get(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    void testWithCancelWhileWaiting() {
        final CustomFuture<String> future = new CustomFuture<>();
        assertFalse(future.done.get(), "future is active");
        assertFalse(future.isCancelled());

        final AtomicReference<String> result = new AtomicReference<>();
        final AtomicBoolean run = new AtomicBoolean(false);
        new Thread(() -> {
            run.set(true);
            assertThrows(CancellationException.class, () -> result.set(future.get()));
            run.set(false);
        }).start();
        await().alias("wait for thread to start").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(true));
        assertFalse(future.isCancelled());
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
        assertTrue(future.cancel(true));
        assertFalse(future.cancel(true));
        assertTrue(future.isCancelled());
        await().alias("wait for thread to finish").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(false));
        assertThrows(IllegalStateException.class, () -> future.setReply("TestString"));

        assertNull(result.get());
    }

    @Test
    void testWithTimeout() {
        final CustomFuture<String> future = new CustomFuture<>();
        assertFalse(future.done.get(), "future is active");
        assertThrows(TimeoutException.class, () -> future.get(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void testWithTimeoutAndCancel() {
        final CustomFuture<String> future = new CustomFuture<>();
        final AtomicBoolean run = new AtomicBoolean(false);
        final Thread testThread = new Thread(() -> {
            run.set(true);
            assertThrows(CancellationException.class, () -> future.get(1, TimeUnit.SECONDS));
            run.set(false);
        });
        testThread.start();
        await().alias("wait for thread to start").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(true));
        future.cancel(false);
        await().alias("wait for thread to finish").atMost(10, TimeUnit.SECONDS).until(run::get, equalTo(false));
    }

    @Test
    void testWithCancelBeforeWaiting() {
        final CustomFuture<String> future = new CustomFuture<>();
        assertFalse(future.done.get(), "future is active");
        assertFalse(future.isCancelled());
        assertTrue(future.cancel(true));
        assertFalse(future.cancel(true));

        final AtomicReference<String> result = new AtomicReference<>();
        final AtomicBoolean run = new AtomicBoolean(true);
        new Thread(() -> {
            assertThrows(CancellationException.class, () -> result.set(future.get()));
            run.set(false);
        }).start();
        assertTrue(future.isCancelled());
        assertThrows(IllegalStateException.class, () -> future.setReply("TestString"));
        await().alias("wait for thread to finish").atMost(1, TimeUnit.SECONDS).until(run::get, equalTo(false));

        assertNull(result.get());
    }

    @Test
    void testWithNullReply() throws ExecutionException, InterruptedException {
        final CustomFuture<String> future = new CustomFuture<>();
        assertFalse(future.done.get(), "future is active");
        future.setReply(null);
        assertNull(future.get());
    }
}
