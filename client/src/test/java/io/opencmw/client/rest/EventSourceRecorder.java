package io.opencmw.client.rest;

import static org.junit.jupiter.api.Assertions.*;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.jetbrains.annotations.NotNull;

import okhttp3.Response;
import okhttp3.internal.platform.Platform;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;

class EventSourceRecorder extends EventSourceListener {
    private final BlockingQueue<Object> events = new LinkedBlockingDeque<>();

    @Override
    public void onOpen(@NotNull EventSource eventSource, @NotNull Response response) {
        Platform.get().log("[ES] onOpen", Platform.INFO, null);
        events.add(new Open(eventSource, response));
    }

    @Override
    public void onEvent(@NotNull EventSource eventSource, String id, String type,@NotNull  String data) {
        Platform.get().log("[ES] onEvent", Platform.INFO, null);
        events.add(new Event(id, type, data));
    }

    @Override
    public void onClosed(@NotNull EventSource eventSource) {
        Platform.get().log("[ES] onClosed", Platform.INFO, null);
        events.add(new Closed());
    }

    @Override
    public void onFailure(@NotNull EventSource eventSource, Throwable t, Response response) {
        Platform.get().log("[ES] onFailure", Platform.INFO, t);
        events.add(new Failure(t, response));
    }

    private Object nextEvent() {
        try {
            Object event = events.poll(10, SECONDS);
            if (event == null) {
                throw new AssertionError("Timed out waiting for event.");
            }
            return event;
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    public void assertExhausted() {
        assertTrue(events.isEmpty());
    }

    public void assertEvent(String id, String type, String data) {
        Object actual = nextEvent();
        assertEquals(new Event(id, type, data), actual);
    }

    public EventSource assertOpen() {
        Object event = nextEvent();
        if (!(event instanceof Open)) {
            throw new AssertionError("Expected Open but was " + event);
        }
        return ((Open) event).eventSource;
    }

    public void assertClose() {
        Object event = nextEvent();
        if (!(event instanceof Closed)) {
            throw new AssertionError("Expected Open but was " + event);
        }
    }

    public void assertFailure(String message) {
        Object event = nextEvent();
        if (!(event instanceof Failure)) {
            throw new AssertionError("Expected Failure but was " + event);
        }
        if (message != null) {
            assertEquals(message, ((Failure) event).t.getMessage());
        } else {
            assertNull(((Failure) event).t);
        }
    }

    static final class Open {
        final EventSource eventSource;
        final Response response;

        Open(EventSource eventSource, Response response) {
            this.eventSource = eventSource;
            this.response = response;
        }

        @Override
        public String toString() {
            return "Open[" + response + ']';
        }
    }

    static final class Failure {
        final Throwable t;
        final Response response;
        final String responseBody;

        Failure(Throwable t, Response response) {
            this.t = t;
            this.response = response;
            String responseBody = null;
            if (response != null) {
                try {
                    responseBody = response.body().string();
                } catch (IOException ignored) {
                }
            }
            this.responseBody = responseBody;
        }

        @Override
        public String toString() {
            if (response == null) {
                return "Failure[" + t + "]";
            }
            return "Failure[" + response + "]";
        }
    }

    static final class Closed {
        @Override
        public String toString() {
            return "Closed[]";
        }
    }
}
