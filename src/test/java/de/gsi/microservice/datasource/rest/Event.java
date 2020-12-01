package de.gsi.microservice.datasource.rest;

import java.util.Objects;

public class Event {
    private final String id;
    private final String type;
    private final String data;

    public Event(final String id, final String type, final String data) {
        if (data == null) {
            throw new IllegalArgumentException("data == null");
        }
        this.id = id;
        this.type = type;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Event{id='" + id + "', type='" + type + "', data='" + data + "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Event))
            return false;
        Event other = (Event) o;
        return Objects.equals(id, other.id) && Objects.equals(type, other.type) && data.equals(other.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(id);
        result = 31 * result + Objects.hashCode(type);
        result = 31 * result + data.hashCode();
        return result;
    }
}
