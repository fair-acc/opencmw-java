package io.opencmw.serialiser.utils;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.AbstractCollection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import de.gsi.dataset.utils.ByteArrayCache;

/**
 * Implements collection of cache-able objects that can be used to store recurring storage container.
 * <p>
 * N.B. this implements only the backing cache of adding, removing, etc. elements. The cache object retrieval should be implemented in the derived class.
 * See for example {@link ByteArrayCache}.
 * 
 * @author rstein
 *
 * @param <T> generic for object type to be cached.
 */
public class CacheCollection<T> extends AbstractCollection<T> {
    protected final List<Reference<T>> contents = Collections.synchronizedList(new LinkedList<>());

    @Override
    public boolean add(T recoveredObject) {
        if (recoveredObject != null) {
            synchronized (contents) {
                if (contains(recoveredObject)) {
                    return false;
                }
                // N.B. here: specific choice of using 'SoftReference'
                // derived classes may overwrite this function and replace this with e.g. WeakReference or similar
                return contents.add(new SoftReference<>(recoveredObject));
            }
        }
        return false;
    }

    @Override
    public void clear() {
        synchronized (contents) {
            contents.clear();
        }
    }

    @Override
    public boolean contains(Object object) {
        if (object != null) {
            synchronized (contents) {
                for (Reference<T> weakReference : contents) {
                    if (object.equals(weakReference.get())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        synchronized (contents) {
            return new CacheCollectionIterator<>(contents.iterator());
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }
        synchronized (contents) {
            Iterator<Reference<T>> iter = contents.iterator();
            while (iter.hasNext()) {
                final Reference<T> candidate = iter.next();
                final T test = candidate.get();
                if (test == null) {
                    iter.remove();
                    continue;
                }
                if (o.equals(test)) {
                    iter.remove();
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int size() {
        synchronized (contents) {
            cleanup();
            return contents.size();
        }
    }

    protected void cleanup() {
        synchronized (contents) {
            List<Reference<T>> toRemove = new LinkedList<>();
            for (Reference<T> weakReference : contents) {
                if (weakReference.get() == null) {
                    toRemove.add(weakReference);
                }
            }
            contents.removeAll(toRemove);
        }
    }

    private static class CacheCollectionIterator<T> implements Iterator<T> {
        private final Iterator<Reference<T>> iterator;
        private T nextElement;

        private CacheCollectionIterator(Iterator<Reference<T>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            if (nextElement != null) {
                return true;
            }
            while (iterator.hasNext()) {
                T t = iterator.next().get();
                if (t != null) {
                    // to ensure next() can't throw after hasNext() returned true, we need to dereference this
                    nextElement = t;
                    return true;
                }
            }
            return false;
        }

        @Override
        public T next() {
            T result = nextElement;
            nextElement = null; // NOPMD
            while (result == null) {
                result = iterator.next().get();
            }
            return result;
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }
}
