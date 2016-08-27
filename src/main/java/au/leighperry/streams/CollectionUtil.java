package au.leighperry.streams;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CollectionUtil {
    /** Wrap an iterator in a stream */
    public static <T> Stream<T> streamOf(final Iterator<? extends T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    /** Wrap an iterable in a stream */
    public static <T> Stream<T> streamOf(final Iterable<? extends T> iterable) {
        return streamOf(iterable.iterator());
    }
}
