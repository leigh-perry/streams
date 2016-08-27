package au.leighperry.streams;

import akka.NotUsed;
import akka.stream.javadsl.Source;

/**
 * Akka-stream that only emits events that are explicitly created via insert() method. Useful
 * for testing streams and other use cases such as trigger conditions.
 */
public class ManualEventStream<T> {
    private StreamsBufferedSubscription<T> subscription;

    public Source<T, NotUsed> observe() {
        return Source.fromPublisher(
            subscriber -> {
                subscription = new StreamsBufferedSubscription<>(subscriber);
                subscriber.onSubscribe(subscription);
            }
        );
    }

    @SafeVarargs
    public final void insert(final T... event) {
        for (final T e : event) {
            subscription.offer(e);
        }
    }
}

