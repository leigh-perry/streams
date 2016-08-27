package au.leighperry.streams;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer that holds an unbounded queue to enqueue values and relays them
 * to a subscriber subscriber on request.
 * <p>
 * Implementation extended from rxjava's internal QueuedValueProducer class
 *
 * @param <T> the value type
 */
public class StreamsBufferedSubscription<T> implements Subscription {
    final Subscriber<? super T> subscriber;
    final Queue<T> queue = new ManyToOneConcurrentLinkedQueue<>();

    final AtomicLong requestCounter = new AtomicLong();
    final AtomicInteger publishInProgressCount = new AtomicInteger();
    private volatile boolean unsubscribed;

    public StreamsBufferedSubscription(final Subscriber<? super T> subscriber) {
        this.subscriber = subscriber;
    }

    @Override
    public void request(final long n) {
        if (n < 0) {
            throw new IllegalArgumentException("n >= 0 required");
        }
        if (n > 0) {
            getAndAddRequest(requestCounter, n);
            publishFromQueue();
        }
    }

    @Override
    public void cancel() {
        unsubscribed = true;
    }

    public long getRequestCount() {
        return requestCounter.get();
    }

    /**
     * Offers a value to this producer and tries to emit any queued values
     * if the subscriber requests allow it.
     *
     * @param value the value to enqueue and attempt to publish
     * @return true if the queue accepted the offer, false otherwise
     */
    public boolean offer(final T value) {
        if (!queue.offer(value)) {
            return false;
        }

        publishFromQueue();
        return true;
    }

    private void publishFromQueue() {
        if (publishInProgressCount.getAndIncrement() == 0) {
            // Won right to publish from queue
            final Subscriber<? super T> c = subscriber;
            final Queue<T> q = queue;
            do {
                if (unsubscribed) {
                    return;
                }

                // Detection mechanism for concurrent publishFromQueue attempt during this loop, ie offer() called
                publishInProgressCount.lazySet(1);

                // Publish until request count exhausted or queue is empty
                long requestCount = requestCounter.get();
                long publishedCount = 0;
                T v;
                while (requestCount != 0 && (v = q.poll()) != null) {
                    try {
                        c.onNext(v);
                    } catch (final Throwable ex) {
                        subscriber.onError(ex);
                        return;
                    }

                    if (unsubscribed) {
                        return;
                    }

                    requestCount--;
                    publishedCount++;
                }

                if (publishedCount != 0 && requestCounter.get() != Long.MAX_VALUE) {
                    requestCounter.addAndGet(-publishedCount);
                }
            } while (publishInProgressCount.decrementAndGet() != 0);
        }
    }

    public boolean isSubscribed() {
        return !unsubscribed;
    }

    /**
     * Adds {@code n} (not validated) to {@code requested} and returns the value prior to addition once the
     * addition is successful (uses CAS semantics). If overflows then sets
     * {@code requested} field to {@code Long.MAX_VALUE}.
     *
     * @param requested atomic long that should be updated
     * @param n         the number of requests to add to the requested count, positive (not validated)
     * @return requested value just prior to successful addition
     */
    public static long getAndAddRequest(final AtomicLong requested, final long n) {
        // add n to field but check for overflow
        while (true) {
            final long current = requested.get();
            final long next = addCap(current, n);
            if (requested.compareAndSet(current, next)) {
                return current;
            }
        }
    }

    /**
     * Adds two positive longs and caps the result at Long.MAX_VALUE.
     *
     * @param a the first value
     * @param b the second value
     * @return the capped sum of a and b
     */
    public static long addCap(final long a, final long b) {
        long u = a + b;
        if (u < 0L) {
            u = Long.MAX_VALUE;
        }
        return u;
    }
}

