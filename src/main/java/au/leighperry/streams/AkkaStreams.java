package au.leighperry.streams;

import akka.NotUsed;
import akka.japi.Pair;
import akka.japi.function.Function2;
import akka.japi.function.Function3;
import akka.japi.function.Function4;
import akka.japi.function.Function5;
import akka.japi.function.Function6;
import akka.japi.function.Function7;
import akka.japi.function.Function8;
import akka.japi.function.Function9;
import akka.stream.javadsl.Source;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

@SuppressWarnings( { "unchecked", "cast" })
public class AkkaStreams {

    /** Missing from akka.japi.function */
    public interface FunctionN<R> {
        R apply(Object... args);
    }

    /**
     * Compensate for missing operator from Akka Streams. Some ugliness working around lack of combineLatest() operator:
     * <p>
     * - merge the content streams into one stream, tagging each data element with an integer index
     * - combine the merged stream by scanning along it accumulating an Object[] result
     */
    public static <T, R> Source<R, NotUsed> combineLatest(
        final Collection<? extends Source<? extends T, NotUsed>> sources,
        final FunctionN<? extends R> combineFunction
    ) {
        final int size = sources.size();
        final int[] i = { 0 };

        // Reduce: merge the content streams into one stream, tagging each data element with an integer.
        final Source<? extends Pair<Integer, ? extends T>, NotUsed> merged =
            CollectionUtil.streamOf(sources)
                .map(source -> {
                    int index = i[0]++;
                    return source.map(e -> Pair.create(index, e));
                })
                .reduce(Source::merge)
                .get();     // Optional always exists

        // Scan: combine the merged stream by scanning along it accumulating an array of objects
        // from each stream and then combining to a R result
        return merged
            .scan(new Object[size], AkkaStreams::aggregate)
            .filter(AkkaStreams::areAllAvailable)
            .map(combineFunction::apply);
    }

    public static <T0, T1, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Function2<? super T0, ? super T1, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1), toFunctionN(combineFunction));
    }

    public static <T0, T1, T2, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Source<? extends T2, NotUsed> s2,
        final Function3<? super T0, ? super T1, ? super T2, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1, s2), toFunctionN(combineFunction));
    }

    public static <T0, T1, T2, T3, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Source<? extends T2, NotUsed> s2,
        final Source<? extends T3, NotUsed> s3,
        final Function4<? super T0, ? super T1, ? super T2, ? super T3, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1, s2, s3), toFunctionN(combineFunction));
    }

    public static <T0, T1, T2, T3, T4, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Source<? extends T2, NotUsed> s2,
        final Source<? extends T3, NotUsed> s3,
        final Source<? extends T4, NotUsed> s4,
        final Function5<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1, s2, s3, s4), toFunctionN(combineFunction));
    }

    public static <T0, T1, T2, T3, T4, T5, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Source<? extends T2, NotUsed> s2,
        final Source<? extends T3, NotUsed> s3,
        final Source<? extends T4, NotUsed> s4,
        final Source<? extends T5, NotUsed> s5,
        final Function6<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1, s2, s3, s4, s5), toFunctionN(combineFunction));
    }

    public static <T0, T1, T2, T3, T4, T5, T6, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Source<? extends T2, NotUsed> s2,
        final Source<? extends T3, NotUsed> s3,
        final Source<? extends T4, NotUsed> s4,
        final Source<? extends T5, NotUsed> s5,
        final Source<? extends T6, NotUsed> s6,
        final Function7<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1, s2, s3, s4, s5, s6), toFunctionN(combineFunction));
    }

    public static <T0, T1, T2, T3, T4, T5, T6, T7, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Source<? extends T2, NotUsed> s2,
        final Source<? extends T3, NotUsed> s3,
        final Source<? extends T4, NotUsed> s4,
        final Source<? extends T5, NotUsed> s5,
        final Source<? extends T6, NotUsed> s6,
        final Source<? extends T7, NotUsed> s7,
        final Function8<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? super T7, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1, s2, s3, s4, s5, s6, s7), toFunctionN(combineFunction));
    }

    public static <T0, T1, T2, T3, T4, T5, T6, T7, T8, R> Source<R, NotUsed> combineLatest(
        final Source<? extends T0, NotUsed> s0,
        final Source<? extends T1, NotUsed> s1,
        final Source<? extends T2, NotUsed> s2,
        final Source<? extends T3, NotUsed> s3,
        final Source<? extends T4, NotUsed> s4,
        final Source<? extends T5, NotUsed> s5,
        final Source<? extends T6, NotUsed> s6,
        final Source<? extends T7, NotUsed> s7,
        final Source<? extends T8, NotUsed> s8,
        final Function9<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? super T7, ? super T8, ? extends R> combineFunction
    ) {
        return (Source<R, NotUsed>) combineLatest(asList(s0, s1, s2, s3, s4, s5, s6, s7, s8), toFunctionN(combineFunction));
    }

    public static <T0, T1, R> FunctionN<R> toFunctionN(final Function2<? super T0, ? super T1, ? extends R> f) {
        return args -> {
            if (args.length != 2) {
                throw new IllegalArgumentException("Function2 expecting 2 arguments, received " + args.length);
            }
            try {
                return f.apply((T0) args[0], (T1) args[1]);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T0, T1, T2, R> FunctionN<R> toFunctionN(final Function3<? super T0, ? super T1, ? super T2, ? extends R> f) {
        return args -> {
            if (args.length != 3) {
                throw new IllegalArgumentException("Function3 expecting 3 arguments, received " + args.length);
            }
            try {
                return f.apply((T0) args[0], (T1) args[1], (T2) args[2]);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T0, T1, T2, T3, R> FunctionN<R> toFunctionN(
        final Function4<? super T0, ? super T1, ? super T2, ? super T3, ? extends R> f
    ) {
        return args -> {
            if (args.length != 4) {
                throw new IllegalArgumentException("Function4 expecting 4 arguments, received " + args.length);
            }
            try {
                return f.apply((T0) args[0], (T1) args[1], (T2) args[2], (T3) args[3]);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T0, T1, T2, T3, T4, R> FunctionN<R> toFunctionN(
        final Function5<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? extends R> f
    ) {
        return args -> {
            if (args.length != 5) {
                throw new IllegalArgumentException("Function5 expecting 5 arguments, received " + args.length);
            }
            try {
                return f.apply((T0) args[0], (T1) args[1], (T2) args[2], (T3) args[3], (T4) args[4]);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T0, T1, T2, T3, T4, T5, R> FunctionN<R> toFunctionN(
        final Function6<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? extends R> f
    ) {
        return args -> {
            if (args.length != 6) {
                throw new IllegalArgumentException("Function6 expecting 6 arguments, received " + args.length);
            }
            try {
                return f.apply((T0) args[0], (T1) args[1], (T2) args[2], (T3) args[3], (T4) args[4], (T5) args[5]);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T0, T1, T2, T3, T4, T5, T6, R> FunctionN<R> toFunctionN(
        final Function7<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? extends R> f
    ) {
        return args -> {
            if (args.length != 7) {
                throw new IllegalArgumentException("Function7 expecting 7 arguments, received " + args.length);
            }
            try {
                return f.apply(
                    (T0) args[0],
                    (T1) args[1],
                    (T2) args[2],
                    (T3) args[3],
                    (T4) args[4],
                    (T5) args[5],
                    (T6) args[6]
                );
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T0, T1, T2, T3, T4, T5, T6, T7, R> FunctionN<R> toFunctionN(
        final Function8<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? super T7, ? extends R> f
    ) {
        return args -> {
            if (args.length != 8) {
                throw new IllegalArgumentException("Function8 expecting 8 arguments, received " + args.length);
            }
            try {
                return f.apply(
                    (T0) args[0],
                    (T1) args[1],
                    (T2) args[2],
                    (T3) args[3],
                    (T4) args[4],
                    (T5) args[5],
                    (T6) args[6],
                    (T7) args[7]
                );
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T0, T1, T2, T3, T4, T5, T6, T7, T8, R> FunctionN<R> toFunctionN(
        final Function9<? super T0, ? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? super T7, ? super T8, ? extends R> f
    ) {
        return args -> {
            if (args.length != 9) {
                throw new IllegalArgumentException("Function9 expecting 9 arguments, received " + args.length);
            }
            try {
                return f.apply(
                    (T0) args[0],
                    (T1) args[1],
                    (T2) args[2],
                    (T3) args[3],
                    (T4) args[4],
                    (T5) args[5],
                    (T6) args[6],
                    (T7) args[7],
                    (T8) args[8]
                );
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static <T> Object[] aggregate(final Object[] previousState, final Pair<Integer, ? extends T> p) {
        final int streamId = p.first();
        final Object[] newState = Arrays.copyOf(previousState, previousState.length);
        newState[streamId] = p.second();

        return newState;
    }

    private static boolean areAllAvailable(final Object[] objects) {
        return Stream.of(objects)
            .allMatch(o -> o != null);
    }
}
