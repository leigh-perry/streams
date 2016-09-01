package au.leighperry.streams;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

import java.sql.Time;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class AkkaStreamsTest {
    private static ActorSystem actorSystem;
    private static ActorMaterializer materializer;

    @BeforeClass
    public static void setup() {
        actorSystem = ActorSystem.create();
        materializer = ActorMaterializer.create(actorSystem);
    }

    @Test
    public void testZipWithN() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            Source.zipWithN(
                list -> String.format("%s:%s:%s", list.get(0), list.get(1), list.get(2)),
                Arrays.asList(s0.observe(), s1.observe(), s2.observe())
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        probe.request(1).expectNext("1:2:3");

        s0.insert(4);
        FiniteDuration waitTime = FiniteDuration.create(250, TimeUnit.MILLISECONDS);
        probe.request(1).expectNoMsg(waitTime);
        s2.insert(5);
        probe.request(1).expectNoMsg(waitTime);

        s1.insert(6);
        probe.expectNext("4:6:5");
    }

    @Test
    public void testCombineLatestN() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                Arrays.asList(s0.observe(), s1.observe(), s2.observe()),
                array -> String.format("%s:%s:%s", array[0], array[1], array[2])
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        probe.request(1).expectNext("1:2:3");

        s0.insert(4);
        probe.request(1).expectNext("4:2:3");
        s2.insert(5);
        probe.request(1).expectNext("4:2:5");

        s1.insert(6);
        s2.insert(7);
        probe.request(2).expectNext("4:6:5", "4:6:7");
    }

    @Test
    public void testCombineLatest2() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                (e0, e1) -> String.format("%s:%s", e0, e1)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        probe.request(1).expectNext("1:2");

        s0.insert(3);
        s1.insert(4);
        probe.request(2).expectNext("3:2", "3:4");

        // these queue up against each stream and emit alternating between streams ie 5,6,7,8,9,10
        s0.insert(5, 7, 8, 9);
        s1.insert(6, 10);
        probe.request(6).expectNext("5:4", "5:6", "7:6", "7:10", "8:10", "9:10");

        s1.insert(11);
        probe.request(1).expectNext("9:11");

        s1.insert(12);
        probe.request(1).expectNext("9:12");

        s1.insert(13);
        probe.request(1).expectNext("9:13");

        s0.insert(14);
        probe.request(1).expectNext("14:13");
    }

    @Test
    public void testCombineLatest3() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                s2.observe(),
                (e0, e1, e2) -> String.format("%s:%s:%s", e0, e1, e2)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        probe.request(1).expectNext("1:2:3");

        s0.insert(4);
        probe.request(1).expectNext("4:2:3");
        s2.insert(5);
        probe.request(1).expectNext("4:2:5");

        s1.insert(6);
        s2.insert(7);
        probe.request(2).expectNext("4:6:5", "4:6:7");
    }

    @Test
    public void testCombineLatest4() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();
        final ManualEventStream<Integer> s3 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                s2.observe(),
                s3.observe(),
                (e0, e1, e2, e3) -> String.format("%s:%s:%s:%s", e0, e1, e2, e3)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        s3.insert(4);
        probe.request(1).expectNext("1:2:3:4");

        s0.insert(5);
        probe.request(1).expectNext("5:2:3:4");
        s2.insert(6);
        probe.request(1).expectNext("5:2:6:4");

        s1.insert(7);
        s3.insert(8, 9);
        probe.request(3).expectNext("5:7:6:4", "5:7:6:8", "5:7:6:9");
    }

    @Test
    public void testCombineLatest5() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();
        final ManualEventStream<Integer> s3 = new ManualEventStream<>();
        final ManualEventStream<Integer> s4 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                s2.observe(),
                s3.observe(),
                s4.observe(),
                (e0, e1, e2, e3, e4) -> String.format("%s:%s:%s:%s:%s", e0, e1, e2, e3, e4)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        s3.insert(4);
        s4.insert(5);
        probe.request(1).expectNext("1:2:3:4:5");

        s0.insert(6);
        probe.request(1).expectNext("6:2:3:4:5");
        s2.insert(7);
        probe.request(1).expectNext("6:2:7:4:5");

        s1.insert(8);
        s4.insert(9, 10);
        probe.request(3).expectNext("6:8:7:4:5", "6:8:7:4:9", "6:8:7:4:10");
    }

    @Test
    public void testCombineLatest6() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();
        final ManualEventStream<Integer> s3 = new ManualEventStream<>();
        final ManualEventStream<Integer> s4 = new ManualEventStream<>();
        final ManualEventStream<Integer> s5 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                s2.observe(),
                s3.observe(),
                s4.observe(),
                s5.observe(),
                (e0, e1, e2, e3, e4, e5) -> String.format("%s:%s:%s:%s:%s:%s", e0, e1, e2, e3, e4, e5)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        s3.insert(4);
        s4.insert(5);
        s5.insert(6);
        probe.request(1).expectNext("1:2:3:4:5:6");

        s0.insert(7);
        probe.request(1).expectNext("7:2:3:4:5:6");
        s2.insert(8);
        probe.request(1).expectNext("7:2:8:4:5:6");

        s4.insert(9, 10);
        probe.request(2).expectNext("7:2:8:4:9:6", "7:2:8:4:10:6");
    }

    @Test
    public void testCombineLatest7() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();
        final ManualEventStream<Integer> s3 = new ManualEventStream<>();
        final ManualEventStream<Integer> s4 = new ManualEventStream<>();
        final ManualEventStream<Integer> s5 = new ManualEventStream<>();
        final ManualEventStream<Integer> s6 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                s2.observe(),
                s3.observe(),
                s4.observe(),
                s5.observe(),
                s6.observe(),
                (e0, e1, e2, e3, e4, e5, e6) -> String.format("%s:%s:%s:%s:%s:%s:%s", e0, e1, e2, e3, e4, e5, e6)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        s3.insert(4);
        s4.insert(5);
        s5.insert(6);
        s6.insert(7);
        probe.request(1).expectNext("1:2:3:4:5:6:7");

        s0.insert(8);
        probe.request(1).expectNext("8:2:3:4:5:6:7");
        s2.insert(9);
        probe.request(1).expectNext("8:2:9:4:5:6:7");

        s6.insert(10, 11);
        probe.request(2).expectNext("8:2:9:4:5:6:10", "8:2:9:4:5:6:11");
    }

    @Test
    public void testCombineLatest8() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();
        final ManualEventStream<Integer> s3 = new ManualEventStream<>();
        final ManualEventStream<Integer> s4 = new ManualEventStream<>();
        final ManualEventStream<Integer> s5 = new ManualEventStream<>();
        final ManualEventStream<Integer> s6 = new ManualEventStream<>();
        final ManualEventStream<Integer> s7 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                s2.observe(),
                s3.observe(),
                s4.observe(),
                s5.observe(),
                s6.observe(),
                s7.observe(),
                (e0, e1, e2, e3, e4, e5, e6, e7) -> String.format("%s:%s:%s:%s:%s:%s:%s:%s", e0, e1, e2, e3, e4, e5, e6, e7)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        s3.insert(4);
        s4.insert(5);
        s5.insert(6);
        s6.insert(7);
        s7.insert(8);
        probe.request(1).expectNext("1:2:3:4:5:6:7:8");

        s0.insert(9);
        probe.request(1).expectNext("9:2:3:4:5:6:7:8");
        s2.insert(10);
        probe.request(1).expectNext("9:2:10:4:5:6:7:8");

        s6.insert(11, 12);
        probe.request(2).expectNext("9:2:10:4:5:6:11:8", "9:2:10:4:5:6:12:8");
    }

    @Test
    public void testCombineLatest9() throws Exception {
        final ManualEventStream<Integer> s0 = new ManualEventStream<>();
        final ManualEventStream<Integer> s1 = new ManualEventStream<>();
        final ManualEventStream<Integer> s2 = new ManualEventStream<>();
        final ManualEventStream<Integer> s3 = new ManualEventStream<>();
        final ManualEventStream<Integer> s4 = new ManualEventStream<>();
        final ManualEventStream<Integer> s5 = new ManualEventStream<>();
        final ManualEventStream<Integer> s6 = new ManualEventStream<>();
        final ManualEventStream<Integer> s7 = new ManualEventStream<>();
        final ManualEventStream<Integer> s8 = new ManualEventStream<>();

        final Source<String, NotUsed> combined =
            AkkaStreams.combineLatest(
                s0.observe(),
                s1.observe(),
                s2.observe(),
                s3.observe(),
                s4.observe(),
                s5.observe(),
                s6.observe(),
                s7.observe(),
                s8.observe(),
                (e0, e1, e2, e3, e4, e5, e6, e7, e8) ->
                    String.format("%s:%s:%s:%s:%s:%s:%s:%s:%s", e0, e1, e2, e3, e4, e5, e6, e7, e8)
            );

        final TestSubscriber.Probe<String> probe =
            combined.runWith(TestSink.probe(actorSystem), materializer);

        s0.insert(1);
        s1.insert(2);
        s2.insert(3);
        s3.insert(4);
        s4.insert(5);
        s5.insert(6);
        s6.insert(7);
        s7.insert(8);
        s8.insert(9);
        probe.request(1).expectNext("1:2:3:4:5:6:7:8:9");

        s0.insert(10);
        probe.request(1).expectNext("10:2:3:4:5:6:7:8:9");
        s2.insert(11);
        probe.request(1).expectNext("10:2:11:4:5:6:7:8:9");

        s8.insert(12, 13);
        probe.request(2).expectNext("10:2:11:4:5:6:7:8:12", "10:2:11:4:5:6:7:8:13");
    }
}
