package aus.leighperry.streams

import akka.NotUsed
import akka.stream.scaladsl.Source

object AkkaStreams {
    type FunctionN[R] = Array[Any] => R

    /**
      * Compensate for missing operator from Akka Streams. Some ugliness working around lack of combineLatest() operator:
      * <p>
      * - merge the content streams into one stream, tagging each data element with an integer index
      * - combine the merged stream by scanning along it accumulating an Object[] result
      */
    def combineLatest[T, R](sources: Seq[Source[T, NotUsed]], combineFunction: FunctionN[R]): Source[R, NotUsed] = {
        val size: Int = sources.size

        // Reduce: merge the content streams into one stream, tagging each data element with an integer.
        var i: Int = 0
        val merged: Source[Tuple2[Int, T], NotUsed] =
            sources
                .map(
                    source => {
                        val mapped: Source[(Int, T), NotUsed] = source.map(e => Tuple2(i, e))
                        i = i + 1
                        mapped
                    }
                )
                .reduce(_ merge _)

        // Scan: combine the merged stream by scanning along it accumulating an array of objects
        // from each stream and then combining to a R result
        merged
            .scan(new Array[Any](size))(aggregate(_, _))
            .filter(areAllAvailable)
            .map(combineFunction.apply)
    }

    def combineLatest[T0, T1, R](
        s0: Source[T0, NotUsed],
        s1: Source[T1, NotUsed],
        combineFunction: Function2[T0, T1, R]
    ): Source[R, NotUsed] = {
        combineLatest(List(s0, s1), toFunctionN(combineFunction))
    }

    def toFunctionN[T0, T1, R](f: Function2[T0, T1, R]): FunctionN[R] =
        args => {
            if (args.length != 2) {
                throw new IllegalArgumentException("Function2 expecting 2 arguments, received " + args.length)
            }
            try
                f.apply(args(0).asInstanceOf[T0], args(1).asInstanceOf[T1])
            catch {
                case e: Exception => throw new RuntimeException(e)
            }
        }

    private def aggregate[T](previousState: Array[Any], p: Tuple2[Int, T]): Array[Any] = {
        val newState: Array[Any] = previousState.clone()
        newState(p._1) = p._2;
        newState
    }

    private def areAllAvailable(objects: Array[Any]): Boolean = !objects.exists(_ == null)
}
