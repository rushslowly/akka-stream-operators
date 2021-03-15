package ch.rslowly.akka.stream.operators

import akka.stream.scaladsl.{Flow, GraphDSL, Merge, MergePreferred, Partition, Sink}
import akka.stream.{FlowShape, Graph}
import akka.{Done, NotUsed}

import scala.concurrent.Future

// early proof of concept!
object PrioritizedInvalidationBuffer {
  def apply[T](
    bufferSize: Int,
    partitionFunc: T => Int,
    invalidationPred: T => Boolean,
    invalidationSink: Sink[T, Future[Done]]): Graph[FlowShape[T, T], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements
      val invalidationSink = b.add(invalidationSink)
      val partition = b.add(Partition[T](6, partitionFunc))
      val invalidationMerge = b.add(Merge(6))

      val buffer0 = b.add(InvalidationBuffer(bufferSize, invalidationPred))
      val buffer1 = b.add(InvalidationBuffer(bufferSize, invalidationPred))
      val buffer2 = b.add(InvalidationBuffer(bufferSize, invalidationPred))
      val buffer3 = b.add(InvalidationBuffer(bufferSize, invalidationPred))
      val buffer4 = b.add(InvalidationBuffer(bufferSize, invalidationPred))
      val buffer5 = b.add(InvalidationBuffer(bufferSize, invalidationPred))

      val prio0To1 = b.add(MergePreferred(1))
      val prio2To3 = b.add(MergePreferred(1))
      val prio4To5 = b.add(MergePreferred(1))
      val prio01To23 = b.add(MergePreferred(1))
      val prio01To23To45 = b.add(MergePreferred(1))

      // connect the graph
      // format: off
      partition.out(0) ~> buffer0
      partition.out(1) ~> buffer1
      partition.out(2) ~> buffer2
      partition.out(3) ~> buffer3
      partition.out(4) ~> buffer4
      partition.out(5) ~> buffer5

      buffer0.out(0) ~> prio0To1.preferred
      buffer1.out(0) ~> prio0To1.in(0)
      buffer2.out(0) ~> prio2To3.preferred
      buffer3.out(0) ~> prio2To3.in(0)
      buffer4.out(0) ~> prio4To5.preferred
      buffer5.out(0) ~> prio4To5.in(0)

      prio0To1.out ~> prio01To23.preferred
      prio2To3.out ~> prio01To23.in(0)
      prio01To23.out ~> prio01To23To45.preferred
      prio4To5.out ~> prio01To23To45.in(0)

      buffer0.out(1) ~> invalidationMerge
      buffer1.out(1) ~> invalidationMerge
      buffer2.out(1) ~> invalidationMerge
      buffer3.out(1) ~> invalidationMerge
      buffer4.out(1) ~> invalidationMerge
      buffer5.out(1) ~> invalidationMerge

      invalidationMerge.out ~> invalidationSink
      // format: on

      // expose ports
      FlowShape(partition.in, prio01To23To45.out)
    })
}