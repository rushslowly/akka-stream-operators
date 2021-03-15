package ch.rslowly.akka.stream.operators

import akka.event.Logging
import akka.stream.stage._
import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

final case class InvalidationBuffer[T](size: Int, fnc: T => Boolean)
  extends GraphStage[UniformFanOutShape[T, T]] {

  private val in = Inlet[T](Logging.simpleName(this) + ".in")
  private val out = Outlet[T](Logging.simpleName(this) + ".out")
  private val outInvalidated = Outlet[T](Logging.simpleName(this) + ".outInvalidated")

  override def createLogic(inheritedAttributes: Attributes): TimerGraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with StageLogging {
      override protected def logSource: Class[_] = classOf[InvalidationBuffer[_]]

      var buffer: ArrayBuffer[T] = _
      var bufferInvalidated: ArrayBuffer[T] = _

      override def preStart(): Unit = {
        buffer = ArrayBuffer.empty
        bufferInvalidated = ArrayBuffer.empty

        // start timer
        // TODO: make it configurable
        val initialDelay = 1.seconds
        val interval = 1.seconds
        scheduleWithFixedDelay(None, initialDelay, interval)

        pull(in)
      }

      override def onPush(): Unit = {
        val elem = grab(in)
        // If out is available, then it has been pulled but no dequeued element has been delivered.
        // It means the buffer at this moment is definitely empty, so we just push the current element to out, then pull.
        if (isAvailable(out)) {
          push(out, elem)
          pull(in)
        } else {
          enqueue(elem)
        }
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (buffer.nonEmpty) {
            val elem = buffer.head
            buffer = buffer.drop(1)
            push(out, elem)
          }
        }

        if (isClosed(in)) {
          if (buffer.isEmpty) completeStage()
        } else if (!hasBeenPulled(in)) {
          pull(in)
        }
      })

      setHandler(outInvalidated, new OutHandler {
        override def onPull(): Unit = {
          if (bufferInvalidated.nonEmpty) {
            val elem = bufferInvalidated.head
            bufferInvalidated = bufferInvalidated.drop(1)
            push(outInvalidated, elem)
          }
        }
      })

      private def enqueue: T => Unit = {
        elem =>
          buffer += elem
          if (buffer.size < size) pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (buffer.isEmpty) completeStage()
      }

      override protected def onTimer(timerKey: Any): Unit = {
        // possibly too expensive
        val partitionBuffer = buffer.partition(elem => fnc(elem))
        buffer = partitionBuffer._1
        bufferInvalidated = bufferInvalidated.appendedAll(partitionBuffer._2)
      }

      setHandler(in, this)
    }

  override def shape: UniformFanOutShape[T, T] = UniformFanOutShape(in, out, outInvalidated)
}
