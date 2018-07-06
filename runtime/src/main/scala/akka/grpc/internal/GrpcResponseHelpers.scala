/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.grpc.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.grpc.scaladsl.headers
import akka.grpc.{ Codec, Grpc, GrpcServiceException, ProtobufSerializer }
import akka.http.scaladsl.model.HttpEntity.{ ChunkStreamPart, LastChunk }
import akka.http.scaladsl.model.{ HttpEntity, HttpHeader, HttpResponse }
import akka.stream._
import akka.stream.scaladsl.{ Flow, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString
import io.grpc.Status

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

/**
 * Some helpers for creating HTTP responses for use with gRPC
 *
 * INTERNAL API
 */
@InternalApi // consumed from generated classes so cannot be private
object GrpcResponseHelpers {

  private class SingleConcat[T](t: T) extends GraphStage[FlowShape[T, T]] {
    val in = Inlet[T]("SingleConcat.in")
    val out = Outlet[T]("SingleConcat.in")
    def shape: FlowShape[T, T] = FlowShape(in, out)
    def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
      def onPush(): Unit = push(out, grab(in))
      def onPull(): Unit = pull(in)

      override def onUpstreamFinish(): Unit = {
        emit(out, t)
        complete(out)
      }
      setHandlers(in, out, this)
    }
  }

  private class SingleConcatFuture[T](t: Future[T]) extends GraphStage[FlowShape[T, T]] {
    val in = Inlet[T]("SingleConcat.in")
    val out = Outlet[T]("SingleConcat.in")
    def shape: FlowShape[T, T] = FlowShape(in, out)
    def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
      def onPush(): Unit = push(out, grab(in))
      def onPull(): Unit = pull(in)

      override def onUpstreamFinish(): Unit = {
        val onComplete: Try[T] => Unit = {
          case Success(t) =>
            emit(out, t)
            complete(out)

          case Failure(ex) =>
            failStage(ex)
        }
        if (t.isCompleted) onComplete(t.value.get)
        else t.onComplete(getAsyncCallback(onComplete).invoke)(materializer.executionContext)
      }
      setHandlers(in, out, this)
    }

  }

  private val okTrailer = trailer(Status.OK)
  private val concatOkTrailer = Flow.fromGraph(new SingleConcat[HttpEntity.ChunkStreamPart](trailer(Status.OK)))
  private val bytesToChunk = Flow[ByteString].map(bytes => HttpEntity.Chunk(bytes))
  private val failureRecover = Flow[ChunkStreamPart].recover {
    case e: GrpcServiceException =>
      trailer(e.status)
    case e: Exception =>
      // TODO handle better
      e.printStackTrace()
      trailer(Status.UNKNOWN.withCause(e).withDescription("Stream failed"))
  }

  def apply[T](e: T)(implicit m: ProtobufSerializer[T], mat: Materializer, codec: Codec): HttpResponse = {
    // HERE WE COULD almost completely avoid doing any stream stuff since we already have the response
    val serialized = m.serialize(e)
    val encoded = Grpc.grpcFrameEncodeSingle(serialized, codec)
    val chunk = HttpEntity.Chunk(encoded)
    // do we need the try-catch?
    val outChunks = Source[HttpEntity.ChunkStreamPart](chunk :: okTrailer :: Nil)
    HttpResponse(
      headers = headers.`Message-Encoding`(codec.name) :: Nil,
      entity = HttpEntity.Chunked(Grpc.contentType, outChunks))
  }

  def apply[T](e: Source[T, NotUsed])(implicit m: ProtobufSerializer[T], mat: Materializer, codec: Codec): HttpResponse =
    GrpcResponseHelpers(e, concatOkTrailer)

  def apply[T](e: Source[T, NotUsed], status: Future[Status])(implicit m: ProtobufSerializer[T], mat: Materializer, codec: Codec): HttpResponse = {
    implicit val ec = mat.executionContext
    GrpcResponseHelpers(
      e,
      Flow.fromGraph(new SingleConcatFuture[HttpEntity.ChunkStreamPart](status.map(trailer(_)))))
  }

  def apply[T](e: Source[T, NotUsed], trailConcat: Flow[HttpEntity.ChunkStreamPart, HttpEntity.ChunkStreamPart, NotUsed])(implicit m: ProtobufSerializer[T], mat: Materializer, codec: Codec): HttpResponse = {
    val outChunks = e
      .map(m.serialize)
      .via(Grpc.grpcFramingEncoder(codec))
      .via(bytesToChunk)
      .via(trailConcat)
      .via(failureRecover)

    HttpResponse(
      headers = headers.`Message-Encoding`(codec.name) :: Nil,
      entity = HttpEntity.Chunked(Grpc.contentType, outChunks))
  }

  def status(status: Status): HttpResponse =
    HttpResponse(entity = HttpEntity.Chunked(Grpc.contentType, Source.single(trailer(status))))

  def trailer(status: Status): LastChunk =
    LastChunk(trailer = statusHeaders(status))

  def statusHeaders(status: Status): List[HttpHeader] =
    List(headers.`Status`(status.getCode.value.toString)) ++ Option(status.getDescription).map(d ⇒ headers.`Status-Message`(d))

}
