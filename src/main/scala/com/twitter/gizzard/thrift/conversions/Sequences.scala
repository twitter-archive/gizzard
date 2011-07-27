package com.twitter.gizzard.thrift.conversions

import scala.collection.{JavaConversions => JC}
import java.nio.{BufferUnderflowException, ByteBuffer, ByteOrder}
import java.util.{AbstractList => JAbstractList, List => JList}
import com.twitter.gizzard.util.{Future, ParallelSeq}


object Sequences {
  class RichSeq[A <: AnyRef](seq: Seq[A]) {
    def parallel(future: Future) = new ParallelSeq(seq, future)

    @deprecated("rely on implicit conversion from scala.collection.JavaConversions._")
    def toJavaList: JList[A] = JC.asJavaList(seq)
    def double = for (i <- seq) yield (i, i)
  }

  implicit def seqToRichSeq[A <: AnyRef](seq: Seq[A]) = new RichSeq(seq)


  class RichIntSeq(seq: Seq[Int]) {
    def parallel(future: Future) = new ParallelSeq(seq, future)

    @deprecated("there is implicit conversion from Seq[Int] to java.util.List[java.lang.Integer]")
    def toJavaList: JList[java.lang.Integer] = intSeqToBoxedJavaList(seq)
    def double = for (i <- seq) yield (i, i)

    def pack: ByteBuffer = {
      val buffer = new Array[Byte](seq.size * 4)
      val byteBuffer = ByteBuffer.wrap(buffer)
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
      seq.foreach { item => byteBuffer.putInt(item) }
      byteBuffer.rewind
      byteBuffer
    }
  }

  implicit def seqToRichIntSeq(seq: Seq[Int]) = new RichIntSeq(seq)

  implicit def intSeqToBoxedJavaList(seq: Seq[Int]) = {
    JC.asJavaList(seq.map(_.asInstanceOf[java.lang.Integer]))
  }

  implicit def boxedJavaListToIntSeq(list: JList[java.lang.Integer]) = {
    JC.asScalaIterable(list).toSeq.map(_.asInstanceOf[Int])
  }


  class RichLongSeq(seq: Seq[Long]) {
    def parallel(future: Future) = new ParallelSeq(seq, future)

    @deprecated("there is implicit conversion from Seq[Long] to java.util.List[java.lang.Long]")
    def toJavaList: JList[java.lang.Long] = longSeqToBoxedJavaList(seq)
    def double = for (i <- seq) yield (i, i)

    def pack: ByteBuffer = {
      val buffer = new Array[Byte](seq.size * 8)
      val byteBuffer = ByteBuffer.wrap(buffer)
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
      seq.foreach { item => byteBuffer.putLong(item) }
      byteBuffer.rewind
      byteBuffer
    }
  }

  implicit def seqToRichLongSeq(seq: Seq[Long]) = new RichLongSeq(seq)

  implicit def longSeqToBoxedJavaList(seq: Seq[Long]) = {
    JC.asJavaList(seq.map(_.asInstanceOf[java.lang.Long]))
  }

  implicit def boxedJavaListToLongSeq(list: JList[java.lang.Long]) = {
    JC.asScalaIterable(list).toSeq.map(_.asInstanceOf[Long])
  }


  class RichByteBuffer(buffer: ByteBuffer) {
    def toIntArray = {
      buffer.order(ByteOrder.LITTLE_ENDIAN)
      val ints = buffer.asIntBuffer
      val results = new Array[Int](ints.limit)
      ints.get(results)
      results
    }

    def toLongArray = {
      buffer.order(ByteOrder.LITTLE_ENDIAN)
      val longs = buffer.asLongBuffer
      val results = new Array[Long](longs.limit)
      longs.get(results)
      results
    }
  }

  implicit def bufferToRichByteBuffer(buffer: ByteBuffer) = new RichByteBuffer(buffer)
}
