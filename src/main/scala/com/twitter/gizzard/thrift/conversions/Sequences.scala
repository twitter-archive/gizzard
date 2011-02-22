package com.twitter.gizzard
package thrift.conversions

import scala.collection.JavaConversions._
import java.nio.{BufferUnderflowException, ByteBuffer, ByteOrder}
import java.util.{AbstractList => JAbstractList, List => JList}


object Sequences {
  class RichSeq[A <: AnyRef](seq: Seq[A]) {
    def parallel(future: Future) = new ParallelSeq(seq, future)
    def toJavaList: JList[A] = seq
    def double = for (i <- seq) yield (i, i)
  }
  implicit def seqToRichSeq[A <: AnyRef](seq: Seq[A]) = new RichSeq(seq)

  class RichIntSeq(seq: Seq[Int]) {
    def parallel(future: Future) = new ParallelSeq(seq, future)
    def toJavaList: JList[java.lang.Integer] = seq.map(_.asInstanceOf[java.lang.Integer])
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

  class RichLongSeq(seq: Seq[Long]) {
    def parallel(future: Future) = new ParallelSeq(seq, future)
    def toJavaList: JList[java.lang.Long] = seq.map(_.asInstanceOf[java.lang.Long])
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

  class RichJavaList[T <: AnyRef](list: JList[T]) {
    def toSeq: Seq[T] = list
    def toList = toSeq.toList
  }
  implicit def javaListToRichSeq[T <: AnyRef](list: JList[T]) = new RichJavaList(list)

  class RichJavaIntList(list: JList[java.lang.Integer]) {
    def toSeq: Seq[Int] = list.map(_.asInstanceOf[Int])
    def toList = toSeq.toList
  }
  implicit def javaIntListToRichSeq(list: JList[java.lang.Integer]) = new RichJavaIntList(list)

  class RichJavaLongList(list: JList[java.lang.Long]) {
    def toSeq: Seq[Long] = list.map(_.asInstanceOf[Long])
    def toList = toSeq.toList
  }
  implicit def javaLongListToRichSeq(list: JList[java.lang.Long]) = new RichJavaLongList(list)

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
