package org.apache.pinot.core.query.aggregation.utils.hashset;

import com.google.api.Page;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * This is the abstract base class for the OffHeapPageManager component of our custom OffHeapHashSet
 * <p>
 * This class manages creating and maintaining pages - which are PinotDataBuffers It also manages writing to and reading
 * from pages.
 * <p>
 * Pages are append only and depending upon data type the serialized format stored in the pages are different - for
 * variable length data type (length,data) is stored in serialized form - for fixed length data type (data) is stored in
 * serialized form
 * <p>
 * Since pages are just buffer, in the class they are referred to as buffers.
 * <p>
 * The abstract class is extended by @FixedLengthOffHeapPageManager and @VariableLengthOffHeapPageManager
 */
public abstract class BaseOffHeapPageManager implements Iterable<byte[]> {
  ArrayList<PinotDataBuffer> _bufferList;
  int _bufferSize;

  PageAddress _curAddress;
  int _numKeys;

  /**
   * @param bufferSize Size of pages(i.e. PinotDataBuffers) allocated in direct memory in bytes
   */
  BaseOffHeapPageManager(int bufferSize) {
    _bufferSize = bufferSize;
    _numKeys = 0;
    _curAddress.bufNum = -1;
    addNewBuffer();
  }

  /**
   * Method to add new page/buffer
   */
  void addNewBuffer() {
    PinotDataBuffer buf = PinotDataBuffer.allocateDirect(_bufferSize, PinotDataBuffer.NATIVE_ORDER, null);
    _bufferList.add(buf);
    _curAddress.bufNum += 1;
    _curAddress.bufOffset = 0;
  }

  /**
   * @param data
   * @return PageAddress the address (i.e. bufNum and bufIdx) at which the data is stored
   */
  public abstract PageAddress addData(byte[] data);

  /**
   * @param addr
   * @return byte[] fetch the value stored at PageAddress addr
   */
  public abstract byte[] fetchData(PageAddress addr);

  /**
   * @param addr
   * @param data
   * @return true if the value stored at address addr is equal to data else return false
   */
  public boolean compareData(PageAddress addr, byte[] data) {
    return (fetchData(addr) == data);
  }

  /**
   * Helper for iterator
   *
   * @param addr moves addr to next addr inside page or to next page if page has no more data
   */
  public abstract void getNextAddr(PageAddress addr);

  /**
   * Iterator for iterating through keys stored in pages
   */
  public Iterator<byte[]> iterator() {
    Iterator<byte[]> it = new Iterator<byte[]>() {
      private int curKeyNum = 0;
      private PageAddress curAddr = new PageAddress(0, 0);

      @Override
      public boolean hasNext() {
        return curKeyNum < _numKeys;
      }

      @Override
      public byte[] next() {
        byte[] value = fetchData(curAddr);
        getNextAddr(curAddr);
        curKeyNum++;
        return value;
      }
    };
    return it;
  }
}
