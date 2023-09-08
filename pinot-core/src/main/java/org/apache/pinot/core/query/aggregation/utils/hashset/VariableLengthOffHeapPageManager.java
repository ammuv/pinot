package org.apache.pinot.core.query.aggregation.utils.hashset;

import java.nio.ByteBuffer;


/**
 * BaseOffHeapPageManager implementation for Variable Length types Data stored in page as (length,data) in serialized
 * form
 */
public class VariableLengthOffHeapPageManager extends BaseOffHeapPageManager {

  VariableLengthOffHeapPageManager(int bufferSize) {
    super(bufferSize);
  }

  @Override
  public PageAddress addData(byte[] data) {

    if ((_curAddress.bufOffset + 4 + data.length) > _bufferSize) {
      addNewBuffer();
    }
    _bufferList.get(_curAddress.bufNum).putInt(_curAddress.bufOffset, data.length);
    // add exception if data exceeds page size
    _bufferList.get(_curAddress.bufNum).readFrom(_curAddress.bufOffset + 4, data, 0,
        data.length); //public void readFrom(long offset, byte[] buffer, int srcOffset, int size)

    PageAddress pageAddress = new PageAddress(_curAddress.bufNum, _curAddress.bufOffset);

    _curAddress.bufOffset += 4 + data.length;
    _numKeys++;
    return pageAddress;
  }

  @Override
  public byte[] fetchData(PageAddress addr) {
    int length = _bufferList.get(addr.bufNum).getInt(addr.bufOffset);
    byte[] data = new byte[length];
    _bufferList.get(addr.bufNum).copyTo(addr.bufOffset, data, 0,
        data.length); //public void copyTo(long offset, byte[] buffer, int destOffset, int size)

    return data;
  }

  @Override
  public void getNextAddr(PageAddress addr) {
    //assumes there exists data in current addr
    int length = _bufferList.get(addr.bufNum).getInt(addr.bufOffset);
    //move to off past current data
    addr.bufOffset += length + 4;

    if (addr.bufOffset + 4 > _bufferSize) {//buffer ends due to less than 4 bytes
      addr.bufNum += 1;
      addr.bufOffset = 0;
    }

    int lengthNext = _bufferList.get(addr.bufNum).getInt(addr.bufOffset);
    if (lengthNext == 0) { //buffer ends because next data length is 0 so no data stored
      addr.bufNum += 1;
      addr.bufOffset = 0;
    }
  }
}
