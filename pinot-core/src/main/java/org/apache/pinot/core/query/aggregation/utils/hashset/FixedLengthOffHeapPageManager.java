package org.apache.pinot.core.query.aggregation.utils.hashset;

/**
 * BaseOffHeapPageManager implementation for Fixed Length types Data stored in page as data in serialized form The data
 * member _length stored length of fixed length types in bytes
 */
public class FixedLengthOffHeapPageManager extends BaseOffHeapPageManager {
  private int _length;

  FixedLengthOffHeapPageManager(int bufferSize, int length) {
    super(bufferSize);
    _length = length;
  }

  @Override
  public PageAddress addData(byte[] data) {
    if (_curAddress.bufOffset + _length > _bufferSize) {
      addNewBuffer();
    }
    // add exception if data exceeds page size
    _bufferList.get(_curAddress.bufNum).readFrom(_curAddress.bufOffset, data, 0,
        data.length); //public void readFrom(long offset, byte[] buffer, int srcOffset, int size)

    PageAddress pageAddress = new PageAddress(_curAddress.bufNum, _curAddress.bufOffset);

    _curAddress.bufOffset += _length;
    _numKeys++;
    return pageAddress;
  }

  @Override
  public byte[] fetchData(PageAddress addr) {
    byte[] data = new byte[_length];
    _bufferList.get(addr.bufNum).copyTo(addr.bufOffset, data, 0,
        data.length); //public void copyTo(long offset, byte[] buffer, int destOffset, int size)
    return data;
  }

  @Override
  public void getNextAddr(PageAddress addr) {
    if (addr.bufOffset + _length > _bufferSize) {
      addr.bufNum += 1;
      addr.bufOffset = 0;
    }
    addr.bufOffset += _length;
  }
}
