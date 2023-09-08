package org.apache.pinot.core.query.aggregation.utils.hashset;

import com.google.api.Page;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class PageAddress {
  public int bufNum;
  public int bufOffset;

  public PageAddress(int bufNum, int bufOffset) {
    this.bufNum = bufNum;
    this.bufOffset = bufOffset;
  }
}
//public PageAddress(PinotDataBuffer buffer, int offset) {
// bufNum = buffer.getInt(offset);
//bufOffset = buffer.getInt(offset + 4);
// }
//}
/*
  public ByteBuffer toByteBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putInt(bufNum);
    buffer.putInt(bufOffset);
    buffer.rewind();
    return buffer;
  }

  public static PageAddress fromBytes(byte[] byteAddr) {
    ByteBuffer buffer = ByteBuffer.wrap(byteAddr);
    int bufNum = buffer.getInt();
    int bufOffset = buffer.getInt();
    PageAddress addr = new PageAddress(bufNum, bufOffset);
    return addr;
  }
 */