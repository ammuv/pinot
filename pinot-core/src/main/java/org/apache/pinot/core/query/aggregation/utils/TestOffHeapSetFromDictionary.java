package org.apache.pinot.core.query.aggregation.utils;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import javax.swing.text.html.HTMLDocument;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.junit.Test;

class RandomUtils {
  private final Random _random = new Random();

  // The generateRandomString method returns a random alphabetic string of length at most maxLength
  public String generateRandomString(int minLength, int maxLength) {

    return RandomStringUtils.randomAlphabetic(minLength,maxLength+1);
  }

  // The generateRandomByteArray method returns a random byte array of length at most maxLength
  public byte[] generateRandomByteArray(int maxLength) {

    // pick a random length which is at most maxLength
    int length = _random.nextInt(maxLength) + 1; // Adding 1 to avoid length of 0

    byte[] byteArray = new byte[length];
    _random.nextBytes(byteArray);

    return byteArray;
  }

  public void buildStringSetRandomRange(Set<String> set, long numEntries, int maxLength){
    for(int i=0;i<numEntries;++i){
      set.add(generateRandomString(1,maxLength));
    }
  }

  public void buildStringSetRandomRangeExact(Set<String> set, long numEntries, int maxLength){
    while(set.size()<numEntries) {
      set.add(generateRandomString(maxLength, maxLength));
    }
  }

  public void CopyFromSet(Set<String> fromSet, Set<String> toSet, long numEntries){ //assumes fromSet has numEntries
    Iterator<String> it = fromSet.iterator();
    int i=0;
    while(i<numEntries){
      i++;
      toSet.add(it.next());
    }
  }

  public void buildByteArraySetRandomRange(Set<byte[]> set, long numEntries, int maxLength){
    for(int i=0;i<numEntries;++i){
      set.add(generateRandomByteArray(maxLength));
    }
  }

  public void buildIntSetRandomRange(Set<Integer> set, long numEntries, int maxValue){
    for(int i=0;i<numEntries;++i){
      set.add(_random.nextInt(maxValue));
    }
  }

  public int getRandomInt(int minValue,int maxValue){
    return _random.nextInt(maxValue-minValue)+minValue;
  }
}


public class TestOffHeapSetFromDictionary {

  private final int NUM_KEYS_PRELOAD = 100;
 private final DirectMemoryManager _memoryManager = new DirectMemoryManager(TestOffHeapSetFromDictionary.class.getName());

 private static String toMB(long init) {
    return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
 }

 @Test
 public void testIntSet()
     throws InterruptedException {
   final long startTime = System.currentTimeMillis();
   int iteration = 1;
   while(iteration<=2000000) {
     IntOffHeapSetFromDictionary _intSet = new IntOffHeapSetFromDictionary(NUM_KEYS_PRELOAD, 0, _memoryManager, null);
     // populate sets for iterator and contains workloads
     for (int value = 0; value < NUM_KEYS_PRELOAD; ++value) {
       _intSet.add(value);
       //System.out.println(_intSet.size());
     }

     Iterator it = _intSet.iterator();
//     while (it.hasNext())
//       it.next();
     long endTime = System.currentTimeMillis();
     //System.out.println("hi iteration " + iteration++ + " time " + (endTime-startTime));
     iteration++;
   }

   List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
   for (BufferPoolMXBean pool : pools) {
     System.out.println(pool.getName());
     System.out.println(pool.getCount());
     System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
     System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
     System.out.println();
   }

   Thread.sleep(60000);
 }

 @Test
 public void testStringSet(){
   RandomUtils random = new RandomUtils();
   Set<String> stringSet = new StringOffHeapSetFromDictionary(10, 0, _memoryManager, null,10);
   random.buildStringSetRandomRange(stringSet,10000000,150);
//   Iterator it = stringSet.iterator();
//   while(it.hasNext()){
//     System.out.println(it.next());
//   }
   System.out.println(stringSet.size());
 }

 @Test
  public void testByteSet(){
    RandomUtils random = new RandomUtils();
    Set<byte[]> set = new BytesOffHeapSetFromDictionary(100, 0, _memoryManager, null,10);
    random.buildByteArraySetRandomRange(set,100,10);
    Iterator it = set.iterator();
    System.out.println(set.size());
    while(it.hasNext()){
      byte[] b = (byte[])it.next();
      System.out.println(b);
      System.out.println(set.size());
      set.add(b);
      System.out.println(set.size());
    }
  }

    private double[] _gb = {0.001,0.005,0.05,0.5,1.0,1.5,2};

    private int[] _numIntKeys;

    private int[] _numVarKeys;

    private int _avgKeyLen = 120;

    void gbToKeys(){
      int GB_TO_BYTES = 1024*1024*1024;
      _numIntKeys = new int[7];
      _numVarKeys = new int[7];

      for(int i=0;i<7;i++) {
        _numIntKeys[i] = (int) (GB_TO_BYTES * _gb[i]) / 4;
        _numVarKeys[i] = (int) (GB_TO_BYTES * _gb[i]) / (_avgKeyLen);
      }
    }

    private String toGB(long init) {
      return (Long.valueOf(init).doubleValue() / (1024 * 1024 * 1024)) + " GB";
    }

    @Test
    public void checkMemoryUsageInt()
        throws InterruptedException {
      gbToKeys();
      List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
      System.out.println("Initial Heap Memory:" +  toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));
      for (int i = 0; i < 5; i++) {
        System.out.println("\n \n ************************ Iteration " + i + " Keys " +_numIntKeys[i] + " GB " + _gb[i] + "*****************************");
        //System.out.println("OffHeap Memory for Chronicle Set " + toGB(set.offHeapMemoryUsed())); //offHeapMemory

        System.gc();
        Thread.sleep(30000);
        System.out.println("Initial Heap Memory:" +  toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));
        //System.out.println("OffHeap Memory before loading " + toMB(intSet._dict.getTotalOffHeapMemUsed())); //offHeapMemory
        for (BufferPoolMXBean pool : pools) {
          System.out.println(pool.getName());
          System.out.println(pool.getCount());
          System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
          System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
          System.out.println();
        }

        IntOffHeapSetFromDictionary intSet = new IntOffHeapSetFromDictionary(1000000, 0, _memoryManager, null);

        // populate set
        for (int value = 0; value < _numIntKeys[i]; ++value) {
          intSet.add(value);
        }

        System.gc();
        Thread.sleep(30000);
        System.out.println("-------After loading-----------------");
        System.out.println("Heap Memory: " +  toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory
        System.out.println("OffHeap Memory after loading " + toMB(intSet._dict.getTotalOffHeapMemUsed())); //offHeapMemory
        System.out.println("Size of set after loading " + intSet.size());

        for (BufferPoolMXBean pool : pools) {
          System.out.println(pool.getName());
          System.out.println(pool.getCount());
          System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
          System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
          System.out.println();
        }
      }
    }

  @Test
  public void checkMemoryUsageString()
      throws InterruptedException {
    gbToKeys();

    RandomUtils random = new RandomUtils();
    Set<String> stringSetKeys = new HashSet<>(_numVarKeys[4]);
    random.buildStringSetRandomRangeExact(stringSetKeys,_numVarKeys[4],_avgKeyLen); // only 1GB worth of keys

    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

    System.out.println("Stringg Initial Heap Memory:" + toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));
    for (int i = 0; i < 5; i++) {
      System.out.println("\n \n ******************* Iteration " + i + " Keys " +_numVarKeys[i] + " GB " + _gb[i] +"*********************");
      //System.out.println("OffHeap Memory for Chronicle Set " + toGB(set.offHeapMemoryUsed())); //offHeapMemory

      System.gc();
      Thread.sleep(30000);
      System.out.println("Initial Heap Memory:" +  toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));
      //System.out.println("OffHeap Memory before loading " + toMB(set._dict.getTotalOffHeapMemUsed())); //offHeapMemory
      for (BufferPoolMXBean pool : pools) {
        System.out.println(pool.getName());
        System.out.println(pool.getCount());
        System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
        System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
        System.out.println();
      }

      StringOffHeapSetFromDictionary set = new StringOffHeapSetFromDictionary(1000000, 0, _memoryManager, null,1);

      random.CopyFromSet(stringSetKeys,set,_numVarKeys[i]); // load keys

      System.gc();
      Thread.sleep(30000);
      System.out.println("-------After loading-----------------");
      System.out.println("Heap Memory: " +  toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory
      System.out.println("OffHeap Memory after loading " + toMB(set._dict.getTotalOffHeapMemUsed())); //offHeapMemory
      System.out.println("Size of set after loading " + set.size());

      for (BufferPoolMXBean pool : pools) {
        System.out.println(pool.getName());
        System.out.println(pool.getCount());
        System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
        System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
        System.out.println();
      }
    }
  }

}
