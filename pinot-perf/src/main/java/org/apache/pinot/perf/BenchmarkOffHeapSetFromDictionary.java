package org.apache.pinot.perf;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.query.aggregation.utils.BytesOffHeapSetFromDictionary;
import org.apache.pinot.core.query.aggregation.utils.IntOffHeapSetFromDictionary;
import org.apache.pinot.core.query.aggregation.utils.StringOffHeapSetFromDictionary;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import java.util.Random;


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



/*
  Baseline Benchmark ChronicleSet for all our Workloads
  Note ChronicleSet always needs upper bound on number of entries in the Set
 */

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 30)
@Measurement(iterations = 3, time = 30)
@Fork(1)
@State(Scope.Benchmark)

public class BenchmarkOffHeapSetFromDictionary {
  private static final int NUM_KEYS_PRELOAD = 1000000;
  private static final int GB_TO_BYTES = 1024*1024*1024; // conversion constant
  private static final int COLLISION_FACTOR = 100;  //about COLLISION_FACTOR many collisions per key for collision workloads
  @Param({"0.05"}) //GB of data to store
  float _gb;

  @Param({"5","20","60","150"}) // key length for variable length workloads
  int _keyLength;
  private RandomUtils _random;
  private Set<Integer> _intSet;
  private Set<String>  _stringSet;

  private int _maxOverflowSize = 2000;
  private DirectMemoryManager _memoryManager;
  @Setup
  public void setUp(){
    _memoryManager = new DirectMemoryManager(BenchmarkOffHeapSetFromDictionary.class.getName());
    _random = new RandomUtils();
    _intSet = new IntOffHeapSetFromDictionary(NUM_KEYS_PRELOAD, _maxOverflowSize, _memoryManager, "intColumn1");
/*
 return new IntOffHeapSetFromDictionary(size, 5000, _memoryManager, "intColumn");

        return new StringOffHeapSetFromDictionary(size, 5000, _memoryManager, "stringColumn",100);
      case BYTES:
        return new BytesOffHeapSetFromDictionary(size, 5000, _memoryManager, "stringColumn",100);
 */
    _stringSet = new StringOffHeapSetFromDictionary(NUM_KEYS_PRELOAD, _maxOverflowSize, _memoryManager, "stringColumn1",50);

    // populate sets for iterator and contains workloads
    for(int value=0;value<NUM_KEYS_PRELOAD;++value)
      _intSet.add(value);

    // populate sets for iterator and contains workloads
    _random.buildStringSetRandomRange(_stringSet,NUM_KEYS_PRELOAD,100);
  }

  /*
    Workload: Int Sorted with no collision
    Number of entries : based on _gb of storage
   */
  @Benchmark
  public void insertIntSortedNoCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
    Set<Integer> set = new IntOffHeapSetFromDictionary(numEntries, _maxOverflowSize, _memoryManager, "intColumn");

    int value;
    for(value=0;value<numEntries;++value)
      set.add(value);
  }

  /*
    Workload: Int Sorted with collision
    Number of entries : based on _gb of storage
    Collision: COLLISION_FACTOR many collision per key
   */
  @Benchmark
  public void insertIntSortedCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
    Set<Integer> set = new IntOffHeapSetFromDictionary(numEntries, _maxOverflowSize, _memoryManager, "intColumn");
    int maxValue = (int) numEntries/COLLISION_FACTOR;
    int value,count=0;

    for(value=0;value<=maxValue;++value) {
      while(count<(value*COLLISION_FACTOR) && count<numEntries) {
        set.add(value);
        ++count;
      }
    }
  }

  /*
    Workload: Int Random with collision
    Number of entries : based on _gb of storage
    Collision: COLLISION_FACTOR many collision per key
   */
  @Benchmark
  public void insertIntRandomCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
    Set<Integer> set = new IntOffHeapSetFromDictionary(numEntries, _maxOverflowSize, _memoryManager, "intColumn");
    int maxValue = numEntries/COLLISION_FACTOR;
    _random.buildIntSetRandomRange(set,numEntries,maxValue);
  }

  /*
   Workload: Random String with minimum collision
   Number of entries : based on _gb of storage
   Collision: maxLength set to 150 so 1/2^25 probability of collision
  */
  @Benchmark
  public void insertStringRandomLowCollision(){
    int maxLength = 150;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2

    Set<String> set = new StringOffHeapSetFromDictionary(numEntries, _maxOverflowSize, _memoryManager, "stringColumn",80); ;

    _random.buildStringSetRandomRange(set,numEntries,maxLength);
  }

  /*
   Workload: Random String with minimum collision
   Number of entries : based on _gb of storage
   Collision: maxLength set to 20 for about 1:10 collision
  */
  @Benchmark
  public void insertStringRandomCollision(){
    String str = StringUtils.repeat("a", _keyLength);
    int maxLength = 20;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2

    Set<String> set = new StringOffHeapSetFromDictionary(numEntries, _maxOverflowSize, _memoryManager, "stringColumn",20); ;

    _random.buildStringSetRandomRange(set,numEntries,maxLength);
  }

  /*
  Workload: Random Byte Array with minimum collision
  Number of entries : based on _gb of storage
  Collision: maxLength set to 20 for about 1:10 collision
  */
  @Benchmark
  public void insertByteArrayRandomLowCollision(){
    int maxLength = 120;
    int numEntries = (int)(GB_TO_BYTES*_gb*2)/maxLength;  // average length is maxLength/2

    Set<byte[]> set = new BytesOffHeapSetFromDictionary(numEntries, _maxOverflowSize, _memoryManager, "bytesColumn",60);

    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);
  }

  /*
  Workload: Random Byte Array with collision
  Number of entries : based on _gb of storage
  Collision: maxLength set to 10 for about 1:10 collision
  */
  @Benchmark
  public void insertByteArrayRandomCollision(){
    int maxLength = 10;
    int numEntries = (int)(GB_TO_BYTES*_gb*2)/maxLength;  // average length is maxLength/2
    Set<byte[]> set = new BytesOffHeapSetFromDictionary(numEntries, _maxOverflowSize, _memoryManager, "bytesColumn",10);

    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);
  }

  // ITERATOR WORKLOADS //
  @Benchmark
  public void iterateInt(){
    Iterator<Integer> it = _intSet.iterator();
    while(it.hasNext()){
      it.next();
    }
  }

  @Benchmark
  public void iterateString(){
    Iterator<String> it = _stringSet.iterator();
    while(it.hasNext()){
      it.next();
    }
  }

  //  CONTAINS WORKLOADS //
  @Benchmark
  public void containsIntWithinRange(){
    _intSet.contains(_random.getRandomInt(0,NUM_KEYS_PRELOAD));
  }

  @Benchmark
  public void containsIntOutsideRange(){
    _intSet.contains(_random.getRandomInt(NUM_KEYS_PRELOAD,1500000));
  }

  @Benchmark
  public void containsStringWithinRange(){
    _stringSet.contains(_random.generateRandomString(1,100));
  }

  @Benchmark
  public void containsStringOutsideRange(){
    _stringSet.contains(_random.generateRandomString(101,150));
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkOffHeapSetFromDictionary.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}


