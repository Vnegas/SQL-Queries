import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.BulkWriteOptions;
import oracle.kv.EntryStream;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValue;
import oracle.kv.Value;

public class InsertReg {
  private static final Map<Integer, String> units;
  private static final Map<Integer, String> tens;
  private static final Map<Integer, String> hundreds;

  public static void main(String[] args) {
    String[] hhosts = {"127.0.0.1:5000"};
    KVStoreConfig kconfig = new KVStoreConfig("kvstore", hhosts);
    try (KVStore kvstore = KVStoreFactory.getStore(kconfig)) {
      // I. normal put
      long start = System.currentTimeMillis();
      insertNormal(kvstore);
      long end = System.currentTimeMillis();
      System.out.println("Time on single put: " + (end - start) + " ms");
      
      // II. delete registers
      deleteAll(kvstore);

      // III. bulk put
      start = System.currentTimeMillis();
      bulkInsert(kvstore);
      end = System.currentTimeMillis();
      System.out.println("Time on bulk put: " + (end - start) + " ms");
    }
  }

  public static void insertNormal(KVStore kvstore) {
    ArrayList<String> majorComponents = new ArrayList<String>();
    for (int i = 1; i <= 25000; i++) {
      majorComponents.add("number");
      majorComponents.add(Integer.toString(i));
      Key myKey = Key.createKey(majorComponents);

      String number = convertNumber(i); // convert integer to letter representation

      Value value = Value.createValue(number.getBytes());
      kvstore.put(myKey, value);

      majorComponents.clear();
    }
  }
  
  public static void deleteAll(KVStore kvstore) {
    ArrayList<String> majorComponents = new ArrayList<String>();
    majorComponents.add("lista");
    Key myKey = Key.createKey(majorComponents);
    kvstore.multiDelete(myKey, null, null);   
  }

  static {
    units = new HashMap<>();
    units.put(1, "uno");
    units.put(2, "dos");
    units.put(3, "tres");
    units.put(4, "cuatro");
    units.put(5, "cinco");
    units.put(6, "seis");
    units.put(7, "siete");
    units.put(8, "ocho");
    units.put(9, "nueve");

    tens = new HashMap<>();
    tens.put(10, "diez");
    tens.put(20, "veinte");
    tens.put(30, "treinta");
    tens.put(40, "cuarenta");
    tens.put(50, "cincuenta");
    tens.put(60, "sesenta");
    tens.put(70, "setenta");
    tens.put(80, "ochenta");
    tens.put(90, "noventa");

    hundreds = new HashMap<>();
    hundreds.put(100, "cien");
    hundreds.put(200, "doscientos");
    hundreds.put(300, "trescientos");
    hundreds.put(400, "cuatrocientos");
    hundreds.put(500, "quinientos");
    hundreds.put(600, "seiscientos");
    hundreds.put(700, "setecientos");
    hundreds.put(800, "ochocientos");
    hundreds.put(900, "novecientos");
  }

  public static String convertNumber(int number) {
    if (number == 0) return "cero";
    if (number < 10) return units.get(number);
    if (number < 100) return convertTens(number);
    if (number < 1000) return convertHundreds(number);
    if (number <= 25000) return convertThousands(number);
    return "";
  }

  private static String convertTens(int number) {
    if (tens.containsKey(number)) return tens.get(number);
    int unit = number % 10;
    int ten = number - unit;
    return tens.get(ten) + " y " + units.get(unit);
  }

  private static String convertHundreds(int number) {
    if (hundreds.containsKey(number)) return hundreds.get(number);
    int remainder = number % 100;
    int hundred = number - remainder;
    return hundreds.get(hundred) + (remainder > 0 ? " " + convertTens(remainder) : "");
  }

  private static String convertThousands(int number) {
    if (number == 1000) return "mil";
    if (number < 2000) return "mil " + convertHundreds(number % 1000);
    int thousand = number / 1000;
    int remainder = number % 1000;
    return convertNumber(thousand) + " mil" + (remainder > 0 ? " " + convertHundreds(remainder) : "");
  }
  
  public static void bulkInsert(KVStore store) {
    int nLoad = 25000; // Número de registros a insertar
    BulkWriteOptions bulkWriteOptions = new BulkWriteOptions(null, 0, null);

    bulkWriteOptions.setStreamParallelism(2);
    
    final List<EntryStream<KeyValue>> streams = new ArrayList<>(2);
    final int num = (nLoad + 1) / 2;

    for (int i = 0; i < 2; i++) {
      final int min = num * i;
      final int max = Math.min(min + num, nLoad);
      streams.add(new LoadKVStream("Stream" + i, i, min, max));
    }
    
    // Inserta los datos en bulk
    store.put(streams, bulkWriteOptions);
  }

  // Class LoadKVStream from documentation
  private static class LoadKVStream implements EntryStream<KeyValue> {
    private final String name;
    private final long index;
    private final long max;
    private final long min;
    private long id;
    private long count;
    private final AtomicLong keyExistsCount;

    LoadKVStream(String name, long index, long min, long max) {
      this.index = index;
      this.max = max;
      this.min = min;
      this.name = name;
      id = min;
      count = 0;
      keyExistsCount = new AtomicLong();
    }

    @Override
    public String name() {
      return name + "-" + index + ": " + min + "~" + max;
    }

    @Override
    public KeyValue getNext() {
      if (id++ == max) {
        return null;
      }
      Key key = Key.fromString("/bulk/" + id);
      Value value = Value.createValue(("value" + id).getBytes());
      KeyValue kv = new KeyValue(key, value);
      count++;
      return kv;
    }

    @Override
    public void completed() {
      // Commented to clean terminal output and just show what is neccessary
      // System.err.println(name() + " completed, loaded: " + count);
    } 

    @Override
    public void keyExists(KeyValue entry) {
      keyExistsCount.incrementAndGet();
    }

    @Override
    public void catchException(RuntimeException exception, KeyValue entry) {
      System.err.println(name() + " excepcion: " + exception.getMessage() + ": " + entry.toString());
      throw exception;
    }

    public long getCount() {
      return count;
    }

    public long getKeyExistsCount() {
      return keyExistsCount.get();
    }
  }
}
