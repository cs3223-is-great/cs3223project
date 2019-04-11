package qp.utils;

import java.util.HashMap;
import java.util.Vector;

public class HashIndex {

   int attributeIndex;
   HashMap<Object, Vector<Tuple>> index;

   public HashIndex() {
       index = new HashMap<Object, Vector<Tuple>>();
   }

   public void add(Object key, Tuple value) {
       Vector<Tuple> currentRecords = index.get(key);
       if (currentRecords == null || currentRecords.isEmpty()) {
           currentRecords = new Vector<Tuple>();
           index.put(key, currentRecords);
       }
       currentRecords.add(value);
       index.put(key, currentRecords);
   }

   public Vector<Tuple> get(Object attr) {
       return index.get(attr);
   }


   public void load (Vector<Tuple> records) {
       for (Tuple t: records) {
           add(t.dataAt(attributeIndex), t);
       }
   }

   public void load (Batch batch) {
       load(batch.tuples);
   }

   public void setAttribute(int attr) {
       attributeIndex = attr;
   }
}