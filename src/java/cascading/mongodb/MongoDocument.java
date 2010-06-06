package cascading.mongodb;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import java.io.Serializable;

/**
 * Objects that are read from/written to MongoDB should implement
 * <code>MongoDocument</code>. MongoDocument, is similar to {@link cascading.jdbc.DBWritable}
 * except that the {@link #write(BasicDBObject)} method takes a
 * {@link BasicDBObject}, and {@link #readFields()}
 * takes a {@link BasicDBObject}.
 * <p>
 * Implementations are responsible for put-ing the fields of the object
 * to BasicDBObject, and reading the fields of the object from the
 * Document.
 * <p/>
 * <p>Example:</p>
 * If we have the following document in the database :
 * <pre>
 * example document
 * </pre>
 * then we can read/write the tuples from/to the document with :
 * <p><pre>
 * public class MyWritable implements Writable, MongoDocument {
 *   // Some data
 *   private int counter;
 *   private long timestamp;
 * <p/>
 *   //Writable#write() implementation
 *   public void write(DataOutput out) throws IOException {
 *     out.writeInt(counter);
 *     out.writeLong(timestamp);
 *   }
 * <p/>
 *   //Writable#readFields() implementation
 *   public void readFields(DataInput in) throws IOException {
 *     counter = in.readInt();
 *     timestamp = in.readLong();
 *   }
 * <p/>
 *   public void write(BasicDBOject document) throws SQLException {
 *     document.set(COUNTER_KEY, counter));
 *     document.set(TIMESTAMP_KEY, timestamp));
 *   }
 * <p/>
 *   public void readFields(BasicDBObject document) throws SQLException {
 *     counter = document.get(COUNTER_KEY);
 *     timestamp = document.get(TIMESTAMP_KEY);
 *   }
 * }
 * </pre></p>
 */
public interface MongoDocument extends Serializable
{
    public void write(TupleEntry tupleEntry) throws MongoException;
    public void readFields(BasicDBObject document) throws MongoException;
    public BasicDBObject getDocument();
    public Tuple getTuple();
}
