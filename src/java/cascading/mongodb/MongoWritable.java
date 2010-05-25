package cascading.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

/**
 * Objects that are read from/written to MongoDB should implement
 * <code>MongoWritable</code>. MongoWritable, is similar to {@link cascading.jdbc.DBWritable}
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
 * public class MyWritable implements Writable, MongoWritable {
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
public interface MongoWritable
{
    public void write(BasicDBObject document) throws MongoException;


    public void readFields(BasicDBObject document) throws MongoException;
}
