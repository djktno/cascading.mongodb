package cascading.mongodb;

import cascading.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * An OutputFormat that sends reduce output to a MongoDB collection.
 * <p/>
 * {@link MongoDBOutputFormat} accepts &lt;key, value&gt; pairs, where
 * the key has a type extending MongoDBWritable.  Returned {@link MongoDBRecordWriter}
 * writes a document to the collection with the Fields names specified as the
 * attributes of the document. 
 * 
 */
public class MongoDBOutputFormat<K extends MongoWritable, V> implements OutputFormat<K, V> {

    static final Logger log = Logger.getLogger(MongoDBOutputFormat.class.getName());

    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {

        MongoDBConfiguration dbConfiguration = new MongoDBConfiguration(jobConf);

        String collection = dbConfiguration.getCollection();
        DB db = dbConfiguration.getDB();

        return new MongoDBRecordWriter(db, collection);

    }

    /**
     * {@inheritDoc}
     */
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        //Don't do anything with this.
    }

    protected class MongoDBRecordWriter implements RecordWriter<K, V> {
        private DB db;
        private String collection;
        private int retryAttempts;

        protected MongoDBRecordWriter(DB db, String collection) {
            this.db = db;
            this.collection = collection;
            this.retryAttempts = 10;
        }

        /**
         * {@inheritDoc}
         */
        public synchronized void write(K k, V v) throws IOException {

            log.debug("MongoDBRecordWriter: {key = " + k + ", value = " + v + ", collection = " + collection + "};");
            BasicDBObject d = (BasicDBObject) v;

            log.debug("Write Document: {document = " + d + ", collection = " + collection + "};");

            int insertAttemptsRemaining = retryAttempts;

            while(true)
            {
                try
                {
                    db.getCollection(collection).insert(d);
                }
                catch (MongoException e)
                {
                    if (insertAttemptsRemaining >= 0)
                    {
                        log.info("Waiting one second before retry...");
                        try
                        {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ie)
                        {}
                        log.info("Document insert failed: { message = " + e.getMessage() + ", attemptsRemaining = " + insertAttemptsRemaining);
                        insertAttemptsRemaining--;
                        continue;
                    }
                    else
                    {
                        throw new IOException("Exhausted 10 attempts to insert document: {document = " + d + "};", e);
                    }
                }
                insertAttemptsRemaining = retryAttempts;
                break;
            }
        }

        /**
         * {@inheritDoc}
         */
        public void close(Reporter reporter) throws IOException {
        
        }

    }


}
