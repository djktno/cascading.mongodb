package cascading.mongodb;

import cascading.tuple.TupleEntry;
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

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * An OutputFormat that sends reduce output to a MongoDB collection.
 * <p/>
 * {@link MongoDBOutputFormat} accepts &lt;key, value&gt; pairs, where
 * the key has a type extending MongoDBWritable.  Returned {@link cascading.mongodb.MongoDBOutputFormat.MongoDBRecordWriter}
 * writes a document to the collection with the Fields names specified as the
 * attributes of the document.
 */
public class MongoDBOutputFormat<K extends MongoDocument, V extends TupleEntry> implements OutputFormat<K, V> {

    static final Logger log = Logger.getLogger(MongoDBOutputFormat.class.getName());
    private Class<? extends MongoDocument> documentFormat;

    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {

        MongoDBConfiguration dbConfiguration = new MongoDBConfiguration(jobConf);

        String collection = dbConfiguration.getCollection();
        String database = dbConfiguration.getDatabase();

        return new MongoDBRecordWriter(database, collection);

    }

    public void setDocumentFormat(Class<? extends MongoDocument> documentFormat) {
        this.documentFormat = documentFormat;
    }

    /**
     * {@inheritDoc}
     */
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        //Don't do anything with this.
    }

    protected class MongoDBRecordWriter<K extends MongoDocument, V>
            implements RecordWriter<K, V> {
        private DB db;
        private String collection;
        private int retryAttempts;

        protected MongoDBRecordWriter(String database, String collection) {

            this.collection = collection;
            this.retryAttempts = 10;
            try {
                this.db = MongoWrapper.instance().getDB(database);
            }
            catch (UnknownHostException e) {
                throw new RuntimeException("This should not happen.");
            }
        }

        /**
         * {@inheritDoc}
         */
        public synchronized void write(K k, V v) throws IOException {

            MongoDBOutputFormat.log.debug("MongoDBRecordWriter: {key = " + k + ", value = " + v + ", collection = " + collection + "};");

            if (!(v instanceof TupleEntry))
                throw new IllegalArgumentException("Expected " + TupleEntry.class.getName() + ", received " + v.getClass().getName());

            if (!(k instanceof MongoDocument))
                throw new IllegalArgumentException("Expected " + MongoDocument.class.getName() + ", received " + k.getClass().getName());

            k.write((TupleEntry) v);

            BasicDBObject d = k.getDocument();

            int insertAttemptsRemaining = retryAttempts;

            while (true) {
                try {
                    DBCollection dbc = db.getCollection(collection);
                    dbc.apply(d, true);

                    MongoDBOutputFormat.log.debug("Inserting document into database...");
                    dbc.save(d);
                }
                catch (MongoException e) {
                    if (insertAttemptsRemaining >= 0) {
                        MongoDBOutputFormat.log.info("Waiting one second before retry...");
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ie) {
                        }
                        MongoDBOutputFormat.log.info("Document insert failed: { message = " + e.getMessage() + ", attemptsRemaining = " + insertAttemptsRemaining);
                        insertAttemptsRemaining--;
                        continue;
                    } else {
                        throw new IOException("Exhausted 10 attempts to insert document: {document = " + d + "};", e);
                    }
                }
                catch (RuntimeException e) {
                    MongoDBOutputFormat.log.error("Caught generic exception saving document.", e);
                    throw new IOException(e);
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
