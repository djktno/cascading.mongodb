package cascading.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Date: May 21, 2010
 * Time: 11:16:04 PM
 */
public class MongoDBOutputFormat<K extends MongoWritable, V> implements OutputFormat<K, V> {

    static final Logger log = Logger.getLogger(MongoDBOutputFormat.class.getName());
    public static final String OUTPUT_COLLECTION = "";



    public static void setOutput(JobConf jobConf, Class<MongoDBOutputFormat> mongoDBOutputFormatClass, String collectionName) {
        //To change body of created methods use File | Settings | File Templates.
    }

    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {

        MongoDBConfiguration dbConfiguration = new MongoDBConfiguration(jobConf);

        String collection = dbConfiguration.getCollection();
        String[] attributeNames = dbConfiguration.getDocumentAttributeNames();

        DB db = dbConfiguration.getDB();

        return new MongoDBRecordWriter(db, collection);
        
    }

    /** {@inheritDoc} */
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        //Don't do anything with this.
    }

    protected class MongoDBRecordWriter implements RecordWriter<K, V>
    {
        private DB db;
        private String collection;

        protected MongoDBRecordWriter(DB db, String collection)
        {
            this.db = db;
            this.collection = collection;
        }

        /** {@inheritDoc} */
        public synchronized void write(K k, V v) throws IOException {

            log.debug("MongoDBRecordWriter: {key = " + k +", value = " + v + ", collection = " + collection + "};");
        }

        /** {@inheritDoc} */
        public void close(Reporter reporter) throws IOException {
            
        }

        private void writeDocument(BasicDBObject document)
        {
            log.debug("Write Document: {document = " + document + ", collection = " + collection + "};");
        }
    }


}
