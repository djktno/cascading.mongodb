package cascading.mongodb;

import com.mongodb.DB;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * An OutputFormat that sends reduce output to a MongoDB collection.
 * <p/>
 * {@link MongoDBOutputFormat} accepts &lt;key, value&gt; pairs, where
 * the key has a type extending MongoDBWritable.  Returned {@link cascading.mongodb.DefaultMongoDBRecordWriter}
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


        String username = dbConfiguration.getUsername();
        if (username == null && "".equals(username))
            log.warn("Username was not set.  If your database has security enabled, your upcoming writes will fail.  If not, no worries!");

        else if (!db.authenticate(dbConfiguration.getUsername(), dbConfiguration.getPassword()))
        {
            throw new IllegalArgumentException("Username or password provided was incorrect.  Check your configuration.");
        }

        return new DefaultMongoDBRecordWriter(db, collection);

    }

    /**
     * {@inheritDoc}
     */
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        //Don't do anything with this.
    }


}
