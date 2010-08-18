package cascading.mongodb;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Date: May 21, 2010
 * Time: 8:32:20 PM
 */
public class MongoDBScheme extends Scheme {
    private static final Logger log = Logger.getLogger(MongoDBScheme.class.getName());

    private Class<? extends OutputFormat> outputFormatClass;
    private Class<? extends InputFormat> inputFormatClass;
    private MongoDocument documentFormat;

    public MongoDBScheme(Class<? extends MongoDBOutputFormat> outputFormatClass, Class<? extends MongoDBInputFormat> inputFormatClass) {
        this.outputFormatClass = outputFormatClass;
        this.inputFormatClass = inputFormatClass;
    }

    public MongoDBScheme(Class<? extends MongoDBInputFormat> inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
    }

    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {

        String collection = ((MongoDBTap) tap).getCollection();
        String database = ((MongoDBTap) tap).getDatabase();
        int port = ((MongoDBTap) tap).getPort();
        String host = ((MongoDBTap) tap).getHostname();
        documentFormat = ((MongoDBTap) tap).getDocumentFormat();

        if( inputFormatClass != null )
            jobConf.setInputFormat( inputFormatClass );
        MongoDBConfiguration.configureMongoDB(jobConf, database, collection, host, port);
        
    }

    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {

        String collection = ((MongoDBTap) tap).getCollection();
        String database = ((MongoDBTap) tap).getDatabase();
        int port = ((MongoDBTap) tap).getPort();
        String host = ((MongoDBTap) tap).getHostname();
        documentFormat = ((MongoDBTap) tap).getDocumentFormat();


        jobConf.setOutputFormat(MongoDBOutputFormat.class);
        MongoDBConfiguration.configureMongoDB(jobConf, database, collection, host, port);
        
    }

    public Tuple source(Object key, Object value) {
        return ((MongoDocument) value).getTupleEntry().getTuple();
    }

    // {@inheritDoc}
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {

        outputCollector.collect(documentFormat, tupleEntry);

    }

    /**
     * Provides hook for subclasses to escape or modify any tuple value before it's sent upstream.
     * @param result
     * @return
     */
    protected Tuple cleanTuple(Tuple result)
    {
        return result;
    }

    
}
