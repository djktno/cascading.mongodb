package cascading.mongodb;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
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

    private Fields fields[];
    private Class<? extends OutputFormat> outputFormatClass;
    private String collection;

    public MongoDBScheme(Class<? extends OutputFormat> outputFormatClass, Fields attributeFields) {
        this.outputFormatClass = outputFormatClass;
        this.fields = Fields.fields(attributeFields);
    }    

    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {

        collection = ((MongoDBTap) tap).getCollection();
        //MongoDBOutputFormat.setOutput( jobConf, MongoDBOutputFormat.class, collectionName);

        if (outputFormatClass != null)
            jobConf.setOutputFormat(outputFormatClass);
        
    }

    public Tuple source(Object key, Object value) {
        return ((MongoDBDocumentRecord) value).getTuple();
    }

    // {@inheritDoc}
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {

        BasicDBObject document = new BasicDBObject();

        for (int i = 0; i < fields.length; i++) {
            Fields field = fields[i];
            TupleEntry values = tupleEntry.selectEntry(field);

            for (int j = 0; j < values.getFields().size(); j++) {
                Fields fields = values.getFields();
                Tuple tuple = values.getTuple();

                document.put(fields.get(j).toString(), tuple.getString(j));
            }
        }

        outputCollector.collect(null, document);

//        Tuple result = tupleEntry.selectTuple(getSinkFields());
//        result = cleanTuple(result);
//        outputCollector.collect(new MongoDBDocumentRecord(tupleEntry), null);

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
