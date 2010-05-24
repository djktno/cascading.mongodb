package com.gameattain.dpa;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Date: May 21, 2010
 * Time: 8:32:20 PM
 */
public class MongoDBScheme extends Scheme {
    private static final Logger log = Logger.getLogger(MongoDBScheme.class.getName());

    private Fields[] fields;

    public MongoDBScheme(Fields[] fields) {
        this.fields = fields;
    }

    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {
        jobConf.setOutputFormat(MongoDBOutputFormat.class);
        jobConf.setOutputKeyClass();
        jobConf.setOutputValueClass(BasicDBObject.class);
    }

    public Tuple source(Object o, Object o1) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

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

    }

    private void setSourceSink(Fields keyFields, Fields[] columnFields) {
        Fields allFields = Fields.join(keyFields, Fields.join(columnFields)); // prepend

        setSourceFields(allFields);
        setSinkFields(allFields);
    }                           
}