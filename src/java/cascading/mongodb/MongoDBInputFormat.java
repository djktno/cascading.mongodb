package cascading.mongodb;

import cascading.mongodb.document.spec.IndexDocument;
import cascading.tuple.TupleEntry;
import com.mongodb.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Date: Jul 28, 2010
 * Time: 9:52:27 PM
 */
public class MongoDBInputFormat<K extends MongoDocument, V extends TupleEntry> implements InputFormat<K, V>, JobConfigurable {

    static final Logger log = Logger.getLogger(MongoDBInputFormat.class.getName());
    private int maxConcurrentReads = 0;
    private int limit = 2000;

    public InputSplit[] getSplits(JobConf jobConf, int chunks) throws IOException {

        chunks = maxConcurrentReads == 0 ? chunks : maxConcurrentReads;

        MongoDBConfiguration dbConf = new MongoDBConfiguration(jobConf);
        int port = dbConf.getPort();
        String host = dbConf.getHost();
        String database = dbConf.getDatabase();
        String collection = dbConf.getCollection();

        Mongo m = MongoWrapper.instance();
        DB db = m.getDB(database);

        DBCollection dbCollection = db.getCollection(collection);

        int count = (int) IndexDocument.getQueryDocument().count(dbCollection);

        if (limit != -1) {
            count = Math.min(limit, count);
        }

        long chunkSize = (count / chunks);

        InputSplit[] splits = new InputSplit[chunks];

        for (int i = 0; i < chunks; i++) {
            MongoDBInputSplit split;

            if (i + 1 == chunks) {
                split = new MongoDBInputSplit(i * chunkSize, count);
            } else
                split = new MongoDBInputSplit(i * chunkSize, i * chunkSize + chunkSize);

            splits[i] = split;
        }

        return splits;
    }


    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {

        //Class inputClass = jobConf.getInputClass();

        return new MongoDBRecordReader((Class<K>) IndexDocument.class, jobConf);

    }

    public void configure(JobConf jobConf) {
        log.info("Configuring the reader for MongoDB.");
    }

    protected class MongoDBRecordReader implements RecordReader<LongWritable, K> {
        private long pos = 0;
        private Class<K> inputClass;
        private JobConf job;
        private DBCursor cursor;

        protected MongoDBRecordReader(Class<K> inputClass, JobConf job) throws IOException {
            this.inputClass = inputClass;
            this.job = job;

            MongoDBConfiguration dbConf = new MongoDBConfiguration(job);
            int port = dbConf.getPort();
            String host = dbConf.getHost();
            String database = dbConf.getDatabase();
            String collection = dbConf.getCollection();

            Mongo m = MongoWrapper.instance();
            DB db = m.getDB(database);

            DBCollection dbcollection = db.getCollection(collection);


            //log.info("Query to " + collection + " using " + dbObject);

            cursor = IndexDocument.getQueryDocument().find(dbcollection);

            log.info(cursor.explain());

        }

        private BasicDBObject getQueryDocument() {
            return new BasicDBObject();
        }

        private DBObject query(DBObject document) {
            return null;
        }


        public boolean next(LongWritable key, K value) throws IOException {

            if (!cursor.hasNext()) {
                return false;
            }

            key.set(pos /*+ split.getStart()*/);
            value.readFields((BasicDBObject) cursor.next());

            pos++;

            return true;

        }

        public LongWritable createKey() {
            return new LongWritable();
        }

        public K createValue() {
            return ReflectionUtils.newInstance(inputClass, job);
        }

        public long getPos() throws IOException {
            return pos;
        }

        public void close() throws IOException {

        }

        public float getProgress() throws IOException {

            // DK - Can't use cursor.length (array mode) after using next() or hasNext() (iterator mode).
            // See http://grepcode.com/file/repo1.maven.org/maven2/org.mongodb/mongo-java-driver/2.0rc1/com/mongodb/DBCursor.java
            // Filed as bug (5128913).
//            if (cursor.length() > 0)
//                return (float) (pos / cursor.length());
//            if (cursor.count() > 0)
//                return (float) (pos / cursor.count());
            return 0;
        }
    }

    protected static class MongoDBInputSplit implements InputSplit {
        private long end = 0;
        private long start = 0;

        public MongoDBInputSplit() {

        }

        public MongoDBInputSplit(long start, long end) {
            this.end = end;
            this.start = start;
        }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(start);
            out.writeLong(end);
        }

        public void readFields(DataInput in) throws IOException {
            start = in.readLong();
            end = in.readLong();
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public long getLength() {
            return end - start;
        }

    }
}
