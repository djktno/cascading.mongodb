package cascading.mongodb;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * Date: May 25, 2010
 * Time: 12:28:27 AM
 */
public class MongoDBTest extends ClusterTestCase {

    String inputFile = "src/test/data/testdata.txt";
    private static final String HOST = "localhost";
    private static final int PORT = 27017;
    private static final String COLLECTION = "cascadingtest";
    private static final String DB = "gameattain";

    //private static final String USERNAME = "gameattain";
    private static final char[] PASSWORD = { 'G', 'A', '.', '2', '0', '0', '9' };

    private static final String USERNAME = null;

    private Mongo mongo;
    private DB db;
    private DBCollection collection;

    public MongoDBTest() {
        super("mongodb tap test", false);
    }

    @Override
    public void setUp() throws IOException {
        super.setUp();

        mongo = new Mongo("localhost");
        db = mongo.getDB(DB);
        collection = db.getCollection(COLLECTION);
    }

    @Override
    public void tearDown() throws IOException {
//        collection.drop();
//        db.dropDatabase();
        mongo.close();
    }


    @Test
    public void testMongoDBWrites() throws Exception {

        //Create new document from source data.
        Pipe parsePipe = new Pipe("insert");
        Thread.sleep(5000);

        Fields tupleFields = new Fields("source", "title", "summary");

        Tap source = new Lfs(new TextLine(), inputFile);
        parsePipe = new Each(parsePipe, new Fields("line"), new RegexSplitter(tupleFields, "\\s"));

        String[] attributeNames = {"source", "title", "summary"};
        CollectionDescriptor desc = new CollectionDescriptor(COLLECTION, attributeNames);

        parsePipe = new Each(parsePipe, DebugLevel.VERBOSE, new Debug());

        Tap mongoTap = new MongoDBTap(HOST, PORT, DB, USERNAME, PASSWORD, COLLECTION, new MongoDBScheme(MongoDBOutputFormat.class, tupleFields), desc);

        Properties props = new Properties();
        props.put(MongoDBConfiguration.OUTPUT_COLLECTION, COLLECTION);
        props.put(MongoDBConfiguration.OUTPUT_DOCUMENT_ATTRIBUTE_NAMES, new String[] { "source", "title", "summary"});
        props.put(MongoDBConfiguration.DATABASE, DB);
        props.put(MongoDBConfiguration.HOSTNAME, HOST);
        props.put(MongoDBConfiguration.PORT, PORT);
        Flow parseFlow = new FlowConnector(props).connect(source, mongoTap, parsePipe);

        parseFlow.complete();

//        verifySink(parseFlow, 3);
    }

    private void verifySink(Flow flow, int expects) throws IOException {
        int count = 0;

        TupleEntryIterator iterator = flow.openSink();

        while (iterator.hasNext()) {
            count++;
            System.out.println("iterator.next() = " + iterator.next());
        }

        iterator.close();

        assertEquals("wrong number of values", expects, count);
    }
}
