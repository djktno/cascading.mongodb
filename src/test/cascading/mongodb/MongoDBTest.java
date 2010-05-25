package cascading.mongodb;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
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

/**
 * Date: May 25, 2010
 * Time: 12:28:27 AM
 */
public class MongoDBTest extends ClusterTestCase {
    String inputFile = "src/test/data/testdata.txt";
    private static final String COLLECTION = "cascading.mongodb.test";

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
        db = mongo.getDB("cascading.mongodb.testdb");
        collection = db.getCollection(COLLECTION);
    }

    @Override
    public void tearDown() throws IOException {
        collection.drop();
        db.dropDatabase();
        mongo.close();
    }

    @Test
    public void testMongoDBWrites() throws Exception {

        //Create new document from source data.

        Fields tupleFields = new Fields("source", "title", "summary");

        Tap source = new Lfs(new TextLine(), inputFile);
        Pipe parsePipe = new Each("insert", new Fields("line"), new RegexSplitter(tupleFields, "\\s"));

        String[] attributeNames = {"source", "title", "summary"};
        CollectionDescriptor desc = new CollectionDescriptor(COLLECTION, attributeNames);

        Tap mongoTap = new MongoDBTap("localhost", "testdb", COLLECTION, new MongoDBScheme(MongoDBOutputFormat.class, tupleFields), desc);

        Flow parseFlow = new FlowConnector(getProperties()).connect(source, mongoTap, parsePipe);

        parseFlow.complete();

        verifySink(parseFlow, 3);
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
