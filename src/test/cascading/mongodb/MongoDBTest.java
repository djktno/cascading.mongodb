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
import com.gameattain.FeedDocument;
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
    private static final String DB = "testdb";

    public MongoDBTest() {
        super("mongodb tap test", false);
    }

    @Override
    public void setUp() throws IOException {
        super.setUp();
    }

    @Override
    public void tearDown() throws IOException {
    }


    @Test
    public void testMongoDBWrites() throws Exception {

        //Create new document from source data.
        Pipe parsePipe = new Pipe("insert");

        Fields tupleFields = new Fields("letter", "number", "symbol");
        Fields selector = new Fields("letter", "number");

        Tap source = new Lfs(new TextLine(), inputFile);
        parsePipe = new Each(parsePipe, new Fields("line"), new RegexSplitter(tupleFields, "\\s"));

        Tap mongoTap = new MongoDBTap(HOST, PORT, DB, COLLECTION, new MongoDBScheme(MongoDBOutputFormat.class), new DefaultMongoDocument(selector));

        Properties props = new Properties();
        Flow parseFlow = new FlowConnector(props).connect(source, mongoTap, parsePipe);

        parseFlow.complete();

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
