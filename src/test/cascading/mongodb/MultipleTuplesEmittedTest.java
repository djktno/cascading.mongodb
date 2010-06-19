package cascading.mongodb;

/**
 * Date: Jun 16, 2010
 * Time: 1:43:54 PM
 */
public class MultipleTuplesEmittedTest extends MongoDBTest {

//    String inputFile = "src/test/data/testxml.xml";
//    String[][] namespaces = {new String[]{"x", "http://cascading.mongodb.com/test"}};
//
//    //TODO: Test still broken...doesn't select any xml from testxml.xml document.
//    public void testMultipleEmittedTuplesInFlowGetToDB() throws Exception {
//        //Create new document from source data.
//        Pipe parsePipe = new Pipe("insert");
//
//        Fields tupleFields = new Fields("letter", "number", "symbol");
//        Fields selector = new Fields("letter", "number");
//
//        Tap source = new Lfs(new XmlScheme(new Fields("document")), inputFile);
//
//        Debug debug = new Debug(true);
//        parsePipe = new Each(parsePipe, debug);
//
//        parsePipe = new Each(parsePipe, new Fields("document"), new XPathGenerator(new Fields("letter"), namespaces, "/"), new Fields("letter"));
//
//
//        Tap mongoTap = new MongoDBTap(HOST, PORT, DB, COLLECTION, new MongoDBScheme(MongoDBOutputFormat.class), new TestMongoDocumentImpl());
//
//        Properties props = new Properties();
//        props.put("xmlinput.start", "<entity ");
//        props.put("xmlinput.end", "</entity>");
//        FlowConnector.setDebugLevel(props, DebugLevel.VERBOSE);
//        Flow parseFlow = new FlowConnector(props).connect(source, mongoTap, parsePipe);
//
//        parseFlow.complete();
//    }


}
