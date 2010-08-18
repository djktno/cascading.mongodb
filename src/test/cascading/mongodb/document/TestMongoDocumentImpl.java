package cascading.mongodb.document;

import cascading.mongodb.MongoDocument;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

/**
 * Date: Jun 16, 2010
 * Time: 1:44:53 PM
 */
public class TestMongoDocumentImpl implements MongoDocument
{
    private BasicDBObject document = new BasicDBObject();
    private Fields fields = new Fields("letter", "number", "symbol");

    public void write(TupleEntry tupleEntry) throws MongoException {

        Tuple tuple = tupleEntry.selectTuple(fields);

        for (int i = 0; i < tuple.size(); i++)
        {
            document.put((String) fields.get(i), tuple.get(i));
        }

    }

    public void readFields(BasicDBObject document) throws MongoException {
        
    }

    public BasicDBObject getDocument() {
        return document;
    }

    public void setDocument(BasicDBObject document) throws MongoException {
        this.document = document;
    }

    public TupleEntry getTupleEntry() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
