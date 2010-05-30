package cascading.mongodb;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import java.util.Map;
import java.util.Set;

/**
 * Date: May 24, 2010
 * Time: 10:55:54 PM
 */
public class MongoDBDocumentRecord implements MongoWritable
{
    private TupleEntry tupleEntry;

    public MongoDBDocumentRecord()
    {}

    public MongoDBDocumentRecord(TupleEntry tupleEntry)
    {
        this.tupleEntry = tupleEntry;
    }

    public void setTupleEntry(TupleEntry tupleEntry)
    {
        this.tupleEntry = tupleEntry;
    }

    public TupleEntry getTupleEntry()
    {
        return tupleEntry;
    }

    public Tuple getTuple()
    {
        return tupleEntry.getTuple();
    }

    

    public void write(BasicDBObject document) throws MongoException {

        Tuple tuple = tupleEntry.getTuple();

        for (int i = 0; i < tuple.size(); i++)
        {
            document.put(tuple.getString(i), null);
        }

    }

    public void readFields(BasicDBObject document) throws MongoException {

        Tuple tuple = new Tuple();

        Set<Map.Entry<String, Object>> entries = document.entrySet();
        for(Map.Entry entry : entries)
        {
            String fieldName = (String) entry.getKey();
            tuple.add((Comparable) fieldName);

        }
    }
}
