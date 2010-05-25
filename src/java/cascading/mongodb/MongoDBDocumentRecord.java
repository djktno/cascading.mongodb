package cascading.mongodb;

import cascading.tuple.Tuple;
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
    private Tuple tuple;

    public MongoDBDocumentRecord()
    {}

    public MongoDBDocumentRecord(Tuple tuple)
    {
        this.tuple = tuple;
    }

    public void setTuple(Tuple tuple)
    {
        this.tuple = tuple;
    }

    public Tuple getTuple()
    {
        return tuple;
    }

    

    public void write(BasicDBObject document) throws MongoException {

        for (int i = 0; i < tuple.size(); i++)
        {
            document.put(tuple.getString(i), null);
        }

    }

    public void readFields(BasicDBObject document) throws MongoException {

        tuple = new Tuple();

        Set<Map.Entry<String, Object>> entries = document.entrySet();
        for(Map.Entry entry : entries)
        {
            String fieldName = (String) entry.getKey();
            tuple.add((Comparable) fieldName);

        }
    }
}
