package cascading.mongodb;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import java.io.Serializable;

/**
 * MongoDocument is an interface used to allow the framework to
 * interact with custom format documents.  Classes that represent
 * a document's format should implement this interface.
 */
public interface MongoDocument extends Serializable
{
    public void write(TupleEntry tupleEntry) throws MongoException;
    public void readFields(BasicDBObject document) throws MongoException;
    public BasicDBObject getDocument();
    void setDocument(BasicDBObject document) throws MongoException;
    public TupleEntry getTupleEntry();


}
