package cascading.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;


/**
 * User: djktno
 * Date: Sep 13, 2010
 * Time: 12:06:46 AM
 */
public interface MongoQueryDocument
{
    DBCursor find(DBCollection collection);
    void skip(int skip);
    int count(DBCollection collection);
    void executeOn(DBCollection collection);
    int position();
    BasicDBObject next();
    boolean hasNext();
}
