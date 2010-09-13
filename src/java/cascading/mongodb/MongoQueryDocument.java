package cascading.mongodb;

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
}
