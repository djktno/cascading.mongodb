package cascading.mongodb;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import java.awt.*;

/**
 * User: djktno
 * Date: Sep 13, 2010
 * Time: 12:06:19 AM
 */
public class DefaultMongoQueryDocument implements MongoQueryDocument
{
    private int skipValue = 0;

    public DBCursor find(DBCollection collection) {
        return collection.find().skip(skipValue);
    }

    public void skip(int skip) {
        this.skipValue = skip;
    }
}
