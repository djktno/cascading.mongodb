package cascading.mongodb;

import com.mongodb.BasicDBObject;
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

    public int count(DBCollection collection) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void executeOn(DBCollection collection) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int position() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public BasicDBObject next() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean hasNext() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
