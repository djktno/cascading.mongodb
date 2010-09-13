package cascading.mongodb.document;

import cascading.mongodb.MongoQueryDocument;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import java.awt.*;

/**
 * User: djktno
 * Date: Sep 13, 2010
 * Time: 12:09:33 AM
 */
public class SelectGameTrainingQuery implements MongoQueryDocument
{
    private int skipValue = 0;
    private DBCursor cursor;
    private static BasicDBObject projection = new BasicDBObject().append("games.name", 1).append("games.articles", 1);
    private static BasicDBObject base = new BasicDBObject().append("games.release_date", new BasicDBObject("$gte", "2009"));
    private static BasicDBObject sort = new BasicDBObject().append("games.name", -1);

    public void executeOn(DBCollection collection)
    {
        cursor = collection.find(SelectGameTrainingQuery.base, SelectGameTrainingQuery.projection).sort(SelectGameTrainingQuery.sort).skip(skipValue);
    }

    public DBCursor find(DBCollection collection) {
        if (cursor == null)
        {
            executeOn(collection);
        }

        return cursor;
    }

    public long count(DBCollection collection) {
        return collection.find(SelectGameTrainingQuery.base).count();
    }

    public void skip(int skip) {
        this.skipValue = skip;
    }

    public BasicDBObject next()
    {
        return (BasicDBObject) cursor.next();
    }
}
