package cascading.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * Date: May 19, 2010
 * Time: 9:21:59 PM
 */
public class MongoTest
{
    private Mongo m;

    @Before
    public void setup() throws Exception
    {
       m = new Mongo("localhost"); 
    }

    @Test
    public void testMongoConnection() throws Exception
    {
        Mongo pm = new Mongo("localhost");

        Assert.assertNotNull(pm);
        System.out.println("Established connection with local Mongo instance.");
    }

    @Test
    public void testGetCollection()
    {
        DB db = m.getDB("testdb");

        Assert.assertNotNull(db);

        Set<String> collection = db.getCollectionNames();
        CollectionWrapper cw = new CollectionWrapper(collection);
        cw.forEach(new Closure()
        {
           public void exec(Object o)
           {
                System.out.println((String) o);   
           }
        });
    }

    @Test
    public void testGetTestCollection()
    {
        DB db = m.getDB("testdb");

        Assert.assertNotNull(db);

        DBCollection collection = db.getCollection("foo");

        Assert.assertNotNull(collection);
    }

    @Test
    public void testInsertDoc()
    {
        DB db = m.getDB("testdb");
        DBCollection collection = db.getCollection("foo");

        BasicDBObject doc = new BasicDBObject();
        doc.put("source", "http://www.twitter.com");
        doc.put("image", "http://www.google.com/image");
        doc.put("timestamp", System.currentTimeMillis());
        doc.put("summary", "");
        doc.put("title", "Title of this Tweet");

        collection.insert(doc);
    }


    
}
