package cascading.mongodb;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;


public class MongoTapTest extends CascadingTestCase {

    public MongoTapTest()
    {
        super("MongoTapTest");
    }


    @Test
    public void testBadHostnameForTap()
    {
        Fields fields = new Fields("test");
        MongoDBScheme scheme = new MongoDBScheme(MongoDBOutputFormat.class, fields);
        try
        {
            MongoDBTap tap = new MongoDBTap("foo.gameattain.com", 27107, "testdb", null, null, "test", scheme, null);
        }
        catch (IllegalArgumentException e)
        {
            return;
        }

        Assert.fail("Should have caught IllegalArgumentException.");
    }

    @Test
    public void testDefaultPortAssigment()
    {
        Fields fields = new Fields("source", "title", "summary");
        MongoDBScheme scheme = new MongoDBScheme(MongoDBOutputFormat.class, fields);
        MongoDBTap tap = new MongoDBTap("localhost", "testdb", "test", scheme, null);

        Assert.assertNotNull(tap);
    }

    //TODO: Remove this before publicizing
    @Test
    public void testGameAttainConnectivity()
    {
        Fields fields = new Fields("source", "title", "summary");
        MongoDBScheme scheme = new MongoDBScheme(MongoDBOutputFormat.class, fields);
        MongoDBTap tap = new MongoDBTap("flame.mongohq.com", 27067, "gameattain", null, null, "test", scheme, null);

        Assert.assertNotNull(tap);
    }
}
