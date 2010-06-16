package cascading.mongodb;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;


public class MongoTapTest extends CascadingTestCase {

    public MongoTapTest()
    {
        super("MongoTapTest");
    }


    @Test
    //TODO:  This one's failing, but it's a weak test anyway.
    public void testBadHostnameForTap()
    {
        Fields fields = new Fields("test");
        MongoDBScheme scheme = new MongoDBScheme(MongoDBOutputFormat.class);
        try
        {
            MongoDBTap tap = new MongoDBTap("foo.bbbbbbbb.gov", 27107, "testdb", null, null, new char[0], scheme, null);
        }
        catch (IllegalArgumentException e)
        {
            return;
        }

        Assert.fail("Should have caught IllegalArgumentException.");
    }
}
