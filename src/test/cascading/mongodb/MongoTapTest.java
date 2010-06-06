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
        MongoDBScheme scheme = new MongoDBScheme(MongoDBOutputFormat.class);
        try
        {
            MongoDBTap tap = new MongoDBTap("foo.nist.gov", 27107, "testdb", null, null, new char[0], scheme, null);
        }
        catch (IllegalArgumentException e)
        {
            return;
        }

        Assert.fail("Should have caught IllegalArgumentException.");
    }
}
