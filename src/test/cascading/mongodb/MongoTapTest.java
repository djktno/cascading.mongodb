package cascading.mongodb;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;


public class MongoTapTest extends CascadingTestCase {


    @Test
    public void testTapConfiguration() {

        Fields fields = new Fields("source", "title", "summary");
        MongoDBScheme scheme = new MongoDBScheme(MongoDBOutputFormat.class, fields);
        MongoDBTap tap = new MongoDBTap("testdb", "foo", scheme);

        Assert.assertNotNull(tap);
        
    }
}
