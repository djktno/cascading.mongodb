package cascading.mongodb;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Date: May 21, 2010
 * Time: 11:16:04 PM
 */
public class MongoDBOutputFormat extends OutputFormat
{
    public static final String OUTPUT_COLLECTION = "";

    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
