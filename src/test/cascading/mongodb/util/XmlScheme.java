package cascading.mongodb.util;

import cascading.scheme.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Created by IntelliJ IDEA.
 * User: djktno
 * Date: Apr 16, 2010
 * Time: 10:02:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class XmlScheme extends TextLine
{
    public XmlScheme(Fields fields)
    {
        super(fields);
    }

    public void sourceInit(Tap tap, JobConf conf)
    {
        if (isXml(XmlInputFormat.getInputPaths(conf)))
            conf.setInputFormat(XmlInputFormat.class);
        else
            conf.setInputFormat(TextInputFormat.class);
    }

    private boolean isXml(Path[] paths)
    {
        boolean isXml = paths[0].getName().endsWith(".xml");

        for (int i = 1; i < paths.length; i++)
        {
            if (isXml != paths[i].getName().endsWith(".xml"))
                throw new IllegalStateException("Can't mix xml and non xml files.");
        }

        return isXml;
    }
}