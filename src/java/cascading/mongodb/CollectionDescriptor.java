package cascading.mongodb;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Date: May 24, 2010
 * Time: 11:17:40 PM
 */
public class CollectionDescriptor implements Serializable
{
    String collectionName;
    String[] attributeNames;

    public CollectionDescriptor(String collectionName)
    {
        this.collectionName = collectionName;
    }

    public CollectionDescriptor(String collectionName, String[] attributeNames)
    {
        this.collectionName = collectionName;
        this.attributeNames = attributeNames;
    }

    public String toString()
    {
        return "CollectionDescriptor: {collectionName = " + collectionName + ", attributeNames = {"
                + (attributeNames == null ? null : Arrays.asList(attributeNames)) + "} };";
    }
}
