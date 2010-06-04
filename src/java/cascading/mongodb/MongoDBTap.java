package cascading.mongodb;

import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Date: May 21, 2010
 * Time: 8:23:51 PM
 */
public class MongoDBTap extends Tap {

    static final Logger log = Logger.getLogger(MongoDBTap.class.getName());

    private static final String SCHEME = "mongodb:/";
    private static final int DEFAULT_MONGO_PORT = 27017;

    private transient Mongo m;
    private transient DB db;
    private transient DBAddress dbAddress;

    private String collection;
    private String connectionUrl;

    private String hostname;
    private String database;
    private int port;

    private String username;
    private char[] password;

    private CollectionDescriptor collectionDescriptor;

    public MongoDBTap(String hostname, int port, String database, String username, char[] password, String collection, MongoDBScheme scheme, CollectionDescriptor descriptor) {
        super(scheme);

        this.connectionUrl = hostname + "/" + database + "/" + collection;
        this.collectionDescriptor = descriptor;
        this.collection = collection;
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;

        initMongo();
        initDBAddress();
    }

    public MongoDBTap(String hostname, String database, String collection, MongoDBScheme scheme, CollectionDescriptor descriptor)
    {
        this(hostname, DEFAULT_MONGO_PORT, database, null, null, collection, scheme, descriptor);
    }

     private void initMongo() {
        if (m == null) {
            m = new Mongo(getDBAddress());
        }
    }

     private void initDBAddress()
    {
        if (dbAddress == null)
        {
            try
            {
                dbAddress = new DBAddress(hostname, port, database);
            }
            catch (UnknownHostException e)
            {
                throw new IllegalArgumentException("hostname is not recognized: " + hostname, e);
            }
        }
    }


    private Mongo getMongo() {

        initMongo();

        return m;
    }

    private DBAddress getDBAddress()
    {
        initDBAddress();
        return dbAddress;
    }

    private DB getDB() {

        log.debug("Requesting params for DB: {db=" + getMongo().getDB(getDBAddress().getDBName()) + "};");
        if (db == null) {
            log.debug("DB was null - requesting refresh.");
            db = getMongo().getDB(getDBAddress().getDBName());

            if (username != null)
            {
                log.debug("Attempting to authenticate user...");
                if (!db.authenticate(username, password))
                {
                    throw new IllegalArgumentException("MongoDBTap: Auth Failed: {username = " + username + ", password = " + Arrays.toString(password) + "};");
                }
                log.debug("Apparently authentication passed for: {username=" + username + "};");
            }
        }

        return db;
    }

    public boolean isSink() {
        return collectionDescriptor != null;
    }

    public boolean isWriteDirect() {
        return true;
    }

    @Override
    public Path getPath() {
        return new Path(SCHEME + connectionUrl.replaceAll(":", "_"));
    }

    @Override
    public TupleEntryIterator openForRead(JobConf jobConf) throws IOException {
        return new TupleEntryIterator(getSourceFields(), new TapIterator(this, jobConf));
    }

    @Override
    public TupleEntryCollector openForWrite(JobConf jobConf) throws IOException {

        if (!isSink())
            throw new TapException("This tap cannot be used as a sink.");

        return new TapCollector(this, jobConf);
    }

    @Override
    public boolean makeDirs(JobConf jobConf) throws IOException {

        if (getDB().collectionExists(collection))
            return true;

        log.debug("Creating collection: {name = " + collection + "};");
        DBCollection coll = getDB().getCollection(collection);


        if (coll != null)
            return true;

        return false;

    }

    @Override
    public boolean deletePath(JobConf jobConf) throws IOException {

        if (!getDB().collectionExists(collection))
            return true;

        log.debug("Removing collection: {name = " + collection + "};");
        getDB().getCollection(collection).drop();

        return true;

    }

    @Override
    public boolean pathExists(JobConf jobConf) throws IOException {

        if (!isSink())
            return true;

        return getDB().collectionExists(collection);
    }

    @Override
    public long getPathModified(JobConf jobConf) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public void sinkInit(JobConf jobConf) throws IOException {

        if (!isSink())
            return;

        log.debug("Sinking to collection: {name=" + collection + "};");

        if (isReplace() && jobConf.get("mapred.task.partition") == null) {
            deletePath(jobConf);
        }

        makeDirs(jobConf);
        super.sinkInit(jobConf);

    }

    @Override
    public String toString() {
        return "MongoDBTap {host=" + getDBAddress().getHost() + ", port=" + getDBAddress().getPort() + ", collection=" + collection + "}";
    }


    public String getCollection() {
        return collection;
    }
}
