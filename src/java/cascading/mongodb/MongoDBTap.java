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

/**
 * Date: May 21, 2010
 * Time: 8:23:51 PM
 */
public class MongoDBTap extends Tap {

    static final Logger log = Logger.getLogger(MongoDBTap.class.getName());

    private static final String SCHEME = "mongodb:/";
    private static final int DEFAULT_MONGO_PORT = 27017;

    private transient DB db;
    private transient DBAddress dbAddress;

    private String collection;
    private String connectionUrl;

    private String hostname;
    private String database;
    private int port;

    private String username;
    private char[] password;

    private boolean isAuthenticated = false;

    private MongoDocument documentFormat;

    /**
     * Standard constructor for using MongoDB as a source only.
     *
     * @param hostname
     * @param port
     * @param database
     * @param collection
     * @param username
     * @param password
     * @param scheme
     */
    public MongoDBTap(String hostname, int port, String database, String collection, String username, char[] password, MongoDBScheme scheme) {
        super(scheme);
        this.connectionUrl = hostname + "/" + database + "/" + collection;
        this.collection = collection;
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;

        initMongo();
    }

    /**
     * Constructor for using the tap as a sink (includes sink model object)
     *
     * @param hostname
     * @param port
     * @param database
     * @param collection
     * @param username
     * @param password
     * @param scheme
     * @param documentDescriptor
     */
    public MongoDBTap(String hostname, int port, String database, String collection, String username, char[] password, MongoDBScheme scheme, MongoDocument documentDescriptor) {
        this(hostname, port, database, collection, username, password, scheme);
        this.documentFormat = documentDescriptor;
    }

    /**
     * Constructor used for host with database on standard mongodb port.
     *
     * @param hostname
     * @param database
     * @param collection
     * @param username
     * @param password
     * @param scheme
     */
    public MongoDBTap(String hostname, String database, String collection, String username, char[] password, MongoDBScheme scheme) {
        this(hostname, DEFAULT_MONGO_PORT, database, collection, username, password, scheme);
    }

    /**
     * Convenience constructor for using mongodb as a source when no authentication is necessary.
     *
     * @param hostname
     * @param port
     * @param database
     * @param collection
     * @param scheme
     */
    public MongoDBTap(String hostname, int port, String database, String collection, MongoDBScheme scheme) {
        this(hostname, port, database, collection, null, null, scheme);
    }

    /**
     * Convenience constructor for use of MongoDB as a sink when no security is present on the database.
     *
     * @param hostname
     * @param port
     * @param database
     * @param collection
     * @param scheme
     * @param descriptor
     */
    public MongoDBTap(String hostname, int port, String database, String collection, MongoDBScheme scheme, MongoDocument descriptor) {
        this(hostname, port, database, collection, null, null, scheme, descriptor);
    }

    private void initMongo() {


        MongoWrapperFactoryFunctor functor = new MongoWrapperFactoryFunctor() {

            public Mongo makeInstance(String hostname, int port, String database) throws UnknownHostException {
                return new Mongo(new DBAddress(hostname, port, database));
            }
        };

        MongoWrapper.setFactory(functor);

        try {
            Mongo m = MongoWrapper.instance(hostname, port, database);
            if (null == username);
            else {
               m.getDB(database).authenticate(username, password);
            }

        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Didn't recognize hostname for MongoDB.", e);
        }


    }

    DB getDBReference() {

        DB db = null;
        log.debug("Requesting params for DB: {db=" + database + "};");
        try {
            db = MongoWrapper.instance(hostname, port, database).getDB(database);
        } catch (UnknownHostException e) {
            throw new RuntimeException("This should not happen.");
        }

        return db;
    }

    public boolean isSink() {
        return documentFormat != null;
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
            throw new TapException("This tap cannot be used as a sink, no document descriptor defined.");

        return new TapCollector(this, jobConf);
    }

    @Override
    public boolean makeDirs(JobConf jobConf) throws IOException {

        if (getDBReference().collectionExists(collection))
            return true;

        log.debug("Creating collection: {name = " + collection + "};");
        DBCollection coll = getDBReference().getCollection(collection);


        if (coll != null)
            return true;

        return false;

    }

    @Override
    public boolean deletePath(JobConf jobConf) throws IOException {

        if (!getDBReference().collectionExists(collection))
            return true;

        log.debug("Removing collection: {name = " + collection + "};");
        getDBReference().getCollection(collection).drop();

        return true;

    }

    @Override
    public boolean pathExists(JobConf jobConf) throws IOException {

        if (!isSink())
            return true;

        return getDBReference().collectionExists(collection);
    }

    @Override
    public long getPathModified(JobConf jobConf) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public void sinkInit(JobConf jobConf) throws IOException {

        if (!isSink())
            return;

        MongoDBConfiguration.configureMongoDB(jobConf, database, collection, hostname, port);

        log.debug("Sinking to collection: {name=" + collection + "};");

        if (isReplace() && jobConf.get("mapred.task.partition") == null) {
            deletePath(jobConf);
        }

        makeDirs(jobConf);
        super.sinkInit(jobConf);

    }

    @Override
    public String toString() {
        return "MongoDBTap {host=" + hostname + ", port=" + port + ", collection=" + collection + "}";
    }


    public String getCollection() {
        return collection;
    }

    public String getDatabase()
    {
        return database;
    }

    public MongoDocument getDocumentFormat() {
        return documentFormat;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }
}
