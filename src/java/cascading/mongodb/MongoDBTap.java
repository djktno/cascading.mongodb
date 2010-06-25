package cascading.mongodb;

import cascading.flow.Flow;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.mongodb.*;
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
    private boolean isMongoInitialized = false;

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
     * Convenience constructor for use of MongoDB as a sink when no authentication is necessary.
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

        log.info("initMongo(): Initializing Mongo {user = " + username + ", hostname = " + hostname + ", port = " + port + "};");

        MongoWrapperFactoryFunctor functor = new MongoWrapperFactoryFunctor() {

            public Mongo makeInstance(String hostname, int port, String database) throws UnknownHostException {
                return new Mongo(new DBAddress(hostname, port, database));
            }
        };

        MongoWrapper.setFactory(functor);

        try {
            Mongo m = MongoWrapper.initialize(hostname, port, database);
            MongoWrapper.setInstance(m);
            if (null == username) ;
            else {
                try
                {
                    isAuthenticated = m.getDB(database).authenticate(username, password);
                }
                catch(IllegalStateException e)
                {
                    //we may have called authenticate twice - eat this.
                    log.warn("May have reauthenticated: " + e.getMessage());
                }


            }

            if (MongoWrapper.instance() != null)
            {
                log.info("MongoWrapper {" + MongoWrapper.class.toString() + "} is not null, and neither is the Mongo instance {" + MongoWrapper.instance() + "} while initializing.");
                isMongoInitialized = true;
            }
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Didn't recognize hostname for MongoDB.", e);
        }


    }

    DB getDBReference() {

        log.info("getDBReference: Requesting params for DB: {db=" + database + "};");
        try {

            Mongo m = MongoWrapper.instance();
            log.info("MongoWrapper {" + MongoWrapper.class.toString() + "}.  Mongo {" + m + "} " + ((m == null) ? " is " : " is not ") + " null." );
            db = MongoWrapper.instance().getDB(database);
            log.info("Got database reference: " + db.getName());

        } catch (UnknownHostException e) {
            throw new RuntimeException("Unknown host passed into configuration for MongoDB.");
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

        log.info("makeDirs(): entering");

        if (getDBReference().collectionExists(collection))
            return true;

        log.info("Creating collection: {name = " + collection + "};");
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

        log.info("sinkInit(): entering");

        MongoDBConfiguration.configureMongoDB(jobConf, database, collection, hostname, port);
        initMongo();



        log.info("Sinking to collection: {name=" + collection + "};");

        if (isReplace() && jobConf.get("mapred.task.partition") == null) {
            deletePath(jobConf);
        }

        makeDirs(jobConf);
        super.sinkInit(jobConf);

    }

    @Override
    public void flowInit(Flow flow)
    {
        JobConf conf = flow.getJobConf();
        log.info("MongoDBTap.flowInit(): Initializing flow...");
    }

    @Override
    public String toString() {
        return "MongoDBTap {host=" + hostname + ", port=" + port + ", collection=" + collection + "}";
    }


    public String getCollection() {
        return collection;
    }

    public String getDatabase() {
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
