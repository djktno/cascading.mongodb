package cascading.mongodb.document;

import cascading.mongodb.document.DefaultMongoDocument;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * User: djktno
 * Date: Sep 11, 2010
 * Time: 11:29:15 PM
 */
public class GameDocument extends DefaultMongoDocument
{
    BasicDBObject format;
    String title;
    String releaseDate;
    String article;

    public GameDocument()
    {
        
    }

    public GameDocument(Fields selector)
    {
        super(selector);
        this.format = new BasicDBObject();

        List<BasicDBObject> gameList = new ArrayList<BasicDBObject>();
        BasicDBObject game = new BasicDBObject();
        game.put("name", "");
        game.put("release_date", "");
        game.put("article", "");

        gameList.add(game);

        this.format.append("games", gameList);
    }

    @Override
    public BasicDBObject getDocument() {
        return format;
    }

    public void setTitle(String title)
    {
        List games = ((List) format.get("games"));
        BasicDBObject game = (BasicDBObject) games.get(0);
        game.put("name", title);
    }

    public void setReleaseDate(String releaseDate)
    {
        List games = ((List) format.get("games"));
        BasicDBObject game = (BasicDBObject) games.get(0);
        game.put("release_date", releaseDate);
    }

    public void write(TupleEntry tupleEntry) throws MongoException {


        for (int j = 0; j < selector.size(); j++) {
            Tuple tuple = tupleEntry.selectTuple(selector);

            document.put(selector.get(j).toString(), tuple.getString(j));
        }


    }

    public void readFields(BasicDBObject document) throws MongoException {

        Set<String> keySet = document.keySet();
        if (document.keySet().isEmpty())
            return;

        
        //only interested in a (-n embedded) list called games (which has title and article wrapped in it.
        Fields fields = new Fields("title", "article");
        Tuple tuple = new Tuple();

        BasicDBList d = (BasicDBList) document.get("games");
        BasicDBObject o = (BasicDBObject) d.get("0");

        String articles = o.getString("articles");
        String name = o.getString("name");

        //To'e up...from the flo' up.

//        fields.append(new Fields("title", "article"));
        tuple.add(name);
        //tuple.add(articles);

//        for (int i = 0; i < d.size(); i++) {
//
//            Object value = d.get(i);
//
//            //fields.append(new Fields(key));
//            tuple.add(value);
//        }

        tupleEntry = new TupleEntry(fields, tuple);

    }


}
