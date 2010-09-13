package cascading.mongodb.document;

import cascading.mongodb.document.DefaultMongoDocument;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import java.util.ArrayList;
import java.util.List;

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

    @Override
    public void readFields(BasicDBObject document) throws MongoException {

        //This demonstrates how to read this type of document.

        Fields fields = new Fields("name", "article");
        Tuple tuple = new Tuple();

        String article = "";
        String name = "";

        List games = (List) document.get("games");
        for (Object o : games)
        {
            BasicDBObject game = (BasicDBObject) o;
            List articles = (List) game.get("articles");
            if (null != articles && articles.size() > 0) article = (String) articles.get(0);
            name = game.getString("name");
        }

        tuple.add(name);
        tuple.add(article);

        //Might need to change this to a tupleEntry[] to account for output
        //of more than one tuple per document read.
        tupleEntry = new TupleEntry(fields, tuple);
    }


//example
    //4c8d617acb64810588000746        [{ "articles" : [ "Star Trek Online, often abbreviated as STO, is a massively multiplayer online role-playing game (MMORPG) developed by Cryptic Studios based on the popular Star Trek series created by Gene Roddenberry. The game is set in the 25th century, 30 years after the events of Star Trek Nemesis. Star Trek Online is the first massively multiplayer online role-playing game within the Star Trek franchise and was released on February 2, 2010.\nIn Star Trek Online, each player acts as the captain of his or her own ship. Players are able to play as a starship, controlling the ship's engineering, tactical and science systems by keyboard/mouse or using an on-screen console. Players can also \"beam down\" and move around as a player character in various settings with access to weapons and specific support and combat skills relating to their own character's class. The two combat systems are intertwined throughout the game: Away-team missions feature fast paced \"run and gun\" combat, while Space combat stresses the long-term tactical aspect of combat between capital ships, both are offered in concert with the Star Trek storyline and entails your own positioning, your team's positioning in consideration of"] , "name" : "Star Trek Online"}]

}
