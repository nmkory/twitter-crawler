import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.queryparser.classic.QueryParser;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;

import static java.util.Objects.requireNonNull;

public class LuceneBuilder {
    //Default directory for the index from LuceneBuilder class path
    static final String INDEX_DIR = "../index";
    //Default directory for the json files from LuceneBuilder class path
    static final String JSON_DIR = "../Crawler";

    //Non static variables that should be accessed through instance of this class only
    String indexDir;
    String JSONdir;
    IndexWriter indexWriter = null;
    IndexSearcher searcher = null;
    QueryParser parser = null;

    /**
     * Default constructor
     */
    public LuceneBuilder() {
        this.indexDir = INDEX_DIR;
        this.JSONdir = JSON_DIR;
    }  //LuceneBuilder()

    /**
     * Parameterized constructor
     * @param indexDir the directory where we will store the Lucene index
     * @param JSONdir the directory where we will get the JSON files farmed from Twitter
     */
    @SuppressWarnings("unused")
    public LuceneBuilder(String indexDir, String JSONdir) {
        this.indexDir = indexDir;
        this.JSONdir = JSONdir;
    }  //LuceneBuilder(String indexDir, String JSONdir)


    /**
     * getIndexWriter() creates or opens the index for writing
     * @param indexDir the directory where will will create or open the index
     * @return the IndexWriter for our Lucene index
     * @throws IOException when directory open is bad
     */
    public IndexWriter getIndexWriter(String indexDir) throws IOException {
        //Directory indexDir = new RAMDirectory(); //use to put directory in RAM
        Directory dir = FSDirectory.open(new File(indexDir).toPath());
        IndexWriterConfig luceneConfig = new IndexWriterConfig(new StandardAnalyzer());

        return(new IndexWriter(dir, luceneConfig));
    }  //getIndexWriter()

    /**
     * parseJSONFiles() Opens the JSON files in the directory provided and adds them to the JSON Array List
     * @param JSONdir the directory where the JSON files are located
     * @return an Array List of JSON Objects of Tweets
     * @throws IOException when opening the file directory goes bad
     */
    public ArrayList <JSONObject> parseJSONFiles(String JSONdir) throws IOException {
        BufferedReader br;
        String jsonString;
        JSONParser parser = new JSONParser();
        ArrayList <JSONObject> jsonArrayList = new ArrayList<>();

        //Go the Crawler directory
        File dir = new File(JSONdir);

        //Get the file names for the json objects therein
        File[] jsonFiles = dir.listFiles((dir1, filename) -> filename.endsWith(".json"));

        //For each new line delimited json file in the file directory
        for (File jsonFile : requireNonNull(jsonFiles)) {

            //Set the buffered reader
            br = new BufferedReader(new FileReader(jsonFile.getCanonicalPath()));

            //While read lines are not null
            while ((jsonString = br.readLine()) != null) {
                //Try to parse it
                try {
                    jsonArrayList.add((JSONObject) parser.parse(jsonString));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }
            //Close the open buffered reader and open the next json file
            br.close();
        }
        return jsonArrayList;
    }  //parseJSONFiles()

    /**
     * buildDocument() builds a document for the index from the JSON object of a collected Tweet
     * @param jsonObject is a farmed Tweet
     * @return a document of that Tweet
     */
    public Document buildDocument(JSONObject jsonObject) {
        String url;
        Document doc = new Document();

        // Add as text field so the text is searchable, per TA
        doc.add(new TextField("text",(String) jsonObject.get("Text"), Field.Store.YES));
        doc.add(new StoredField("user",(String) jsonObject.get("User")));
        doc.add(new StoredField("datetime",(String) jsonObject.get("Datetime")));
        doc.add(new StoredField("latitude", (double) jsonObject.get("Latitude")));
        doc.add(new StoredField("longitude", (double) jsonObject.get("Longitude")));
        
        if ((url = (String) jsonObject.get("URL")) != null) {
            doc.add(new StoredField("url", url));
        }
        // Add as doc values field so we can compute range facets and sort and score
        doc.add(new NumericDocValuesField("timestamp", (long) jsonObject.get("Timestamp")));

        // Add as numeric field so we can drill-down
        doc.add(new LongPoint("time", (long) jsonObject.get("Timestamp")));
        doc.add(new DoublePoint("lat", (double) jsonObject.get("Latitude")));
        doc.add(new DoublePoint("long", (double) jsonObject.get("Longitude")));

        return doc;
    }  //buildDocument()

    /**
     * indexTweets() adds json Tweets to the Lucene index
     * @param jsonArrayList is an Array List of JSON objects, each one is a Tweet
     * @param indexWriter is the index writer for the Lucene index we're working with
     */
    public void indexTweets(ArrayList <JSONObject> jsonArrayList, IndexWriter indexWriter) {
        Document doc;
        for (JSONObject jsonObject : jsonArrayList) {
            doc = buildDocument(jsonObject);
            try {
                indexWriter.addDocument(doc);
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }
    }  //indexTweets()

    /**
     * search() looks in the Lucene index based on the search term and returns what it found. Results are boosted if
     * they are more recent.
     * @param q a string that is what the user is searching for. Looks at Tweet text.
     * @param numResults the number of max results we want.
     * @return TopDocs object that has the search results in order.
     * @throws org.apache.lucene.queryparser.classic.ParseException a ParseException thrown when the Lucene parser goes
     * haywire (this is a different parse exception from JSON so added the library directly
     * @throws IOException is thrown when the index cannot be located
     */
    public TopDocs search(String q, int numResults) throws org.apache.lucene.queryparser.classic.ParseException,
            IOException {
        Query termQuery = parser.parse(q);
        // Build a boost based on the timestamp doc value we retained and indexed
        FunctionQuery dateBoost = new FunctionQuery (new LongFieldSource("timestamp"));
        // Use default boost values
        CustomScoreQuery query = new CustomScoreQuery(termQuery, dateBoost);
        return searcher.search(query, numResults);
    }  //search()

    /**
     * buildSearcher() builds the IndexSearcher and Parser out of the existing index we created. Uses "text" from
     * Tweets are the default field when searching.
     * @throws IOException when opening the index goes wrong
     */
    public void buildSearcher() throws IOException {
        IndexReader rdr = DirectoryReader.open(FSDirectory.open(new File(indexDir).toPath()));
        searcher = new IndexSearcher(rdr);
        parser = new QueryParser("text", new StandardAnalyzer());
    }  //buildSearcher()

    /**
     * buildIndex() uses the directory information to build or append a Lucene index
     * @throws IOException for any number of issues related to opening files that cannot be found
     */
    @SuppressWarnings("unused")
    public void buildIndex() throws IOException {
        indexWriter = getIndexWriter(indexDir);
        ArrayList <JSONObject> jsonArrayList = parseJSONFiles(JSONdir);
        indexTweets(jsonArrayList, indexWriter);
        indexWriter.close();
    } //buildIndex()

    /**
     * main used to test the LuceneBuilder class
     * @param args from command line, should expect nothing
     * @throws IOException when opening or closing a file goes wrong
     */
    public static void main(String[] args) throws IOException, org.apache.lucene.queryparser.classic.ParseException {
        LuceneBuilder luceneIndex = null;  //String indexDir, String JSONdir
        if (args.length == 2) {
            try {
                System.out.println("Generating Lucene index in directory "+ args[0] +" using .json files located in directory "+ args[1] +".");
                luceneIndex = new LuceneBuilder(args[0], args[1]);
                luceneIndex.buildIndex();
            } catch (IOException ex) {
                ex.printStackTrace();
                System.out.println("Invalid input. Check command line parameters to make sure directory locations are correct.");
                System.exit(-1);
            }
        }
        else{
            try {
                System.out.println("Default run. Generating Lucene index in directory "+ "/index" +" using .json files located in directory "+ "/Crawler" +".");
                luceneIndex = new LuceneBuilder();
                luceneIndex.buildIndex();
            } catch (IOException ex) {
                ex.printStackTrace();
                System.out.println("Invalid input. Check the file structure to make sure Lucene index can use directory "+ INDEX_DIR +" and .json files are located in directory "+ JSON_DIR +".");
                System.exit(-1);
            }
        }

        luceneIndex.buildSearcher();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String query;

        System.out.println("Search the index for top 10 results by inputting something. Type 'exit' to exit.");
        System.out.print("Search for: ");
        while (!(query = reader.readLine()).equals("exit")) {

            TopDocs hits = luceneIndex.search(query, 10);

            if (hits.scoreDocs.length == 0) {
                System.out.println("No farmed Tweets match that search term.");
                System.out.println();
            }

            for (ScoreDoc scoreDoc : hits.scoreDocs) {
                Document doc = luceneIndex.searcher.doc(scoreDoc.doc);
                System.out.println(doc.get("text"));
                System.out.println("@" + doc.get("user"));
                System.out.println(doc.get("datetime"));
                System.out.println("Latitude: " + doc.get("latitude"));
                System.out.println("Longitude: " + doc.get("longitude"));
                if (doc.get("url") != null) {
                    System.out.println(doc.get("url"));
                }
                System.out.println();
            }
            System.out.println("Search the index again for top 10 results by inputting something. Type 'exit' to exit.");
            System.out.print("Search for: ");
        }
        System.exit(0);
    }  //main()
}  //public class LuceneBuilder
