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

    public TopDocs search(String q, int numResults) throws org.apache.lucene.queryparser.classic.ParseException,
            IOException {
        Query termQuery = parser.parse(q);
        FunctionQuery dateBoost = new FunctionQuery (new LongFieldSource("timestamp"));
        CustomScoreQuery query = new CustomScoreQuery(termQuery, dateBoost);
        return searcher.search(query, numResults);
    }

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
        LuceneBuilder luceneIndex = new LuceneBuilder();
        //luceneIndex.buildIndex();
        luceneIndex.buildSearcher();
        TopDocs hits = luceneIndex.search("black lives matter", 10);

        for(ScoreDoc scoreDoc : hits.scoreDocs) {
            Document doc = luceneIndex.searcher.doc(scoreDoc.doc);
            System.out.println("score: " + scoreDoc.score + " doc: " + doc);
        }



    }  //main()
}  //public class LuceneBuilder
