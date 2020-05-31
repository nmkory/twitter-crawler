import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.FSDirectory;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;

public class LuceneBuilder {
    static final String INDEX_DIR = "../index";
    static final String JSON_DIR = "../Crawler";

    String indexDir;
    String JSONdir;
    IndexWriter indexWriter = null;

    public LuceneBuilder(String indexDir, String JSONdir) {
        this.indexDir = indexDir;
        this.JSONdir = JSONdir;
    }

    public LuceneBuilder() {
        this.indexDir = INDEX_DIR;
        this.JSONdir = JSON_DIR;
    }

    public IndexWriter getIndexWriter(String indexDir) throws IOException {
        //Directory indexDir = new RAMDirectory(); //use to put directory in RAM
        Directory dir = FSDirectory.open(new File(indexDir).toPath());
        IndexWriterConfig luceneConfig = new IndexWriterConfig(new StandardAnalyzer());

        return(new IndexWriter(dir, luceneConfig));
    }


    public ArrayList <JSONObject> parseJSONFiles(String JSONdir) throws IOException {
        BufferedReader br;
        String jsonString;
        JSONParser parser = new JSONParser();
        ArrayList <JSONObject> jsonArrayList = new ArrayList<JSONObject>();

        //Go the Crawler directory
        File dir = new File(JSONdir);

        //Get the file names for the json objects therein
        File[] jsonFiles = dir.listFiles((dir1, filename) -> filename.endsWith(".json"));

        //For each new line delimited json file in the file directory
        for (File jsonFile : jsonFiles) {

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
    }

    public Document buildDocument(JSONObject jsonObject) {
        Document doc = new Document();

        doc.add(new TextField("text",(String) jsonObject.get("Text"), Field.Store.NO));
        doc.add(new StringField("user",(String) jsonObject.get("User"), Field.Store.YES));
        doc.add(new StringField("datetime",(String) jsonObject.get("Datetime"), Field.Store.YES));
        // Add as doc values field, so we can compute range facets
        doc.add(new NumericDocValuesField("timestamp", (long) jsonObject.get("Timestamp")));
        // Add as numeric field so we can drill-down
        doc.add(new LongPoint("timestamp", (long) jsonObject.get("Timestamp")));
        doc.add(new DoublePoint("latitude", (double) jsonObject.get("Latitude")));
        doc.add(new DoublePoint("longitude", (double) jsonObject.get("Longitude")));

        return doc;
    }

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
    }

    public void buildIndex() throws IOException, ParseException {
        IndexWriter indexWriter = getIndexWriter(indexDir);
        ArrayList <JSONObject> jsonArrayList = parseJSONFiles(JSONdir);
        indexTweets(jsonArrayList, indexWriter);
        indexWriter.close();
    }

    public static void main(String args[]) throws IOException, ParseException {
        LuceneBuilder luceneIndex = new LuceneBuilder();
        luceneIndex.buildIndex();
    }
}
