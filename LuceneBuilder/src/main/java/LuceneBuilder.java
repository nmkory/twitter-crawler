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
import java.util.Date;

public class LuceneBuilder {

    public static IndexWriter getIndexWriter(String dir) throws IOException {
        //Directory indexDir = new RAMDirectory(); //use to put directory in RAM
        Directory indexDir = FSDirectory.open(new File(dir).toPath());
        IndexWriterConfig luceneConfig = new IndexWriterConfig(new StandardAnalyzer());

        return(new IndexWriter(indexDir, luceneConfig));
    }


    public static ArrayList <JSONObject> parseJSONFiles() throws IOException, ParseException {
        BufferedReader br;
        String jsonString;
        JSONParser parser = new JSONParser();
        //JSONObject jsonObject;
        ArrayList <JSONObject> jsonArray = new ArrayList<JSONObject>();

        //Go the Crawler directory
        File dir = new File("../Crawler");

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
                    jsonArray.add((JSONObject) parser.parse(jsonString));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }
            //Close the open buffered reader and open the next json file
            br.close();
        }
        return jsonArray;
    }

    public Document buildDocument(JSONObject jsonObject) {
        Document doc = new Document();
        doc.add(new TextField("text",(String) jsonObject.get("Text"), Field.Store.NO));
        doc.add(new StringField("user",(String) jsonObject.get("User"), Field.Store.YES));
        doc.add(new StringField("timestamp",(String) jsonObject.get("Timestamp"), Field.Store.YES));
        doc.add(new NumericDocValuesField("timestamp", jsonObject.get("Timestamp")getTime()));



        return doc;
    }

    public void indexTweets(ArrayList <JSONObject> jsonArray, IndexWriter indexWriter) {

    }

    public static void main(String args[]) throws IOException, ParseException {
        IndexWriter indexWriter = getIndexWriter("../index");
        ArrayList <JSONObject> jsonArray = parseJSONFiles();
        for (JSONObject jsonObject : jsonArray) {
            System.out.println(jsonObject.get("User"));
        }
    }
}
