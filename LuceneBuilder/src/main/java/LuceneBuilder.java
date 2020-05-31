import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.FSDirectory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class LuceneBuilder {

    public IndexWriter getIndexWriter(String dir) throws IOException {
        Directory indexDir = FSDirectory.open(new File(dir).toPath());
        IndexWriterConfig luceneConfig = new IndexWriterConfig(new StandardAnalyzer());

        return(new IndexWriter(indexDir, luceneConfig));
    }


    public static void parseJSONFiles() throws IOException, ParseException {
        BufferedReader br;
        String jsonString;
        JSONParser parser = new JSONParser();
        JSONObject jsonObject;

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
                    jsonObject = (JSONObject) parser.parse(jsonString);
                    System.out.println(jsonObject.get("User"));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }
            //Close the open buffered reader and open the next json file
            br.close();
        }
    }

    public static void main(String args[]) throws IOException, ParseException {
        parseJSONFiles();
    }
}
