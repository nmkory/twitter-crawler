import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class LuceneBuilder {
    public static void main(String args[]) throws IOException, ParseException {
        parseJSONFiles();
    }

    public static void parseJSONFiles() throws IOException, ParseException {
        BufferedReader br;
        String jsonObject;
        JSONParser parser = new JSONParser();

        //Go the Crawler directory
        File dir = new File("../Crawler");

        //Get the file names for the json objects therein
        File[] jsonFiles = dir.listFiles((dir1, filename) -> filename.endsWith(".json"));

        //for each new line delimited json file in the file directory
        for (File jsonFile : jsonFiles) {

            br = new BufferedReader(new FileReader(jsonFile.getCanonicalPath()));
            while ((jsonObject = br.readLine()) != null) {

                JSONObject json = (JSONObject) parser.parse(jsonObject);
                System.out.println("Record:\t" + json);
            }
            br.close();

        }
    }
}
