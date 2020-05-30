import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.*;

public class LuceneBuilder {
    public static void main(String args[]) throws IOException {
        parseJSONFile();
    }

    public static void parseJSONFile() throws IOException {
        BufferedReader br;
        String jsonObject;

        //Go the Crawler directory
        File dir = new File("../Crawler");

        //Get the file names for the json objects therein
        File[] jsonFiles = dir.listFiles((dir1, filename) -> filename.endsWith(".json"));

        //for each new line delimited json file in the file directory
        for (File jsonFile : jsonFiles) {

            br = new BufferedReader(new FileReader(jsonFile.getCanonicalPath()));
            while ((jsonObject = br.readLine()) != null) {
                System.out.println("Record:\t" + jsonObject);
            }
            br.close();

        }
    }
}
