import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import org.json.simple.JSONObject;
import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Tcrawler {
    //Variable for creating json files for Part B
    private static final int TEN_MB = 10000 * 1024;
    //Variable for creating json files for grading
    private static final int TWENTY_KB = 20 * 1024;
    //Data structure for class implementing StatusListener streaming API
    private static LinkedBlockingQueue<Status> statuses = new LinkedBlockingQueue<Status>();
    //Data structure to dump API statuses into
    private static ArrayList<Status> tweets = new ArrayList<Status>();

    /**
     * crawlURL returns data structure from Twitter4j API object (written to adhere to Twitter4j standards)
     * @param tweet is a status from the Twitter4j API
     * @return URLEntity[] which is an array of URLs that we can use so as to not clog the Twitter4j API
     */
    public URLEntity[] crawlURL(Status tweet) {
        return tweet.getURLEntities();
    }  //crawlURL()

    /**
     * main runs the crawler, generating json files based on English language tweets with geolocation enabled inside
     * bounding box GPS coordinates around the United States of America
     * @param args command line arguments that determine the size and number of the json files generated
     *             (defaults to 10 20KB files if not provided for grading; 200 10MB - 2GB - generated for Part B)
     * @throws IOException if bad things happen
     */
    public static void main(String args[]) throws IOException {
        int fileNum = 001;
        File file = new File(String.format("%03d", fileNum) +".json");
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("LT4IPByNOJS6RctWDBHN0Favi")
                .setOAuthConsumerSecret("MYvp76RGm2D8fCZNanEr3aHAt03BBRZEppjmdcOvwZGj9bkAQ7")
                .setOAuthAccessToken("42011375-weTQ4bvTir50lNUwbJYxwGckVolxQXf6kemrnTVdW")
                .setOAuthAccessTokenSecret("IhTUGURpvkMKGu9KKOOqoBsCuTTHGemb5ss3noK6GkEFj");

        //Our implemention of StatusListener. Twitter4J will use this to create a thread, consuming the stream.
        StatusListener listener = new StatusListener() {

            /**
             * Our onException method just does what Twitter4j recommends (prints the stack trace)
             * @param ex is the exception thrown
             */
            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg) {
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }

            /**
             * Our onStatus method checks if a status on the stream is geolocation enabled. If so, it adds it to the
             * blocking queue. If not, it does nothing with it.
             * @param status is a new status received from the stream
             */
            @Override
            public void onStatus(Status status) {
                    if (status.getGeoLocation() != null) {
                        statuses.add(status);
                    }
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }
        };

        // US bounding from http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries.zip
        double[][] us = { {-171.791110603, 18.91619, },
                { -66.96466, 71.3577635769,  } };

        FilterQuery fq = new FilterQuery();
        fq.language("en");
        fq.locations(us);

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        twitterStream.addListener(listener);

        twitterStream.filter(fq);
        int length;
        String url;

        while (file.length() < TEN_MB) {
            statuses.drainTo(tweets);
            
            for (Status tweet : tweets) {
                JSONObject obj = new JSONObject();
                obj.put("Text", tweet.getText());
                obj.put("Timestamp", tweet.getCreatedAt());
                obj.put("Geolocation", tweet.getGeoLocation());
                obj.put("User", tweet.getUser().getScreenName());

                length = tweet.getURLEntities().length;
                if (length > 0) {
                    try {
                        url = tweet.getURLEntities()[0].getExpandedURL();
                        obj.put("URL", url);
                        obj.put("URLTitle", Jsoup.connect(url).get().title());
                    } catch (NullPointerException ex) {
                        ex.printStackTrace();
                    } catch (HttpStatusException ex) {
                        obj.put("URLTitle", "404 Not Found");
                        ex.printStackTrace();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }

                bw.write(obj.toJSONString() + "\n");
            }
            tweets.clear();
        }
        bw.close();

        System.exit(0);
    }

}
