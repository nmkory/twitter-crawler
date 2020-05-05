import org.jsoup.Jsoup;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import org.json.simple.JSONObject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Tcrawler {
    private static final Object lock = new Object();
    private static List<Status> tweets = new ArrayList<Status>();

    public URLEntity[] crawlURL(Status tweet) {
        return tweet.getURLEntities();
    }

    public static void main(String args[]) throws TwitterException, FileNotFoundException, IOException {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        FileWriter fw = new FileWriter("output.json");
        BufferedWriter bw = new BufferedWriter(fw);

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("LT4IPByNOJS6RctWDBHN0Favi")
                .setOAuthConsumerSecret("MYvp76RGm2D8fCZNanEr3aHAt03BBRZEppjmdcOvwZGj9bkAQ7")
                .setOAuthAccessToken("42011375-weTQ4bvTir50lNUwbJYxwGckVolxQXf6kemrnTVdW")
                .setOAuthAccessTokenSecret("IhTUGURpvkMKGu9KKOOqoBsCuTTHGemb5ss3noK6GkEFj")
                .setJSONStoreEnabled(true);


        StatusListener listener = new StatusListener() {

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

            @Override
            public void onStatus(Status status) {
                synchronized (lock) {
                    if (status.getGeoLocation() != null) {
                        tweets.add(status);
                    }
                }
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }
        };


        double[][] us = { {-171.791110603, 18.91619, },
                { -66.96466, 71.3577635769,  } };

        FilterQuery fq = new FilterQuery();
        fq.language("en");
        fq.locations(us);

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        twitterStream.addListener(listener);

        twitterStream.filter(fq);
        int length;
        int count = 0;
        String url;
        while (count < 100) {
            synchronized (lock) {
                for (Status tweet : tweets) {
                    JSONObject obj = new JSONObject();
                    obj.put("Text", tweet.getText());
                    obj.put("Timestamp", tweet.getCreatedAt());
                    obj.put("Geolocation", tweet.getGeoLocation());
                    obj.put("User", tweet.getUser().getScreenName());
                    length = tweet.getURLEntities().length;
                    for (int i = 0; i < length; i++) {
                        url = tweet.getURLEntities()[i].getExpandedURL();
                        obj.put("url" + i, url);
                        obj.put("urltitle" + i, Jsoup.connect(url).get().title());
                    }
                    bw.write(obj.toJSONString() + "\n");
                    count++;
                }
                tweets.clear();
            }
        }
        bw.close();





        System.exit(0);
    }

}
