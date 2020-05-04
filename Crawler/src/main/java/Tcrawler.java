import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Tcrawler {

    public static void main(String args[]) throws TwitterException, FileNotFoundException, IOException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        FileWriter fw = new FileWriter("output.txt");
        BufferedWriter bw = new BufferedWriter(fw);

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("LT4IPByNOJS6RctWDBHN0Favi")
                .setOAuthConsumerSecret("MYvp76RGm2D8fCZNanEr3aHAt03BBRZEppjmdcOvwZGj9bkAQ7")
                .setOAuthAccessToken("42011375-weTQ4bvTir50lNUwbJYxwGckVolxQXf6kemrnTVdW")
                .setOAuthAccessTokenSecret("IhTUGURpvkMKGu9KKOOqoBsCuTTHGemb5ss3noK6GkEFj")
                .setJSONStoreEnabled(true);


        StatusListener listener = new StatusListener() {

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
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
                String json = TwitterObjectFactory.getRawJSON(status);
                System.out.println(json);
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }
        };

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        twitterStream.addListener(listener);

        twitterStream.sample("en");

        System.exit(0);
    }

}
