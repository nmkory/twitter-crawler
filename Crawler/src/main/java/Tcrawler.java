import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Tcrawler {

    public static void main(String args[]) throws TwitterException, FileNotFoundException, IOException {
        final Object lock = new Object();
        final List<Status> tweets = new ArrayList<Status>();
        ConfigurationBuilder cb = new ConfigurationBuilder();
        FileWriter fw = new FileWriter("output.txt");
        BufferedWriter bw = new BufferedWriter(fw);

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("LT4IPByNOJS6RctWDBHN0Favi")
                .setOAuthConsumerSecret("MYvp76RGm2D8fCZNanEr3aHAt03BBRZEppjmdcOvwZGj9bkAQ7")
                .setOAuthAccessToken("42011375-PmplF1B0I613j9BWJNG8krXKMDMf9fHCfHH5n5pp6")
                .setOAuthAccessTokenSecret("NPD0nNhQNdPPFoHrT1xflSDU971fCDi65nPokpY8IPOrg");


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
                tweets.add(status);
                System.out.println(tweets.size() + ":" + status.getText());
                if (tweets.size() > 100) {
                    synchronized (lock) {
                        lock.notify();
                    }
                    System.out.println("unlocked");
                }
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }
        };

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        twitterStream.addListener(listener);

        twitterStream.sample("en");

        try {
            synchronized (lock) {
                lock.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Status tweet : tweets) {
            System.out.println(tweet);
        }


        System.exit(0);
    }

}
