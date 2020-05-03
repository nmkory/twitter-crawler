import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Tcrawler {
    //Nick's Twitter account consumer keys - requires login
    private static final String CONSUMER_KEY = "LT4IPByNOJS6RctWDBHN0Favi";
    private static final String CONSUMER_SECRET = "MYvp76RGm2D8fCZNanEr3aHAt03BBRZEppjmdcOvwZGj9bkAQ7";

    private static void storeAccessToken(long useId, AccessToken accessToken){
        //TODO Store tokens safely
        //accessToken.getToken();
        //accessToken.getTokenSecret();
    }

    private static Twitter getAccessToken() throws Exception{
        // The factory instance is re-useable and thread safe.
        Twitter twitter = TwitterFactory.getSingleton();
        twitter.setOAuthConsumer(CONSUMER_KEY, CONSUMER_SECRET);
        RequestToken requestToken = twitter.getOAuthRequestToken();
        AccessToken accessToken = null;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (null == accessToken) {
            System.out.println("Open the following URL and grant access to your account:");
            System.out.println(requestToken.getAuthorizationURL());
            System.out.print("Enter the PIN(if aviailable) or just hit enter.[PIN]:");
            String pin = br.readLine();
            try{
                if(pin.length() > 0){
                    accessToken = twitter.getOAuthAccessToken(requestToken, pin);
                }else{
                    accessToken = twitter.getOAuthAccessToken();
                }
            } catch (TwitterException te) {
                if(401 == te.getStatusCode()){
                    System.out.println("Unable to get the access token.");
                }else{
                    te.printStackTrace();
                }
            }
        }
        //persist to the accessToken for future reference.
        //storeAccessToken(twitter.verifyCredentials().getId() , accessToken);

        return twitter;
    }

    private static void crawlTwitter(Twitter twitter) throws Exception{

    }

    public static void main(String args[]) throws Exception{
        Twitter twitter = getAccessToken();
        //TODO program has keys, now crawl
        crawlTwitter(twitter);

        System.exit(0);
    }

}
