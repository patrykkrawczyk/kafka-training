package pro.patrykkrawczyk.kafka.twitter_app;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) throws InterruptedException {
        TwitterProducer twitterProducer = new TwitterProducer();

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client = twitterProducer.createTwitterClient(msgQueue);

        client.connect();

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = msgQueue.poll(5, TimeUnit.SECONDS);

            if (msg != null) {
                logger.info(msg);
            }
        }
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("java");
        hosebirdEndpoint.trackTerms(terms);

        String consumerKey = System.getenv("TWITTER_APP_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_APP_CONSUMER_SECRET");
        String token = System.getenv("TWITTER_APP_TOKEN");
        String secret = System.getenv("TWITTER_APP_TOKEN_SECRET");
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
