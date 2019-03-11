import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.twitter.hbc.core.Constants.*;

public class clientPRoducer {
    static final String consumerKey = "8CCqoC7dXIckTz1cWyDqntioP";
    static final String consumerSecret = "DCqSY96TAJ5FYgXa6r0najMW3spknFtgyROsFR5jzWnlGbxJOJ";
    static final String token = "4872857735-3rILDXpkV1BRj3HTeBdfnRg4ZLiZmsXilHOvxgk";
    static final String secret = "4yv7knLEDOF3RyNxWFG0LLgsgwZ1VpBbDBQc9FVxxQ6Kz";
    final Logger logger = LoggerFactory.getLogger(clientPRoducer.class);

    public clientPRoducer() {
    }

    public static void main(String[] args) {
        clientPRoducer clientPRoducer = new clientPRoducer();
        clientPRoducer.run();

    }
    public void run()  {
        logger.info("start running");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create a twitter client
        Client cli = createTwitterClient(msgQueue);
        //connextion
        cli.connect();
        //create kafka producer
        KafkaProducer<String,String> kafkaProducer = createKafkaProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("shutdown twitter retrieve");
            logger.info("shutdown client twitterr");
            cli.stop();
            logger.info("stop producer");
            kafkaProducer.close();
            logger.info("exit the application");
        }));

        //loop read tweets
        while (!cli.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                cli.stop();
            }
            if(msg != null){
                System.out.println(msg);
                kafkaProducer.send(new ProducerRecord<String, String>(KafkaConstants.topic_name, null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("erreur",e);
                        }
                    }
                });

            }
            else {
                cli.stop();
                logger.info("stop running");
            }

        }

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
                Hosts hosebirdHosts = new HttpHosts(STREAM_HOST);
                StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
                List<String> terms = Lists.newArrayList("twitter", "api");
                hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
                Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    public KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaConstants.bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);


    }
}


