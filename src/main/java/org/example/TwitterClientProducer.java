package org.example;

import com.twitter.clientlib.JSON;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;

import java.io.BufferedInputStream;
import java.lang.*;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import com.google.common.reflect.TypeToken;
import com.twitter.clientlib.model.StreamingTweetResponse;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;


public class TwitterClientProducer {

    public TwitterClientProducer(){}
    public static void main(String[] args) {
        new TwitterClientProducer().run();
    }
     public void run(){
         // Steps to get data from Twitter and send it to kafka
         // 1. Create Twitter Client
         // 2. Create Kafka Producer
         // 3. Loop to send tweets to Kafka

         // Twitter Client
         InputStream result = CreateTwitterClient();
         JSON json = new JSON();
         Type localVarReturnType = new TypeToken<StreamingTweetResponse>(){}.getType();

         // Kafka Producer
         KafkaProducer<String, String> producer = createKakfaProducer();
         try {
             Logger logger = LoggerFactory.getLogger(TwitterClientProducer.class);

             BufferedReader reader = new BufferedReader(new InputStreamReader(result));
             //BufferedInputStream newBuffer = new BufferedInputStream(result,256);
             String line = reader.readLine();
             while (line != null) {

                 if(line.isEmpty()) {
                     System.err.println("==> " + line.isEmpty());
                     line = reader.readLine();
                     continue;
                 }
                 Object jsonObject = json.getGson().fromJson(line, localVarReturnType);
                 System.out.println(jsonObject != null ? jsonObject.toString() : "Null object");
                 producer.send(new ProducerRecord<>("twitter_stream", null,jsonObject.toString()), new Callback() {

                     @Override
                     public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                         if (e==null){
                             logger.info("Exception", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                         }
                         else {
                             logger.error("Error while attempting");
                         }
                     }
                 });

                 line = reader.readLine();
             }
         } catch (Exception e) {
             System.err.println("Reason: " + e);
             e.printStackTrace();
         }

     }
      public InputStream CreateTwitterClient(){
        String bearerToken = "AAAAAAAAAAAAAAAAAAAAADiliwEAAAAAnb9kl1i68s14y6ntd39UV2NoNqE%3DxE8tGHaCr4z9qDlEiv4Nga3ZghZ4wEbF4zC3GhBG0TR5gx2xJj";
        TwitterCredentialsBearer credentials = new TwitterCredentialsBearer(bearerToken);
        TwitterApi apiInstance = new TwitterApi(credentials);

          OffsetDateTime startTime = OffsetDateTime.parse("2022-11-11T18:40:40.000Z"); // OffsetDateTime | YYYY-MM-DDTHH:mm:ssZ. The earliest UTC timestamp from which the Tweets will be provided.
          OffsetDateTime endTime = OffsetDateTime.parse("2022-11-11T18:40:40.000Z"); // OffsetDateTime | YYYY-MM-DDTHH:mm:ssZ. The latest UTC timestamp to which the Tweets will be provided.
          Set<String> tweetFields = new HashSet<>(Arrays.asList("attachments", "author_id", "created_at", "geo", "id","possibly_sensitive", "source", "text")); // Set<String> | A comma separated list of Tweet fields to display.
          Set<String> expansions = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of fields to expand.
          Set<String> mediaFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of Media fields to display.
          Set<String> pollFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of Poll fields to display.
          Set<String> userFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of User fields to display.
          Set<String> placeFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of Place fields to display.


      try{
         InputStream result = apiInstance.tweets().sampleStream()
                  .backfillMinutes(0)
                  .tweetFields(tweetFields).expansions(expansions).placeFields(placeFields)
                  .execute();
         return result;
      }catch (Exception e) {
          System.err.println("Reason: " + e);
          e.printStackTrace();
      }
          return null;
      }

      public KafkaProducer<String, String> createKakfaProducer (){
          Properties properties = new Properties();
          properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
          properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

          // Creating the Producer
          KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
          return producer;

      }
}

