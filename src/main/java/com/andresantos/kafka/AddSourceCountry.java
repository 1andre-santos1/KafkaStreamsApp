package com.andresantos.kafka;

import com.maxmind.geoip2.model.CountryResponse;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONException;
import org.json.JSONObject;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;

/**
 * This application will consume the production topic (given that the name of the topic is 'message')
 * (message:  {ts: <message timestamp>, source: <telnet source>, payload: <message content>})
 *  and will add a country attribute given the telnet source IP
 *  (result:  {ts: <message timestamp>, source: <telnet source ip>, country: <IP country>, payload: <message content>}
 */
public class AddSourceCountry
{
    // Country Database path
    public static final String DATABASE_COUNTRY_PATH = "com/andresantos/kafka/geoip2/GeoLite2-Country.mmdb";

    public static void main( String[] args ) throws IOException {
        // A File object pointing to the GeoLite2 database
        File dbFile = new File(DATABASE_COUNTRY_PATH);

        // This creates the DatabaseReader object,
        // which should be reused across lookups to the country database
        final DatabaseReader reader = new DatabaseReader.Builder(dbFile).build();

        Properties props = new Properties();

        //assign a unique identifier to this application when connecting with the Kafka cluster
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"AddSourceCountry");
        //assuming that the Kafka broker is running on localhost:9092
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();

        //consume the source messages from the topic 'message'
        KStream<String,String> source = builder.stream("message");

        //go through all of the source messages and then send each value to a new topic called 'messageWithCountry'
        source.flatMapValues(new ValueMapper<String,Iterable<String>>(){
            @Override
            public Iterable<String> apply(String value){
                //here the value should be in this format: {ts: <message timestamp>, source: <telnet source>, payload: <message content>}

                //we can convert the string into a JSONObject to access easily its attributes
                JSONObject jsonObject;
                try {
                    jsonObject = new JSONObject(value);

                    //get the JSONObject source IP address
                    String telnetSource = jsonObject.get("source").toString();

                    //get the country location of the IP address with the com.maxmind.geoip2 library
                    InetAddress ipaddress = InetAddress.getByName(telnetSource);
                    CountryResponse response = reader.country(ipaddress);
                    String countryName = response.getCountry().getName();

                    //create a new value string with the old and new parameters
                    String stringWithCountry = "{ts: "+jsonObject.get("ts")+ ", source: "+jsonObject.get("source")+", country: "+countryName+", payload: "+jsonObject.get("payload")+"}";

                }catch (JSONException | UnknownHostException err){
                    err.printStackTrace();
                } catch (GeoIp2Exception e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return Arrays.asList(value);
            }
        }).to("messageWithCountry");
    }
}
