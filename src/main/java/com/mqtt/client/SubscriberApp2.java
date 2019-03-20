package com.mqtt.client;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SubscriberApp2 {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SubscriberApp2.class.getName());
    private static Utils utils ;

    public static MqttClient connectToBroker( String topic, Properties properties, String subscriberId ){
    	MqttClient subscriber = null;
    	System.out.println("----1");
        try {
            subscriber = new MqttClient(properties.getProperty("aircon_mqttbroker_url"),subscriberId);
        } catch (MqttException e) {
            LOGGER.warn(e.getMessage() + " Code: " + e.getReasonCode());
            return null;
        }

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        options.setUserName(properties.getProperty("username") );
        options.setPassword( properties.getProperty("password").toCharArray() );

        System.out.println("----2");
        try {
        	System.out.println("----3");
            Objects.requireNonNull(subscriber).connect(options);
            System.out.println("----4");
        } catch (MqttException e) {
            LOGGER.warn("Cannot connect to MQTT Broker");
            return null;
        }
        System.out.println("----5");
        return subscriber;
    }
    public static void subscribeToTopic( String topicProperty , MqttClient subscriber ) {
        try {
            subscriber.subscribe(topicProperty, (topic, msg) -> {
            	System.out.println("----8");
                byte[] payload = msg.getPayload();
                LOGGER.debug("[I82] Message received: topic={}, payload={}", topic, new String(payload));
                String[] recievedValuesAsSrings = new String(payload).split("/");
                System.out.println("---------RECEIVED START ---------");
                System.out.println(Arrays.toString(recievedValuesAsSrings));
                System.out.println("--------RECEIVED END----------");
            });
//            Thread.sleep(1000);
            System.out.println("----9");
        } catch (MqttException e) {
            LOGGER.warn("Cannot subscribe on {}. Code={}.{}", topicProperty, e.getReasonCode(), e.getMessage());
        }  
    }

    public static void main(String[] args) {
/*    	
        InputStream resourcesInputStream = PublisherApp.class.getClassLoader().getResourceAsStream("application.properties");
        Properties properties = new Properties();
        try {
            properties.load(resourcesInputStream);
        } catch (IOException e) {
            LOGGER.warn("Cannot read property ", e);
        }
        String topicProperty = properties.getProperty("subscription_topic");
//        String topicProperty = properties.getProperty("subscription_topic_reqID") + "1000";
        
        String subscriberId = UUID.randomUUID().toString();
        MqttClient subscriber = null;
        subscriber = connectToBroker( topicProperty, properties, subscriberId );
        if(subscriber!=null){
        	subscribeToTopic(topicProperty, subscriber );
        }else{
        	LOGGER.warn("Cannot connect to MQTT Broker");
        }
*/        
// --------------------------------------------------------------------------------        
    	utils = new Utils();
    	Properties properties = utils.getProps();        	
        String clientId = UUID.randomUUID().toString();
        String broker = properties.getProperty("aircon_mqttbroker_url");
        
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setAutomaticReconnect(true);
        mqttConnectOptions.setCleanSession(true);
        mqttConnectOptions.setConnectionTimeout(10);
        mqttConnectOptions.setUserName(properties.getProperty("username") );
        mqttConnectOptions.setPassword( properties.getProperty("password").toCharArray() );
        
        MqttClientAsync aClient = new MqttClientAsync( broker,  clientId, mqttConnectOptions);
        aClient.connect();

        Set<Integer> set = new HashSet<Integer>();        
        int requestID = utils.getRandomNumberInRange(1,100000);
//        int requestID = 1000;
        while(set.contains(requestID)){
        	requestID = utils.getRandomNumberInRange(1,100000);
        }
        set.add(requestID);
//        String msg = utils.contructPublishPayloadString();
//        String topicToPublish = properties.getProperty("subscription_topic") ;
        int qos = Integer.parseInt(properties.getProperty("qos") );
//        if (!aClient.isConnected()) {
//        	aClient.connect();
//        }
//        aClient.publish( topicToPublish, msg ,  qos);      
        
        String topicToSubscribe = properties.getProperty("subscription_topic") ;
        aClient.subscribe(topicToSubscribe, qos);

    }
}
