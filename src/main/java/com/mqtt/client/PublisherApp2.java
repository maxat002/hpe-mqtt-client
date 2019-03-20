package com.mqtt.client;

import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PublisherApp2 {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PublisherApp2.class.getName());
    private static Utils utils ;
    


    public static void main(String[] args) {
    	
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
        int requestID = getRandomNumberInRange(1,100000);
//        int requestID = 1000;
        while(set.contains(requestID)){
        	requestID = getRandomNumberInRange(1,100000);
        }
        set.add(requestID);
        String msg = utils.contructPublishPayloadString();
        String topicToPublish = properties.getProperty("publishing_topic_reqID") + requestID;
        int qos = Integer.parseInt(properties.getProperty("qos") );
//        if (!aClient.isConnected()) {
//        	aClient.connect();
//        }
        aClient.publish( topicToPublish, msg ,  qos);      
        
        String topicToSubscribe = properties.getProperty("subscription_topic_reqID") + requestID;
        aClient.subscribe(topicToSubscribe, qos);
        

    }
    private static class RunnableSubscription implements Runnable, MqttCallback { 
    	 
    	private Properties properties;
    	private int requestID;
    	private String subscriberId;
    	
    	public RunnableSubscription( Properties properties, int requestID, String subscriberId ) {
    	       this.properties = properties;
    	       this.requestID = requestID;
    	       this.subscriberId = subscriberId;
    	}
        public void run() 
        { 
            System.out.println(Thread.currentThread().getName() 
                             + ", executing run() method!"); 
            subscribeToReqID( properties,  requestID,  subscriberId);
        } 
        private void subscribeToReqID(Properties properties, int requestID, String subscriberId) {
        	
        	String topicProperty = properties.getProperty("subscription_topic_reqID") + requestID ;
            
        	MqttAsyncClient subscriber = null;
            try {
				subscriber = connectToBrokerAsync( topicProperty, properties, subscriberId );
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

            System.out.println("----6");
            if(subscriber!=null){
            	System.out.println("----7");
//            	subscribeToTopicAsync(topicProperty, subscriber );
            	
            	try {
            		subscriber.subscribe(topicProperty, 2);
            		System.out.println("---------RECEIVED MSG ---------" + subscriber.getBufferedMessage(requestID) );
                    System.out.println("----9");
                } catch (MqttException e) {
                    LOGGER.warn("Cannot subscribe on {}. Code={}.{}", topicProperty, e.getReasonCode(), e.getMessage());
                }catch (  Exception e) {
                    LOGGER.warn("Cannot subscribe on {}. Code={}", topicProperty,  e.getMessage());
                }
               	
            }
            
        }
        private MqttAsyncClient connectToBrokerAsync( String topic, Properties properties, String subscriberId ) throws InterruptedException{
        	MqttAsyncClient subscriber = null;
        	System.out.println("----1");
            try {
                subscriber = new MqttAsyncClient(properties.getProperty("aircon_mqttbroker_url"),subscriberId);
                subscriber.setCallback(this);
//                subscriber.setCallback(new MqttSubscribeSample());
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
            	subscriber.connect(options);
                System.out.println("Connected");
                Thread.sleep(1000);
//                Objects.requireNonNull(subscriber).connect(options);
                System.out.println("----4");
            } catch (MqttException e) {
                LOGGER.warn("Cannot connect to MQTT Broker");
                return null;
            }
            System.out.println("----5");
            return subscriber;
        }
//        public void subscribe(String[] topicFilters, int[] qos) throws MqttException {
//        	  IMqttToken tok = aClient.subscribe(topicFilters, qos, null, null);
//        	  tok.waitForCompletion(getTimeToWait());
//        	  int[] grantedQos = tok.getGrantedQos();
//        	  for (int i = 0; i < grantedQos.length; ++i) {
//        	    qos[i] = grantedQos[i];
//        	  }
//        	  if (grantedQos.length == 1 && qos[0] == 0x80) {
//        	    throw new MqttException(MqttException.REASON_CODE_SUBSCRIBE_FAILED);
//        	  }
//        	}
/*        
        private void subscribeToTopicAsync( String topicProperty , MqttAsyncClient subscriber ) {
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
//                Thread.sleep(1000);
                System.out.println("----9");
            } catch (MqttException e) {
                LOGGER.warn("Cannot subscribe on {}. Code={}.{}", topicProperty, e.getReasonCode(), e.getMessage());
            }  
        }
*/
		@Override
		public void connectionLost(Throwable cause) {
			// TODO Auto-generated method stub
			System.err.println("connection lost");
			
		}
		@Override
		public void messageArrived(String topic, MqttMessage message) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("topic: " + topic);
			System.out.println("message: " + new String(message.getPayload()));
		}
		@Override
		public void deliveryComplete(IMqttDeliveryToken token) {
			// TODO Auto-generated method stub
			System.err.println("delivery complete");
		}

    }
	private static int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}
}
