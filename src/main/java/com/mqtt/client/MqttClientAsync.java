package com.mqtt.client;


import java.util.Arrays;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttClientAsync implements MqttCallback {
  
    
    // private variables
    private String broker ;
    private String clientId ;

    // private instance variable

    private  MemoryPersistence persistence;
    private  MqttConnectOptions connOpts;
    private  MqttAsyncClient sampleClient;


    // Interface MqttCallback Implementation
    /**
     * 
     * connectionLost
     * This callback is invoked upon losing the MQTT connection.
     * 
     */
    @Override
    public void connectionLost(Throwable arg0) {
        System.err.println("connection lost");

    }

    /**
     * 
     * deliveryComplete
     * This callback is invoked when a message published by this client
     * is successfully received by the broker.
     * 
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        System.out.println("Pub complete");
        //System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
    }

    /**
     * 
     * messageArrived
     * This callback is invoked when a message is received on a subscribed topic.
     * 
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("topic: " + topic);
        System.out.println("message: " + new String(message.getPayload()));
    }


    // constructor
     public  MqttClientAsync(String broker, String clientId, MqttConnectOptions connOpts){
        // Do initialization here 
        this.broker = broker;
        this.clientId = clientId;
        this.connOpts = connOpts;
        try {
            persistence = new MemoryPersistence();
//            connOpts = new MqttConnectOptions();
            sampleClient = new MqttAsyncClient(broker, clientId, persistence);

        } catch (Exception e){
            System.out.println(e);

        }
     }
    
     
    // connect to broker
    public void connect(){
        try {
            connOpts.setCleanSession(true);
            sampleClient.setCallback(this);
            System.out.println("Connecting to broker: " + broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected connect()");
            Thread.sleep(500); // wait until connection is complete

        } catch (Exception e){
            System.out.println("conn error" +e);
        }
    }

    // publish a message to a topic with a qos
    public void publish(String topic, MqttMessage message, int qos){
        try {
        	if (!sampleClient.isConnected()) {
        		sampleClient.connect();
            }
             IMqttDeliveryToken token = null;
             MqttMessage Mqttmsg = message;
             Mqttmsg.setQos(qos);
             Mqttmsg.setRetained(false);
             token = sampleClient.publish(topic, Mqttmsg);
             // Wait until the message has been delivered to the broker
             token.waitForCompletion();
             Thread.sleep(100);
             System.out.println("Message published");

        } catch (Exception e) {
            System.out.println("pub error :"+ e);
        }
    }
    
    // publish a message to a topic with a qos
    public void publish(String topic, String message, int qos){
        try {
             
             IMqttDeliveryToken token = null;
             MqttMessage Mqttmsg = new MqttMessage(message.getBytes());
             Mqttmsg.setQos(qos);
             Mqttmsg.setRetained(false);
             token = sampleClient.publish(topic, Mqttmsg);
             // Wait until the message has been delivered to the broker
             token.waitForCompletion();
             Thread.sleep(100);
             System.out.println("Message published publish2()");

        } catch (Exception e) {
            System.out.println("pub error :"+ e);
        }
    }

    // subscribe multiple topics with Qos
    public void subscribe(String[] topics, int[] Qos){
        try {
            sampleClient.subscribe(topics, Qos);
            System.out.println("Subscribed");
        } catch (Exception e){
            System.out.println("sub error :"+e);
        }
        
    }

    // subscribe a topic with qos
    public void subscribe(String topic, int qos){
        try {
            sampleClient.subscribe(topic, qos);
            System.out.println("Subscribed");
        } catch (Exception e){
            System.out.println("sub error: " +e);
        }        
    }
    // subscribe a topic with qos
    public void subscribe2(String topic, int qos){
        try {
            sampleClient.subscribe(topic, qos);
            System.out.println("Subscribed");
            
//            sampleClient.subscribe(topic, (topicItem, msg) -> {
//            	System.out.println("----8");
//                byte[] payload = msg.getPayload();
//                LOGGER.debug("[I82] Message received: topic={}, payload={}", topicItem, new String(payload));
//                String[] recievedValuesAsSrings = new String(payload).split("/");
//                System.out.println("---------RECEIVED START ---------");
//                System.out.println(Arrays.toString(recievedValuesAsSrings));
//                System.out.println("--------RECEIVED END----------");
//            });
            
        } catch (Exception e){
            System.out.println("sub error: " +e);
        }        
    }

    // disconnect from a broker
    public void disconnect(){
        try {
            sampleClient.disconnect();
            System.out.println("Disconnected");

        } catch (Exception e){
            System.out.println("disconnect error" + e);
        }
    }
}