package com.mqtt.client;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class AirconControlSystemPublisher implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AirconControlSystemPublisher.class.getName());
    private IMqttClient client;
    private String publisherId;
    private ThreadLocalRandom rnd = ThreadLocalRandom.current();
    private int requestID;

    public AirconControlSystemPublisher(IMqttClient client, String publisherId, int requestID) {
        this.client = client;
        this.publisherId = publisherId;
        this.requestID = requestID;
    }

    @Override
    public void run()  {


        if (!client.isConnected()) {
            LOGGER.info("[I31] Client not connected.");
            System.exit(0);
        }

//        MqttMessage msg = readEngineTempHumidity();
        MqttMessage msg = contructPublishPayload();        

        InputStream resourcesInputStream = PublisherApp.class.getClassLoader().getResourceAsStream("application.properties");
        Properties properties = new Properties();
        try {
            properties.load(resourcesInputStream);
        } catch (IOException e) {
            LOGGER.warn("Cannot read property ", e);
        }
        // QoS
        //
        // 0 – “at most once” semantics, also known as “fire-and-forget”.
        // Use this option when message loss is acceptable, as it does not require any kind of acknowledgment or persistence
        //
        // 1 – “at least once” semantics.
        // Use this option when message loss is not acceptable and your subscribers can handle duplicates
        //
        // 2 – “exactly once” semantics.
        // Use this option when message loss is not acceptable and your subscribers cannot handle duplicates

        msg.setQos(0);
        msg.setRetained(true);
        try {
            client.publish(properties.getProperty("publishing_topic_reqID"), msg);
            LOGGER.info("On topic {} message: {} sent", properties.getProperty("publishing_topic_reqID") + this.requestID, msg);
//            subscribeToReqID( properties );
        } catch (MqttException e) {
            LOGGER.warn(e.getMessage());
        }
    }

//    {"method":"setState", "params":{"state":"on"}}
    private MqttMessage contructPublishPayload(){
    	JSONObject state = new JSONObject();
    	state.put("state", "on");
    	JSONObject jsonObject = new JSONObject();
        jsonObject.put("method", "setState");
        jsonObject.put("params", state);      
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(jsonObject.toString().getBytes());
        System.out.println( mqttMessage.getPayload().toString());
        return mqttMessage;
    }
}



