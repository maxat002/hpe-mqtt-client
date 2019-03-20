package com.mqtt.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

public class Utils {
	
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Utils.class.getName());
	
    public Properties getProps(){
    	InputStream resourcesInputStream = PublisherApp2.class.getClassLoader().getResourceAsStream("application.properties");
        Properties properties = new Properties();
        try {
            properties.load(resourcesInputStream);
        } catch (IOException e) {
            LOGGER.warn("Cannot read property ", e);
        }
    	return properties;
    }
//  {"method":"setState", "params":{"state":"on"}}
    public MqttMessage contructPublishPayload(){
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
    public String contructPublishPayloadString(){
    	JSONObject state = new JSONObject();
	  	state.put("state", "on");
	  	JSONObject jsonObject = new JSONObject();
	  	jsonObject.put("method", "setState");
	  	jsonObject.put("params", state); 
	    return jsonObject.toString();
    }
    public int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}
}
