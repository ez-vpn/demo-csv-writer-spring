package online.ezvpn.iohub.democsvwriter.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.function.BiConsumer;

@Service
public class MqttReader implements MqttCallbackExtended {

    private static final Logger LOG = LoggerFactory.getLogger(MqttReader.class);

    private static final int MQTT_RECONNECT_PERIOD = 5;

    @Value("${MQTT_HOST:127.0.0.1}")
    private String mqttHost;

    @Value("${MQTT_PORT:1883}")
    private int mqttPort;

    @Value("${MQTT_IN_TOPIC:fld/+/r/#}")
    private String mqttInTopic;

    private IMqttClient subscriber;

    private BiConsumer<String, String> messageListener;

    @Override
    public void connectComplete(boolean arg0, String arg1) {
        try {
            subscriber.subscribe(this.mqttInTopic, 0);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void connectionLost(Throwable cause) {
        LOG.error("Connection lost because: " + cause);
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
        // never used
    }

    public void messageArrived(String topic, MqttMessage message) {
        this.messageListener.accept(topic, new String(message.getPayload()));
    }

    public void mqttConnect(BiConsumer<String, String> messageListener) throws MqttException {
        this.messageListener = messageListener;

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(MQTT_RECONNECT_PERIOD);

        subscriber = new MqttClient(String.format("tcp://%s:%d", mqttHost, mqttPort), UUID.randomUUID().toString(), new MqttDefaultFilePersistence(System.getProperty("java.io.tmpdir")));
        subscriber.setCallback(this);

        subscriber.connect(options);
    }
}
