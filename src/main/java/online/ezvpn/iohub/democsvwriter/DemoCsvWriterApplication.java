package online.ezvpn.iohub.democsvwriter;

import online.ezvpn.iohub.democsvwriter.csv.CsvWriter;
import online.ezvpn.iohub.democsvwriter.mqtt.MqttReader;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoCsvWriterApplication implements CommandLineRunner {

    private MqttReader mqttReader;

    private CsvWriter csvWriter;

    @Autowired
    public void setMqttReader(MqttReader mqttReader) {
        this.mqttReader = mqttReader;
    }

    @Autowired
    public void setCsvWriter(CsvWriter csvWriter) {
        this.csvWriter = csvWriter;
    }

    @Override
    public void run(String... args) {
        try {
            mqttReader.mqttConnect((topic, message) -> csvWriter.manageMeasurement(topic, message));
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoCsvWriterApplication.class, args);
    }
}
