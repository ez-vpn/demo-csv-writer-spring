package online.ezvpn.iohub.democsvwriter.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class CsvWriter {

    private static final Logger LOG = LoggerFactory.getLogger(CsvWriter.class);

    @Value("${CSV_PATH:/tmp/log.csv}")
    private String csvPath;

    private static final Pattern topicRegex = Pattern.compile("^([^/]+)/([^/]+)/?.*/([^/]+)$");

    public void manageMeasurement(String topic, String msg) {
        LOG.info("{}:{}", topic, msg);

        // parse topic
        Matcher matcher = topicRegex.matcher(topic);
        if (!matcher.matches()) {
            LOG.warn("bad topic, skipping");
            return;
        }
        String protocol = matcher.group(2);
        String measurement = matcher.group(3);

        FileWriter fileWriter = null;
        try {
            // parse json payload
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(msg);

            // save to file
            fileWriter = new FileWriter(csvPath, true);
            CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT);
            csvPrinter.printRecord(protocol, measurement, jsonNode.get("value").asText(), jsonNode.get("ts").asText());
            csvPrinter.flush();
        } catch (JsonProcessingException e) {
            LOG.error("Cannot parse JON payload", e);
        } catch (IOException e) {
            LOG.error("CSV Operation failed", e);
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
        }
    }
}
