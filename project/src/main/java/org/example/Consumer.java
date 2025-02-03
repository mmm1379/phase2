package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class Consumer {
    public static void main(String[] args) throws Exception {
        // Kafka broker configuration.
        String broker = "localhost:9092";

        // Create the Flink execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka Source for raw sensor data.
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(broker)
                .setTopics("sensor-data")
                .setGroupId("main-server")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // Configure Kafka Sink for forwarding raw data.
        KafkaSink<String> rawDataSink = KafkaSink.<String>builder()
                .setBootstrapServers(broker)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("processing_raw")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // Configure Kafka Source for processed model results.
        KafkaSource<String> resultSource = KafkaSource.<String>builder()
                .setBootstrapServers(broker)
                .setTopics("processing_result")
                .setGroupId("result-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Configure Kafka Sink for the final output.
        KafkaSink<String> resultSink = KafkaSink.<String>builder()
                .setBootstrapServers(broker)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("results")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        // Forward raw sensor data.
        DataStream<String> rawDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        rawDataStream.sinkTo(rawDataSink);

        // Process the processed model results:
        // Each message is expected to be a JSON string containing three numeric fields:
        // { "emotion": <double>, "drowsiness": <double>, "eeg": <double> }
        DataStream<String> resultStream = env.fromSource(resultSource, WatermarkStrategy.noWatermarks(), "Processed Results Source")
                .map(new CombinedModeCalculator(10));
        resultStream.sinkTo(resultSink);

        // Execute the Flink job.
        env.execute("Image and EEG Processing with Moving Mode and Combined Function");
    }

    /**
     * CombinedModeCalculator computes a moving mode for three numeric values (emotion, drowsiness, EEG)
     * from incoming JSON messages. It then applies a custom function on the combined value.
     */
    public static class CombinedModeCalculator extends RichMapFunction<String, String> {
        private final int windowSize;
        private Queue<Double> emotionWindow;
        private Queue<Double> drowsinessWindow;
        private Queue<Double> eegWindow;

        public CombinedModeCalculator(int windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters){
            this.emotionWindow = new LinkedList<>();
            this.drowsinessWindow = new LinkedList<>();
            this.eegWindow = new LinkedList<>();
        }

        @Override
        public String map(String value){
            try {
                // Parse the incoming JSON.
                ObjectMapper mapper = new ObjectMapper();
                ResultData resultData = mapper.readValue(value, ResultData.class);

                double emotionValue = resultData.getEmotion();
                double drowsinessValue = resultData.getDrowsiness();
                double eegValue = resultData.getEeg();

                // Update the windows.
                updateWindow(emotionWindow, emotionValue);
                updateWindow(drowsinessWindow, drowsinessValue);
                updateWindow(eegWindow, eegValue);

                // Compute mode for each window.
                double emotionMode = getMode(emotionWindow);
                double drowsinessMode = getMode(drowsinessWindow);
                double eegMode = getMode(eegWindow);

                // Apply a custom function on the combined value.
                String finalResult = customFunction(emotionMode,drowsinessMode,eegMode);

                // Create an output object that includes the individual modes, combined value, and final result.
                return mapper.writeValueAsString(finalResult);
            } catch (Exception e) {
                return "Error in CombinedModeCalculator: " + e.getMessage();
            }
        }

        /**
         * Updates the given window (queue) with a new value, ensuring its size does not exceed windowSize.
         */
        private void updateWindow(Queue<Double> window, double newValue) {
            window.add(newValue);
            if (window.size() > windowSize) {
                window.poll();
            }
        }

        /**
         * Computes the mode (most frequently occurring value) in the given window.
         * If multiple values have the same frequency, one of them is returned.
         */
        private double getMode(Queue<Double> window) {
            Map<Double, Integer> frequencyMap = new HashMap<>();
            for (Double val : window) {
                frequencyMap.put(val, frequencyMap.getOrDefault(val, 0) + 1);
            }
            double mode = 0.0;
            int maxCount = 0;
            for (Map.Entry<Double, Integer> entry : frequencyMap.entrySet()) {
                if (entry.getValue() > maxCount) {
                    mode = entry.getKey();
                    maxCount = entry.getValue();
                }
            }
            return mode;
        }

        /**
         * A custom function applied to the combined value.
         * For example, if the combined value exceeds a threshold, output "Alert"; otherwise, "Normal".
         */
        private String customFunction(double emotionMode,double drowsinessMode,double eegMode) {
            if (emotionMode == 0 || emotionMode == 1 || emotionMode == 2 || emotionMode == 5) {
                if (drowsinessMode == 1 || drowsinessMode == 2 || eegMode == 2) {
                    return "Alert";
                }
            }
            return "Normal";
        }
    }

    /**
     * POJO for reading processed result JSON.
     * Expected JSON structure:
     * { "emotion": <double>, "drowsiness": <double>, "eeg": <double> }
     */
    public static class ResultData {
        private double emotion;
        private double drowsiness;
        private double eeg;

        public ResultData() {
        }

        public double getEmotion() {
            return emotion;
        }

        public void setEmotion(double emotion) {
            this.emotion = emotion;
        }

        public double getDrowsiness() {
            return drowsiness;
        }

        public void setDrowsiness(double drowsiness) {
            this.drowsiness = drowsiness;
        }

        public double getEeg() {
            return eeg;
        }

        public void setEeg(double eeg) {
            this.eeg = eeg;
        }
    }

}
