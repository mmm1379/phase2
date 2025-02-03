//package org.example;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.opencv.core.Mat;
//import org.opencv.core.MatOfByte;
//import org.opencv.imgcodecs.Imgcodecs;
//import org.opencv.videoio.VideoCapture;
//import org.opencv.core.Core;
//import java.awt.image.DataBufferByte;
//
//import javax.swing.*;
//import java.awt.*;
//import java.awt.image.BufferedImage;
//import java.util.Base64;
//import java.util.Collections;
//import java.util.Properties;
//import java.util.concurrent.atomic.AtomicReference;
//
//public class Producer {
//
//    static {
//        // Load OpenCV native library
//        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
//    }
//
//    public static void main(String[] args) {
//        String bootstrapServers = "localhost:9092";
//        String inputTopic = "sensor-data";
//        String outputTopic = "results";
//
//        // Kafka Producer Configuration
//        Properties producerProps = new Properties();
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
//
//        // Kafka Consumer Configuration
//        Properties consumerProps = new Properties();
//        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "image-processing-group2");
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
//        consumer.subscribe(Collections.singletonList(outputTopic));
//
//        // OpenCV Video Capture
//        VideoCapture camera = new VideoCapture(0);
//        if (!camera.isOpened()) {
//            System.out.println("Error: Could not open camera.");
//            return;
//        }
//
//        JFrame frame = new JFrame("Camera Feed");
//        JLabel label = new JLabel();
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        frame.setSize(800, 600);
//        frame.add(label);
//        frame.setVisible(true);
//
//        AtomicReference<String> lastEmotion = new AtomicReference<>("Unknown");
//
//        try {
//            while (true) {
//                Mat image = new Mat();
//                if (!camera.read(image)) {
//                    System.out.println("Failed to capture image.");
//                    break;
//                }
//
//                // Encode image to Base64
//                String base64Image = encodeToBase64(image);
//
//                // Prepare JSON payload
//                String jsonPayload = String.format(
//                        "{\"filename\": \"image_%d.jpg\", \"data\": \"%s\"}",
//                        System.currentTimeMillis(),
//                        base64Image
//                );
//
//                // Produce to Kafka topic
//                producer.send(new ProducerRecord<>(inputTopic, "camera_1", jsonPayload));
//                producer.flush();
//
//                // Display the frame with the last emotion received
//                BufferedImage bufferedImage = convertMatToBufferedImage(image);
//                Graphics g = bufferedImage.getGraphics();
//                g.setFont(new Font("Arial", Font.BOLD, 16));
//                g.setColor(Color.RED);
//                g.drawString("Emotion: " + lastEmotion, 10, 20);
//
//                label.setIcon(new ImageIcon(bufferedImage));
//                frame.repaint();
//
//                // Poll for emotion predictions
//                ConsumerRecords<String, String> records = consumer.poll(100);
//                records.forEach(record -> {
//                    System.out.println("Received emotion: " + record.value());
//                    lastEmotion.set(record.value());
//                });
//
//                // Break if 'q' is pressed (simulate using delay for demo purposes)
//                Thread.sleep(500); // Simulate delay
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            camera.release();
//            producer.close();
//            consumer.close();
//            frame.dispose();
//            System.out.println("Camera and Kafka resources released.");
//        }
//    }
//
//    // Encode OpenCV Mat image to Base64 string
//    private static String encodeToBase64(Mat frame) {
//        MatOfByte matOfByte = new MatOfByte(); // Correct type for the third argument
//        Imgcodecs.imencode(".jpg", frame, matOfByte); // Encode the image to a byte array
//        byte[] imageBytes = matOfByte.toArray(); // Convert to a byte array
//        return Base64.getEncoder().encodeToString(imageBytes); // Encode to Base64
//    }
//
//    // Convert OpenCV Mat image to BufferedImage for display
//    private static BufferedImage convertMatToBufferedImage(Mat mat) {
//        int type = BufferedImage.TYPE_BYTE_GRAY;
//        if (mat.channels() > 1) {
//            type = BufferedImage.TYPE_3BYTE_BGR;
//        }
//        int bufferSize = mat.channels() * mat.cols() * mat.rows();
//        byte[] buffer = new byte[bufferSize];
//        mat.get(0, 0, buffer);
//        BufferedImage image = new BufferedImage(mat.cols(), mat.rows(), type);
//        final byte[] targetPixels = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
//        System.arraycopy(buffer, 0, targetPixels, 0, buffer.length);
//        return image;
//    }
//}
