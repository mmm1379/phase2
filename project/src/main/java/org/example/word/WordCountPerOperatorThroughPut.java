//package org.example.word;
//
//import com.sun.management.OperatingSystemMXBean;
//import com.codahale.metrics.SlidingWindowReservoir;
//import com.codahale.metrics.Snapshot;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.common.functions.RichReduceFunction;
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.file.sink.FileSink;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
//import org.apache.flink.metrics.Counter;
//import org.apache.flink.metrics.Gauge;
//import org.apache.flink.metrics.Histogram;
//import org.apache.flink.metrics.Meter;
//import org.apache.flink.metrics.MeterView;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
//import org.apache.flink.util.Collector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import oshi.SystemInfo;
//import oshi.hardware.*;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.lang.management.*;
//import java.time.Duration;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//public class WordCountPerOperatorThroughPut {
//
//    private static final Logger LOG = LoggerFactory.getLogger(WordCountPerOperatorThroughPut.class);  // SLF4J Logger
//
//    public static void main(String[] args) throws Exception {
//
//        // Set up the streaming execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.enableCheckpointing(10000); // Enable checkpointing every 5 seconds
////        env.getConfig().setLatencyTrackingInterval(1000000);
//
//        // Define Kafka source to consume messages from Kafka topic "ali"
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("localhost:9092")    // Kafka broker address
//                .setTopics("WordCount")                         // Kafka topic to consume from
//                .setGroupId("wordcount-group")            // Consumer group ID
//                .setStartingOffsets(OffsetsInitializer.earliest()) // Start from earliest available offset
//                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize Kafka messages as strings
//                .build();
//
//        // Add Kafka source to the Flink environment
//        DataStream<String> text = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
//
//        // Process the incoming text data by splitting each line into words, grouping by words, and counting their occurrences
//        DataStream<Tuple2<String, Integer>> wordCounts = text
//                .flatMap(new Tokenizer())       // Split each incoming line into words
//                .keyBy(value -> value.f0)       // Group the words (key by the word itself)
//                .reduce(new SumReducer());      // Use custom reducer
//
//        // Map the word counts to a readable string format for output (e.g., "word: count")
//        DataStream<String> resultStream = wordCounts
//                .map(new FormatMapFunction());  // Use custom map function
//
//        // Print the result stream to the console for debugging (optional, for checking if data is flowing)
//        resultStream.print();
//
//        // Define a FileSink to write the output data to a file system (local or HDFS)
//        FileSink<String> sink = FileSink
//                .forRowFormat(new Path("flink-output"), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(DefaultRollingPolicy.builder()
//                        .withRolloverInterval(Duration.ofSeconds(10))   // Create a new file every 10 seconds
//                        .withInactivityInterval(Duration.ofSeconds(3))  // Close files after 3 seconds of inactivity
//                        .withMaxPartSize(1024 * 1024 * 1024)            // Limit file size to 1 GB
//                        .build())
//                .withOutputFileConfig(OutputFileConfig.builder()
//                        .withPartPrefix("xzc")  // Prefix for output files
//                        .withPartSuffix(".txt") // Suffix for output files
//                        .build())
//                .build();
//
//        // Write the result stream to the FileSink
//        resultStream.sinkTo(sink);
//
//        // Execute the Flink job
//        env.execute("Kafka Streaming WordCount");
//    }
//
//public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>>
//        implements CheckpointedFunction {
//
//    private static final Logger LOG = LoggerFactory.getLogger(Tokenizer.class);
//
//    private transient Counter wordCounter;          // Counter for counting total words
//    private transient Meter throughputMeter;        // Meter for throughput per second
//    private transient Histogram processingLatencyHistogram; // Histogram for processing latency
//    private transient Histogram latencyHistogram;   // Histogram for latency percentiles
//    private transient BufferedWriter writer;        // Writer to log metrics to a file
//    private transient SystemInfo systemInfo;
//    private transient CentralProcessor processor;
//    private transient long[] prevTicks;             // For CPU metrics calculation
//    private transient long[][] prevProcTicks;       // For per-CPU metrics calculation
//    private transient Gauge<Double> cpuUsageGauge;  // Gauge for CPU usage
//    private transient Gauge<Long> memoryUsageGauge; // Gauge for Memory usage
//    private transient Counter errorCounter;         // Counter for errors
//    private transient Counter successCounter;       // Counter for successful processing
//    private transient List<GarbageCollectorMXBean> gcBeans;
//    private transient ThreadMXBean threadBean;
//    private transient OperatingSystemMXBean osBean;
//    private transient List<NetworkIF> networkIFs;
//    private transient Map<String, Long> prevNetBytesRecv;
//    private transient Map<String, Long> prevNetBytesSent;
//    private int subtaskIndex;
//    private transient Counter checkpointCounter;    // Counter for completed checkpoints
//    private transient ValueState<Integer> state;    // Example state
//    private transient Gauge<Long> stateSizeGauge;   // Gauge for state size
//
//    private final transient int valueToExpose = 0;
//    private transient Map<String, Integer> wordCountMap;
//
//    // New Metrics
//    private transient Counter numRecordsIn;
//    private transient Meter numRecordsInPerSecond;
//    private transient Counter numRecordsOut;
//    private transient Meter numRecordsOutPerSecond;
//    private transient Counter numLateRecordsDropped;
//    private transient Gauge<Long> currentInputWatermark;
//    private transient Gauge<Long> currentOutputWatermark;
//
//    private transient long lastEmittedWatermark = Long.MIN_VALUE;
//    private transient long lastInputWatermark = Long.MIN_VALUE;
//
//    // For controlling metrics collection frequency
//    private transient long lastMetricsCollectionTime;
//    private final long metricsCollectionInterval = 1000; // Collect metrics every 1 second
//
//    // Power Metrics
//    private transient List<PowerSource> powerSources;
//    private transient double lastPowerUsage;        // in watts
//    private transient long lastPowerTimestamp;      // in milliseconds
//    private transient double totalEnergyConsumed;   // in watt-hours (Wh)
//
//    @Override
//    public void open(Configuration config) {
//        try {
//            processingLatencyHistogram = getRuntimeContext().getMetricGroup()
//                    .histogram("processingLatency", new DropwizardHistogramWrapper(
//                            new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100))));
//
//            // Initialize latency histogram for percentiles
//            latencyHistogram = getRuntimeContext().getMetricGroup()
//                    .histogram("latencyHistogram", new DropwizardHistogramWrapper(
//                            new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100))));
//
//            // Register a simple gauge
//            getRuntimeContext()
//                    .getMetricGroup()
//                    .gauge("MyGauge", () -> valueToExpose);
//
//            wordCountMap = new HashMap<>();
//            // Initialize the processing latency histogram
//
//
//            // Initialize throughputMeter
//            throughputMeter = getRuntimeContext().getMetricGroup()
//                    .meter("throughput", new MeterView(1)); // 1-second time span
//
//            // Register custom metrics
//            wordCounter = getRuntimeContext().getMetricGroup().counter("wordCount");
//
//            // Initialize error and success counters
//            errorCounter = getRuntimeContext().getMetricGroup().counter("errorCount");
//            successCounter = getRuntimeContext().getMetricGroup().counter("successCount");
//
//            // Initialize OSHI system info and processor
//            systemInfo = new SystemInfo();
//            processor = systemInfo.getHardware().getProcessor();
//            prevTicks = processor.getSystemCpuLoadTicks(); // Get initial CPU ticks
//            prevProcTicks = processor.getProcessorCpuLoadTicks();
//
//            // Initialize network interfaces
//            networkIFs = systemInfo.getHardware().getNetworkIFs();
//            prevNetBytesRecv = new HashMap<>();
//            prevNetBytesSent = new HashMap<>();
//            for (NetworkIF net : networkIFs) {
//                net.updateAttributes();
//                prevNetBytesRecv.put(net.getName(), net.getBytesRecv());
//                prevNetBytesSent.put(net.getName(), net.getBytesSent());
//            }
//
//            // Initialize power sources
//            powerSources = systemInfo.getHardware().getPowerSources();
//            lastPowerUsage = 0.0;
//            lastPowerTimestamp = System.currentTimeMillis();
//            totalEnergyConsumed = 0.0;
//
//            // Register CPU Usage Gauge
//            cpuUsageGauge = getRuntimeContext().getMetricGroup().gauge("cpuUsage", () -> {
//                double load = processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100;
//                return load;
//            });
//
//            // Register Memory Usage Gauge
//            memoryUsageGauge = getRuntimeContext().getMetricGroup().gauge("memoryUsage", () -> {
//                GlobalMemory memory = systemInfo.getHardware().getMemory();
//                return memory.getTotal() - memory.getAvailable();
//            });
//
//            // Initialize GC beans
//            gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
//
//            // Initialize ThreadMXBean
//            threadBean = ManagementFactory.getThreadMXBean();
//
//            // Initialize OperatingSystemMXBean
//            osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
//
//            // Get subtask index
//            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
//
//            // Initialize I/O Metrics
//            numRecordsIn = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
//            numRecordsOut = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
//
//            // Create MeterViews for per-second rates
//            numRecordsInPerSecond = getRuntimeContext().getMetricGroup().meter("numRecordsInPerSecond", new MeterView(numRecordsIn));
//            numRecordsOutPerSecond = getRuntimeContext().getMetricGroup().meter("numRecordsOutPerSecond", new MeterView(numRecordsOut));
//
//            // Initialize custom late records dropped counter (if applicable)
//            numLateRecordsDropped = getRuntimeContext().getMetricGroup().counter("numLateRecordsDropped");
//
//            // Initialize watermarks (if applicable)
//            currentInputWatermark = getRuntimeContext().getMetricGroup().gauge("currentInputWatermark", () -> lastInputWatermark);
//            currentOutputWatermark = getRuntimeContext().getMetricGroup().gauge("currentOutputWatermark", () -> lastEmittedWatermark);
//
//            // Initialize last metrics collection time
//            lastMetricsCollectionTime = System.currentTimeMillis();
//
//            // Initialize the BufferedWriter for saving metrics to a file
//            writer = new BufferedWriter(new FileWriter("metrics_tokenizer_WCTH.csv", true));
//            writer.write("Timestamp, Subtask, Throughput, Latency(ns), CPU Usage, Memory Usage, Error Count, Success Count, " +
//                    "GC Count, GC Time, Thread Count, Process CPU Load, Process CPU Time, " +
//                    "P90 Latency, P99 Latency, P999 Latency, numRecordsIn, numRecordsInPerSecond, numRecordsOut, numRecordsOutPerSecond, " +
//                    "CPU Idle%, CPU Sys%, CPU User%, CPU IOWait%, CPU Irq%, CPU SoftIrq%, CPU Nice%, CPU Steal%, " +
//                    "Load1min, Load5min, Load15min, Total Memory, Available Memory, Total Swap, Used Swap, " +
//                    "Per-CPU Usage%, Network Receive Rates (bytes/s), Network Send Rates (bytes/s), " +
//                    "Current Power Usage (W), Total Energy Consumed (Wh)\n");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//
//        // Initialize checkpoint counter
//        checkpointCounter = getRuntimeContext().getMetricGroup().counter("completedCheckpoints");
//
//        // Initialize state and state size gauge
//        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
//                "state", Integer.class, 0);
//        try {
//            state = getRuntimeContext().getState(descriptor);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        stateSizeGauge = getRuntimeContext().getMetricGroup().gauge("stateSize", () -> {
//            try {
//                return state.value() != null ? 4L : 0L; // Assuming Integer size is 4 bytes
//            } catch (Exception e) {
//                return 0L;
//            }
//        });
//
//
//    }
//
//    private void outputTopWords() {
//        // Sort the words by their count and extract the top 3 and top 5
//        List<Map.Entry<String, Integer>> sortedEntries = wordCountMap.entrySet()
//                .stream()
//                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
//                .collect(Collectors.toList());
//
//        // Get top 3 and top 5 words
//        List<String> top3Words = sortedEntries.stream()
//                .limit(3)
//                .map(entry -> entry.getKey() + ": " + entry.getValue())
//                .collect(Collectors.toList());
//
//        List<String> top5Words = sortedEntries.stream()
//                .limit(5)
//                .map(entry -> entry.getKey() + ": " + entry.getValue())
//                .collect(Collectors.toList());
//
//        // Log the top 3 and top 5 words
//        LOG.info("Top 3 words: {}", top3Words);
//        LOG.info("Top 5 words: {}", top5Words);
//    }
//
//    @Override
//    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
//        numRecordsIn.inc(); // Increment numRecordsIn for each received record
//
//        long startTime = System.nanoTime(); // Start time in nanoseconds
//
//        try {
//            // Normalize the input string to lowercase and split it into words
//            String[] words = value.toLowerCase().split("\\W+");
//
//            // Emit each word as a tuple of (word, 1) and increment the counters
//            for (String word : words) {
//                if (word.length() > 0) {
//                    out.collect(new Tuple2<>(word, 1));
//                    wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + 1);
//                    numRecordsOut.inc(); // Increment numRecordsOut for each emitted record
//
//                    wordCounter.inc();               // Increment word count
//                    throughputMeter.markEvent();     // Mark an event for throughput
//                    successCounter.inc();
//                }
//            }
//
//            // Output top words periodically (e.g., every 1000 words processed)
//            if (wordCountMap.size() % 1000 == 0) {
//                outputTopWords();
//            }
//
//        } catch (Exception e) {
//            errorCounter.inc();
//            LOG.error("Error processing record: {}", value, e);
//        }
//
//        long endTime = System.nanoTime(); // End time in nanoseconds
//        long latency = endTime - startTime; // Calculate latency in nanoseconds
//
//        // Update histograms with the latency
//        processingLatencyHistogram.update(latency);
//        latencyHistogram.update(latency);
//
//        // Get latency percentiles
//        Snapshot snapshot = ((DropwizardHistogramWrapper) latencyHistogram).getDropwizardHistogram().getSnapshot();
//        long p90 = (long) snapshot.get95thPercentile();
//        long p99 = (long) snapshot.get99thPercentile();
//        long p999 = (long) snapshot.get999thPercentile();
//
//        long currentTimeMillis = System.currentTimeMillis();
//        if (currentTimeMillis - lastMetricsCollectionTime >= metricsCollectionInterval) {
//            collectSystemMetrics(currentTimeMillis, latency, p90, p99, p999);
//            lastMetricsCollectionTime = currentTimeMillis;
//        }
//    }
//
//    private void collectSystemMetrics(long currentTimeMillis, long latency, long p90, long p99, long p999) {
//        // Capture system resource metrics
//        double cpuLoadBetweenTicks = processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100;
//        long[] ticks = processor.getSystemCpuLoadTicks();
//
//        long user = ticks[CentralProcessor.TickType.USER.getIndex()] - prevTicks[CentralProcessor.TickType.USER.getIndex()];
//        long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
//        long sys = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
//        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
//        long iowait = ticks[CentralProcessor.TickType.IOWAIT.getIndex()] - prevTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
//        long irq = ticks[CentralProcessor.TickType.IRQ.getIndex()] - prevTicks[CentralProcessor.TickType.IRQ.getIndex()];
//        long softirq = ticks[CentralProcessor.TickType.SOFTIRQ.getIndex()] - prevTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
//        long steal = ticks[CentralProcessor.TickType.STEAL.getIndex()] - prevTicks[CentralProcessor.TickType.STEAL.getIndex()];
//        long totalCpu = user + nice + sys + idle + iowait + irq + softirq + steal;
//
//        double userPct = 100d * user / totalCpu;
//        double nicePct = 100d * nice / totalCpu;
//        double sysPct = 100d * sys / totalCpu;
//        double idlePct = 100d * idle / totalCpu;
//        double iowaitPct = 100d * iowait / totalCpu;
//        double irqPct = 100d * irq / totalCpu;
//        double softirqPct = 100d * softirq / totalCpu;
//        double stealPct = 100d * steal / totalCpu;
//
//        prevTicks = ticks;
//
//        // Get load averages
//        double[] loadAverages = processor.getSystemLoadAverage(3);
//        double load1min = loadAverages[0];
//        double load5min = loadAverages[1];
//        double load15min = loadAverages[2];
//
//        // Get per-CPU usage
//        double[] processorLoadBetweenTicks = processor.getProcessorCpuLoadBetweenTicks(prevProcTicks);
//        prevProcTicks = processor.getProcessorCpuLoadTicks();
//
//        // Get memory metrics
//        GlobalMemory memory = systemInfo.getHardware().getMemory();
//        long totalMemory = memory.getTotal();
//        long availableMemory = memory.getAvailable();
//
//        // Get swap metrics
//        long totalSwap = memory.getVirtualMemory().getSwapTotal();
//        long usedSwap = memory.getVirtualMemory().getSwapUsed();
//
//        // Get GC metrics
//        long gcCount = 0;
//        long gcTime = 0;
//        for (GarbageCollectorMXBean gcBean : gcBeans) {
//            gcCount += gcBean.getCollectionCount();
//            gcTime += gcBean.getCollectionTime();
//        }
//
//        // Get thread count
//        int threadCount = threadBean.getThreadCount();
//
//        // Get process CPU load and time
//        double processCpuLoad = osBean.getProcessCpuLoad() * 100;
//        long processCpuTime = osBean.getProcessCpuTime();
//
//        // Capture network I/O metrics
//        Map<String, Double> netRecvRates = new HashMap<>();
//        Map<String, Double> netSendRates = new HashMap<>();
//        for (NetworkIF net : networkIFs) {
//            net.updateAttributes();
//            long bytesRecv = net.getBytesRecv();
//            long bytesSent = net.getBytesSent();
//
//            long prevRecv = prevNetBytesRecv.getOrDefault(net.getName(), 0L);
//            long prevSent = prevNetBytesSent.getOrDefault(net.getName(), 0L);
//
//            double recvRate = (bytesRecv - prevRecv) * 1000.0 / metricsCollectionInterval; // bytes per second
//            double sendRate = (bytesSent - prevSent) * 1000.0 / metricsCollectionInterval; // bytes per second
//
//            netRecvRates.put(net.getName(), recvRate);
//            netSendRates.put(net.getName(), sendRate);
//
//            prevNetBytesRecv.put(net.getName(), bytesRecv);
//            prevNetBytesSent.put(net.getName(), bytesSent);
//        }
//
//        // Get power usage
//        double currentPowerUsage = 0.0; // in watts
//
//        if (powerSources != null && powerSources.size() > 0) {
//            // Sum the power usage from all power sources
//            for (PowerSource ps : powerSources) {
//                currentPowerUsage += ps.getPowerUsageRate(); // in watts
//            }
//        } else {
//            // No power sources available, estimate power usage based on CPU usage
//            // Assume a maximum power consumption (e.g., TDP)
//            double maxPower = 115.0; // Assume a TDP of 65W for CPU
//            double cpuUsageFraction = cpuLoadBetweenTicks / 100.0; // Convert percentage to fraction
//            currentPowerUsage = cpuUsageFraction * maxPower;
//        }
//
//        long timeIntervalMillis = currentTimeMillis - lastPowerTimestamp; // Time since last measurement
//
//        // Calculate energy consumed during this interval
//        double energyConsumed = currentPowerUsage * timeIntervalMillis / (1000.0 * 3600.0); // Convert to watt-hours
//
//        // Update total energy consumed
//        totalEnergyConsumed += energyConsumed;
//
//        // Update last power usage and timestamp
//        lastPowerUsage = currentPowerUsage;
//        lastPowerTimestamp = currentTimeMillis;
//
//        // Write metrics to the file
//        try {
//            StringBuilder sb = new StringBuilder();
//            sb.append(currentTimeMillis).append(", ")
//                    .append(subtaskIndex).append(", ")
//                    .append(throughputMeter.getRate()).append(", ")
//                    .append(latency).append(", ")
//                    .append(cpuLoadBetweenTicks).append(", ")
//                    .append(memoryUsageGauge.getValue()).append(", ")
//                    .append(errorCounter.getCount()).append(", ")
//                    .append(successCounter.getCount()).append(", ")
//                    .append(gcCount).append(", ")
//                    .append(gcTime).append(", ")
//                    .append(threadCount).append(", ")
//                    .append(processCpuLoad).append(", ")
//                    .append(processCpuTime).append(", ")
//                    .append(p90).append(", ").append(p99).append(", ").append(p999).append(", ")
//                    .append(numRecordsIn.getCount()).append(", ")
//                    .append(numRecordsInPerSecond.getRate()).append(", ")
//                    .append(numRecordsOut.getCount()).append(", ")
//                    .append(numRecordsOutPerSecond.getRate()).append(", ")
//                    .append(idlePct).append(", ")
//                    .append(sysPct).append(", ")
//                    .append(userPct).append(", ")
//                    .append(iowaitPct).append(", ")
//                    .append(irqPct).append(", ")
//                    .append(softirqPct).append(", ")
//                    .append(nicePct).append(", ")
//                    .append(stealPct).append(", ")
//                    .append(load1min).append(", ")
//                    .append(load5min).append(", ")
//                    .append(load15min).append(", ")
//                    .append(totalMemory).append(", ")
//                    .append(availableMemory).append(", ")
//                    .append(totalSwap).append(", ")
//                    .append(usedSwap);
//
////            // Append per-CPU usage
////            for (int i = 0; i < processorLoadBetweenTicks.length; i++) {
////                sb.append(", ").append("CPU").append(i).append(" Usage%: ").append(processorLoadBetweenTicks[i] * 100);
////            }
//
//            // Append network rates
////            for (String netName : netRecvRates.keySet()) {
////                sb.append(", ").append(netName).append(" ReceiveRate: ").append(netRecvRates.get(netName));
////            }
////            for (String netName : netSendRates.keySet()) {
////                sb.append(", ").append(netName).append(" SendRate: ").append(netSendRates.get(netName));
////            }
//
//            // Append power usage and energy consumption
//            sb.append(", ").append(currentPowerUsage).append(", ").append(totalEnergyConsumed);
//
//            sb.append("\n");
//
//            writer.write(sb.toString());
//            writer.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        // Log the metrics using SLF4J
//        LOG.info("Tokenizer Subtask {}: CPU Usage: {}%, Memory Usage: {} bytes, Power Usage: {} W, Total Energy: {} Wh, Errors: {}, Successes: {}, Threads: {}, Process CPU Load: {}%, numRecordsIn: {}, numRecordsOut: {}",
//                subtaskIndex, cpuLoadBetweenTicks, memoryUsageGauge.getValue(), currentPowerUsage, totalEnergyConsumed, errorCounter.getCount(), successCounter.getCount(),
//                threadCount, processCpuLoad, numRecordsIn.getCount(), numRecordsOut.getCount());
//    }
//
//    @Override
//    public void close() throws Exception {
//        outputTopWords();
//        super.close();
//        if (writer != null) {
//            writer.close(); // Close the writer when the function is done
//        }
//    }
//
//    @Override
//    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
//        // Implement state snapshot logic if needed
//    }
//
//    @Override
//    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
//        // Implement state initialization logic if needed
//    }
//}
//
//
//    /**
//     * SumReducer class sums up the counts of words.
//     * It also measures per-operator throughput and latency.
//     */
//    public static final class SumReducer extends RichReduceFunction<Tuple2<String, Integer>> {
//
//        private static final Logger LOG = LoggerFactory.getLogger(SumReducer.class);
//
//        private transient Meter throughputMeter;
//        private transient Histogram processingLatencyHistogram;
//        private transient BufferedWriter writer;
//        private transient SystemInfo systemInfo;
//        private transient CentralProcessor processor;
//        private transient long[] prevTicks;
//        private transient Gauge<Double> cpuUsageGauge;
//        private transient Gauge<Long> memoryUsageGauge;
//        private transient Counter errorCounter;
//        private transient Counter successCounter;
//        private transient List<GarbageCollectorMXBean> gcBeans;
//        private transient ThreadMXBean threadBean;
//        private transient OperatingSystemMXBean osBean;
//        private int subtaskIndex;
//
//        // New Metrics
//        private transient Counter numRecordsIn;
//        private transient Meter numRecordsInPerSecond;
//        private transient Counter numRecordsOut;
//        private transient Meter numRecordsOutPerSecond;
//
//        @Override
//        public void open(Configuration config) {
//            // Initialize throughputMeter
//            throughputMeter = getRuntimeContext().getMetricGroup()
//                    .meter("throughput", new MeterView(1)); // 1-second time span
//
//            // Initialize the processing latency histogram
//            processingLatencyHistogram = getRuntimeContext().getMetricGroup()
//                    .histogram("processingLatency", new DropwizardHistogramWrapper(
//                            new com.codahale.metrics.Histogram(new SlidingWindowReservoir(10))));
//
//            try {
//                writer = new BufferedWriter(new FileWriter("metrics_sum_reducer_WCTH.csv", true));
//                writer.write("Timestamp, Subtask, Throughput, Latency(ns), CPU Usage, Memory Usage, Error Count, Success Count, numRecordsIn, numRecordsInPerSecond, numRecordsOut, numRecordsOutPerSecond\n");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            // Initialize error and success counters
//            errorCounter = getRuntimeContext().getMetricGroup().counter("errorCount");
//            successCounter = getRuntimeContext().getMetricGroup().counter("successCount");
//
//            // Initialize OSHI system info and processor
//            systemInfo = new SystemInfo();
//            HardwareAbstractionLayer hardware = systemInfo.getHardware();
//            processor = systemInfo.getHardware().getProcessor();
//            prevTicks = processor.getSystemCpuLoadTicks();
//
//
//            // Register CPU Usage Gauge
//            cpuUsageGauge = getRuntimeContext().getMetricGroup().gauge("cpuUsage", () -> {
//                long[] currentTicks = processor.getSystemCpuLoadTicks();
//                double load = processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100;
//                prevTicks = currentTicks;
//                return load;
//            });
//
//            // Register Memory Usage Gauge
//            memoryUsageGauge = getRuntimeContext().getMetricGroup().gauge("memoryUsage", () -> {
//                Runtime runtime = Runtime.getRuntime();
//                return runtime.totalMemory() - runtime.freeMemory();
//            });
//
//            // Initialize GC beans
//            gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
//
//            // Initialize ThreadMXBean
//            threadBean = ManagementFactory.getThreadMXBean();
//
//            // Initialize OperatingSystemMXBean
//            osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
//
//            // Get subtask index
//            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
//
//            // Initialize I/O Metrics
//            numRecordsIn = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
//            numRecordsOut = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
//
//            // Create MeterViews for per-second rates
//            numRecordsInPerSecond = getRuntimeContext().getMetricGroup().meter("numRecordsInPerSecond", new MeterView(numRecordsIn));
//            numRecordsOutPerSecond = getRuntimeContext().getMetricGroup().meter("numRecordsOutPerSecond", new MeterView(numRecordsOut));
//        }
//
//        @Override
//        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
//            numRecordsIn.inc(2); // Two records are being reduced
//
//            long startTime = System.nanoTime(); // Start time measurement
//
//            Tuple2<String, Integer> result;
//            try {
//                result = new Tuple2<>(value1.f0, value1.f1 + value2.f1);
//
//                numRecordsOut.inc();
//                successCounter.inc();
//                throughputMeter.markEvent();
//
//                long endTime = System.nanoTime(); // End time measurement
//                long latency = endTime - startTime; // Calculate latency
//
//                // Update histogram
//                processingLatencyHistogram.update(latency);
//
//                // Capture system resource metrics
//                double cpuLoad = cpuUsageGauge.getValue();
//                long memoryUsage = memoryUsageGauge.getValue();
//
//                // Get thread count
//                int threadCount = threadBean.getThreadCount();
//
//                // Log throughput and latency
//                long currentTime = System.currentTimeMillis();
//                writer.write(currentTime + ", "
//                        + subtaskIndex + ", "
//                        + throughputMeter.getRate() + ", "
//                        + latency + ", "
//                        + cpuLoad + ", "
//                        + memoryUsage + ", "
//                        + errorCounter.getCount() + ", "
//                        + successCounter.getCount() + ", "
//                        + numRecordsIn.getCount() + ", "
//                        + numRecordsInPerSecond.getRate() + ", "
//                        + numRecordsOut.getCount() + ", "
//                        + numRecordsOutPerSecond.getRate() + "\n");
//                writer.flush();
//
//                // Log the metrics using SLF4J
//                LOG.info("SumReducer Subtask {}: Throughput: {}, Latency: {} ns, CPU Load: {}%, Memory Usage: {} bytes, numRecordsIn: {}, numRecordsOut: {}",
//                        subtaskIndex, throughputMeter.getRate(), latency, cpuLoad, memoryUsage,
//                        numRecordsIn.getCount(), numRecordsOut.getCount());
//
//                return result;
//            } catch (Exception e) {
//                errorCounter.inc();
//                LOG.error("Error reducing values: {}, {}", value1, value2, e);
//                return value1; // Or handle accordingly
//            }
//        }
//
//        @Override
//        public void close() throws Exception {
//            super.close();
//            if (writer != null) {
//                writer.close();
//            }
//        }
//    }
//
//    /**
//     * FormatMapFunction class formats the word counts for output.
//     * It also measures per-operator throughput and latency.
//     */
//    public static final class FormatMapFunction extends RichMapFunction<Tuple2<String, Integer>, String> {
//
//        private static final Logger LOG = LoggerFactory.getLogger(FormatMapFunction.class);
//
//        private transient Meter throughputMeter;
//        private transient Histogram processingLatencyHistogram;
//        private transient BufferedWriter writer;
//        private transient SystemInfo systemInfo;
//        private transient CentralProcessor processor;
//        private transient long[] prevTicks;
//        private transient Gauge<Double> cpuUsageGauge;
//        private transient Gauge<Long> memoryUsageGauge;
//        private transient Counter errorCounter;
//        private transient Counter successCounter;
//        private transient List<GarbageCollectorMXBean> gcBeans;
//        private transient ThreadMXBean threadBean;
//        private transient OperatingSystemMXBean osBean;
//        private int subtaskIndex;
//
//        // New Metrics
//        private transient Counter numRecordsIn;
//        private transient Meter numRecordsInPerSecond;
//        private transient Counter numRecordsOut;
//        private transient Meter numRecordsOutPerSecond;
//
//        @Override
//        public void open(Configuration config) {
//            // Initialize throughputMeter
//
//            throughputMeter = getRuntimeContext().getMetricGroup()
//                    .meter("throughput", new MeterView(1)); // 1-second time span
//
//            // Initialize the processing latency histogram
//            processingLatencyHistogram = getRuntimeContext().getMetricGroup()
//                    .histogram("processingLatency", new DropwizardHistogramWrapper(
//                            new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
//
//            try {
//                writer = new BufferedWriter(new FileWriter("metrics_format_map_WCTH.csv", true));
//                writer.write("Timestamp, Subtask, Throughput, Latency(ns), CPU Usage, Memory Usage, Error Count, Success Count, numRecordsIn, numRecordsInPerSecond, numRecordsOut, numRecordsOutPerSecond\n");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            // Initialize error and success counters
//            errorCounter = getRuntimeContext().getMetricGroup().counter("errorCount");
//            successCounter = getRuntimeContext().getMetricGroup().counter("successCount");
//
//            // Initialize OSHI system info and processor
//            systemInfo = new SystemInfo();
//            processor = systemInfo.getHardware().getProcessor();
//            prevTicks = processor.getSystemCpuLoadTicks();
//
//            // Register CPU Usage Gauge
//            cpuUsageGauge = getRuntimeContext().getMetricGroup().gauge("cpuUsage", () -> {
//                long[] currentTicks = processor.getSystemCpuLoadTicks();
//                double load = processor.getSystemCpuLoadBetweenTicks(prevTicks) * 100;
//                prevTicks = currentTicks;
//                return load;
//            });
//
//            // Register Memory Usage Gauge
//            memoryUsageGauge = getRuntimeContext().getMetricGroup().gauge("memoryUsage", () -> {
//                Runtime runtime = Runtime.getRuntime();
//                return runtime.totalMemory() - runtime.freeMemory();
//            });
//
//            // Initialize GC beans
//            gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
//
//            // Initialize ThreadMXBean
//            threadBean = ManagementFactory.getThreadMXBean();
//
//            // Initialize OperatingSystemMXBean
//            osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
//
//            // Get subtask index
//            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
//
//            // Initialize I/O Metrics
//            numRecordsIn = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
//            numRecordsOut = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
//
//            // Create MeterViews for per-second rates
//            numRecordsInPerSecond = getRuntimeContext().getMetricGroup().meter("numRecordsInPerSecond", new MeterView(numRecordsIn));
//            numRecordsOutPerSecond = getRuntimeContext().getMetricGroup().meter("numRecordsOutPerSecond", new MeterView(numRecordsOut));
//        }
//
//        @Override
//        public String map(Tuple2<String, Integer> tuple) {
//            numRecordsIn.inc();
//
//            long startTime = System.nanoTime(); // Start time measurement
//
//            String result;
//            try {
//                result = tuple.f0 + ": " + tuple.f1;
//
//                numRecordsOut.inc();
//                successCounter.inc();
//                throughputMeter.markEvent();
//
//                long endTime = System.nanoTime(); // End time measurement
//                long latency = endTime - startTime; // Calculate latency
//
//
//                // Update histogram
//                processingLatencyHistogram.update(latency);
//
//                // Capture system resource metrics
//                double cpuLoad = cpuUsageGauge.getValue();
//                long memoryUsage = memoryUsageGauge.getValue();
//
//                // Get thread count
//                int threadCount = threadBean.getThreadCount();
//
//                // Log throughput and latency
//                long currentTime = System.currentTimeMillis();
//                writer.write(currentTime + ", "
//                        + subtaskIndex + ", "
//                        + throughputMeter.getRate() + ", "
//                        + latency + ", "
//                        + cpuLoad + ", "
//                        + memoryUsage + ", "
//                        + errorCounter.getCount() + ", "
//                        + successCounter.getCount() + ", "
//                        + numRecordsIn.getCount() + ", "
//                        + numRecordsInPerSecond.getRate() + ", "
//                        + numRecordsOut.getCount() + ", "
//                        + numRecordsOutPerSecond.getRate() + "\n");
//                writer.flush();
//
//                // Log the metrics using SLF4J
//                LOG.info("FormatMapFunction Subtask {}: Throughput: {}, Latency: {} ns, CPU Load: {}%, Memory Usage: {} bytes, numRecordsIn: {}, numRecordsOut: {}",
//                        subtaskIndex, throughputMeter.getRate(), latency, cpuLoad, memoryUsage,
//                        numRecordsIn.getCount(), numRecordsOut.getCount());
//
//                return result;
//            } catch (Exception e) {
//                errorCounter.inc();
//                LOG.error("Error formatting tuple: {}", tuple, e);
//                return null; // Or handle accordingly
//            }
//        }
//
//        @Override
//        public void close() throws Exception {
//            super.close();
//            if (writer != null) {
//                writer.close();
//            }
//        }
//    }
//}
