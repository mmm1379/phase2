import random
from keras.models import load_model
import time
import os
import base64
import json
import cv2
import numpy as np

# Flink and Kafka imports
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
)
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import MapFunction

# Define label dictionary for the emotion model (if needed)
label_dict = {0: 'Angry', 1: 'Disgust', 2: 'Fear', 3: 'Happy', 4: 'Sad', 5: 'Surprise', 6: 'Neutral'}


# ---------------------------------------------------------------------------
# Image Processing MapFunction (using two models)
# ---------------------------------------------------------------------------
class ImageProcessor(MapFunction):
    def __init__(self, emotion_model_path, drowsiness_model_path):
        self.emotion_model_path = emotion_model_path
        self.drowsiness_model_path = drowsiness_model_path
        self.emotion_model = None
        self.drowsiness_model = None
        self.emotion_model = load_model(self.emotion_model_path)
        self.drowsiness_model = load_model(self.drowsiness_model_path)

    def open(self, runtime_context):
        # Load both models when the function starts.
        pass
        # print(self.emotion_model)
        # print(self.drowsiness_model)

    def map(self, json_string):
        try:
            # Parse the JSON message.
            payload = json.loads(json_string)
            # Expect the base64-encoded JPEG image in the "data" field.
            base64_str = payload["data"]
            base64_decoded = base64.b64decode(base64_str)
            image_array = np.frombuffer(base64_decoded, dtype=np.uint8)
            image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
            if image is None:
                print("Failed to decode image.")
                return json.dumps({"emotion": -1, "drowsiness": -1})

            # Preprocess for the emotion model:
            img_gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            img_resized = cv2.resize(img_gray, (48, 48))
            img_norm = img_resized.astype("float32") / 255.0
            img_input = img_norm.reshape(1, 48, 48, 1)
            emotion_prediction = self.emotion_model.predict(img_input)
            emotion_index = int(np.argmax(emotion_prediction[0]))

            # Preprocess for the drowsiness model:
            # (Here we resize the original image to 80x80.)
            img_resized_drowsy = cv2.resize(image, (80, 80))
            img_norm_drowsy = img_resized_drowsy.astype("float32") / 255.0
            drowsiness_prediction = self.drowsiness_model.predict(img_norm_drowsy[np.newaxis, ...])
            drowsiness_index = int(np.argmax(drowsiness_prediction[0]))

            # Return a JSON string with numeric predictions.
            result = {"emotion": emotion_index, "drowsiness": drowsiness_index}
            return json.dumps(result)
        except Exception as e:
            print(f"Error processing image: {e}")
            return json.dumps({"emotion": -1, "drowsiness": -1})


# ---------------------------------------------------------------------------
# EEG Processing MapFunction
# ---------------------------------------------------------------------------
class EEGProcessor(MapFunction):
    def __init__(self, eeg_model_path):
        self.eeg_model_path = eeg_model_path
        self.eeg_model = None

    def open(self, runtime_context):
        # Load the EEG model once at startup.
        self.eeg_model = load_model(self.eeg_model_path)

    @staticmethod
    def preprocess_eeg_bytes(eeg_bytes, fs=100, spec_len=30, n_fft=256, hop_length=64, n_mels=64):
        """
        Preprocess raw EEG data (in pickled bytes) into groups of 5 mel-spectrogram segments.

        Parameters:
            eeg_bytes (bytes): Pickled pandas DataFrame containing raw EEG data.
            fs (int): Sampling frequency (default 100 Hz).
            spec_len (int): Segment length in seconds (default 30).
            n_fft (int): FFT window length for mel spectrogram.
            hop_length (int): Hop length for mel spectrogram.
            n_mels (int): Number of mel frequency bins.

        Returns:
            X1, X2, X3, X4, X5 (numpy arrays): Each with shape (n_groups, n_mels, time_bins, 2)
                where each array corresponds to a segment position (1st, 2nd, …, 5th) in a group.
        """
        import pickle
        import pandas as pd
        import librosa
        from librosa.feature import melspectrogram
        from librosa.core import power_to_db

        data = pickle.loads(eeg_bytes)
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Decoded EEG data is not a pandas DataFrame.")

        for ch in ['EEG Fpz-Cz', 'EEG Pz-Oz']:
            if ch not in data.columns:
                raise ValueError(f"Column '{ch}' not found in EEG data.")

        ch1 = data['EEG Fpz-Cz'].values
        ch2 = data['EEG Pz-Oz'].values

        segment_samples = fs * spec_len
        num_segments = len(data) // segment_samples

        groups = []
        current_group = []
        for seg in range(num_segments):
            start = seg * segment_samples
            end = start + segment_samples

            seg_ch1 = ch1[start:end]
            seg_ch2 = ch2[start:end]

            if np.std(seg_ch1) == 0 or np.std(seg_ch2) == 0:
                continue

            seg_ch1 = (seg_ch1 - np.mean(seg_ch1)) / np.std(seg_ch1)
            seg_ch2 = (seg_ch2 - np.mean(seg_ch2)) / np.std(seg_ch2)

            spec_ch1 = melspectrogram(y=seg_ch1, sr=fs, n_fft=n_fft, hop_length=hop_length, n_mels=n_mels)
            spec_ch1 = power_to_db(spec_ch1, ref=np.max)
            spec_ch2 = melspectrogram(y=seg_ch2, sr=fs, n_fft=n_fft, hop_length=hop_length, n_mels=n_mels)
            spec_ch2 = power_to_db(spec_ch2, ref=np.max)

            seg_combined = np.stack([spec_ch1, spec_ch2], axis=-1)
            current_group.append(seg_combined)
            if len(current_group) == 5:
                groups.append(current_group)
                current_group = []
        if len(groups) == 0:
            raise ValueError("No valid EEG segment groups were generated.")

        X1, X2, X3, X4, X5 = [], [], [], [], []
        for group in groups:
            X1.append(group[0])
            X2.append(group[1])
            X3.append(group[2])
            X4.append(group[3])
            X5.append(group[4])
        return np.array(X1), np.array(X2), np.array(X3), np.array(X4), np.array(X5)

    def map(self, json_string):
        try:
            data = json.loads(json_string)
            eeg_base64 = data.get("eeg_data")
            if eeg_base64 is None:
                return json.dumps({"eeg": -1})

            eeg_bytes = base64.b64decode(eeg_base64)
            X1, X2, X3, X4, X5 = EEGProcessor.preprocess_eeg_bytes(eeg_bytes)
            prediction = self.eeg_model.predict([X1, X2, X3, X4, X5])
            eeg_pred_class = int(np.argmax(prediction[0]))
            return json.dumps({"eeg": eeg_pred_class})
        except Exception as e:
            print(f"EEG processing error: {e}")
            return json.dumps({"eeg": -1})


# ---------------------------------------------------------------------------
# Function to simulate EEG model output (without running the model)
# ---------------------------------------------------------------------------
def simulate_eeg_prediction(json_string):
    """
    Returns a simulated EEG prediction as a JSON string with the "eeg" field.
    """
    # simulated_class = random.randint(0, 2)
    return json.dumps({"eeg": 2})


# ---------------------------------------------------------------------------
# Main Flink Job
# ---------------------------------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_bootstrap_servers = "localhost:9092"
    # Our input payload now contains both image and EEG data.
    input_topic = "processing_raw"
    output_topic = "processing_result"
    group_id = "data-processing-group"
    # Model paths (adjust these paths as needed)
    emotion_model_path = 'model_fer2013.keras'
    drowsiness_model_path = 'drowsiness.keras'
    eeg_model_path = 'model_eeg.h5'

    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_bootstrap_servers) \
        .set_topics(input_topic) \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_bootstrap_servers) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(output_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    data_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    def process_data(json_string):
        """
        Process incoming JSON messages.
        Expected JSON structure:
            {
                "eeg_data": "<base64-encoded EEG data>",
                "image_data": "<base64-encoded image data>"
            }
        The function processes both image and EEG data, combines the results, and returns
        a JSON string containing three numeric fields:
            { "emotion": <double>, "drowsiness": <double>, "eeg": <double> }
        """
        try:
            data = json.loads(json_string)
            result = {}
            if "image_data" in data:
                image_payload = {"data": data["image_data"]}
                image_json = json.dumps(image_payload)
                image_result_str = ImageProcessor(emotion_model_path, drowsiness_model_path).map(image_json)
                image_result = json.loads(image_result_str)
                result.update(image_result)
            else:
                result.update({"emotion": -1, "drowsiness": -1})
            if "eeg_data" in data:
                # Use simulated EEG prediction.
                eeg_result_str = simulate_eeg_prediction(json_string)
                eeg_result = json.loads(eeg_result_str)
                result.update(eeg_result)
            else:
                result.update({"eeg": -1})
            print(json.dumps(result))
            return json.dumps(result)
        except Exception as e:
            print(e)
            return json.dumps({"emotion": -1, "drowsiness": -1, "eeg": -1, "error": str(e)})

    processed_stream = data_stream.map(lambda x: process_data(x), output_type=Types.STRING())
    processed_stream.sink_to(sink)
    env.execute("Data Processing Job")


if __name__ == "__main__":
    main()
