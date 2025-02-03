import cv2
import numpy as np
import pandas as pd
import pickle
import base64
import json
import time
from confluent_kafka import Consumer, Producer, KafkaError

# =============================================================================
# Kafka Configuration
# =============================================================================

# Consumer configuration (replace with your actual consumer group ID)
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'main-server',
    'auto.offset.reset': 'latest'
})

# Producer configuration
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def send_message(topic, key, message):
    """Send a message to the specified Kafka topic."""
    producer.produce(topic, key=key, value=message)
    producer.flush()

# =============================================================================
# EEG Data Simulation Function
# =============================================================================

def simulate_eeg_data(fs=100, duration_sec=30):
    """
    Simulate EEG data as a pandas DataFrame with two channels.
    
    Parameters:
        fs (int): Sampling frequency (Hz)
        duration_sec (int): Duration of the simulated EEG segment in seconds
    
    Returns:
        base64_eeg (str): Base64-encoded pickled EEG DataFrame
    """
    num_samples = fs * duration_sec
    time_vec = np.linspace(0, duration_sec, num_samples, endpoint=False)
    
    # Simulate two EEG channels (sine waves with a slight phase difference)
    dummy_ch1 = np.sin(2 * np.pi * 1 * time_vec)         # 1 Hz sine wave
    dummy_ch2 = np.sin(2 * np.pi * 1 * time_vec + 0.5)     # 1 Hz sine wave, phase shifted
    
    # Create a DataFrame with the simulated EEG data
    eeg_df = pd.DataFrame({
        'EEG Fpz-Cz': dummy_ch1,
        'EEG Pz-Oz': dummy_ch2
    })
    
    # Pickle the DataFrame and encode it in base64 (to send as a string)
    eeg_bytes = pickle.dumps(eeg_df)
    base64_eeg = base64.b64encode(eeg_bytes).decode('utf-8')
    return base64_eeg

# =============================================================================
# Main Loop: Capture Image, Simulate EEG, and Send over Kafka
# =============================================================================

# Open the camera using OpenCV
camera = cv2.VideoCapture(0)
if not camera.isOpened():
    print("Error: Could not open camera.")
    exit()

print("Press 'q' to quit and stop sending messages.")

last_result = None  # To store the latest message from the 'results' topic

try:
    # Subscribe to the Kafka topic for incoming results
    consumer.subscribe(['results'])

    while True:
        # Capture image from the camera
        ret, frame = camera.read()
        if not ret:
            print("Failed to capture image.")
            break

        # Encode the captured image as JPEG and then to base64
        success, encoded_image = cv2.imencode('.jpg', frame)
        if not success:
            print("Failed to encode image.")
            continue
        base64_image = base64.b64encode(encoded_image).decode('utf-8')

        # Simulate EEG data and encode it similarly
        base64_eeg = simulate_eeg_data(fs=100, duration_sec=30)

        # Create the JSON payload
        payload = {
            "eeg_data": base64_eeg,
            "image_data": base64_image
        }
        json_payload = json.dumps(payload).encode('utf-8')

        # Send the JSON message to the 'sensor-data' topic
        producer.produce('sensor-data', key='camera_1', value=json_payload)
        producer.flush()

        # Poll for new messages (non-blocking) on the 'results' topic
        msg = consumer.poll(0)
        if msg is not None:
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
            else:
                last_result = msg.value().decode('utf-8')
                print("Received result:", last_result)
                consumer.commit(msg)  # Commit offset after processing

        # Check if 'q' is pressed to exit
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

        # sleep briefly to simulate a realistic transmission rate
        time.sleep(0.5)

except KeyboardInterrupt:
    print("Interrupted by user. Exiting...")

finally:
    # Clean up: release camera and close Kafka connections
    camera.release()
    cv2.destroyAllWindows()
    consumer.close()
    producer.flush()
    print("Released camera and Kafka resources.")
