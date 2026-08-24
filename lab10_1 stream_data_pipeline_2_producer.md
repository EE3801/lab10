# Lab 10.1 Stream Data Pipeline II - Overview and Producer

- Scenario: Streaming audio\
  Stream audio, process it with a machine learning model, and save the data for reporting.

---
Create a new Jupyter notebook file named `stream_data_pipeline_2_producer.ipynb`. 

```python
import os
home_directory = os.path.expanduser("~")
os.chdir(os.path.join(home_directory, "Documents", "projects", "ee3801"))
```

# 1. Scenario: Streaming audio

The company wants to build an in-house speech transcription tool to be shown at a conference. One device records audio of the speaker remotely, another computer receives the audio and transcribes the audio in real time. The system should stream audio, transcribe it with OpenAI Whisper, and display the transcribed text in real-time.

# 1.1 Single-stream audio auto-transcription

In the previous lab exercise, you observed missing words in recording and transcription. In this lab, you will attempt to capture complete audio streams and transcribe them more reliably. Note the time taken to read, write, and transcribe the audio.

You need two jupyter notebooks (.ipynb) files running concurrently:
- Producer: this notebook `stream_data_pipeline_2_producer.ipynb`
- Consumer: `stream_data_pipeline_2_consumer.ipynb`

1. Go to AWS Console to start your EC2 instance. SSH into the EC2 instance:

    ```bash
    ssh -i "MyKeyPair.pem" ec2-user@<ip_address>
    ```

2. Start the Kafka containers:

    ```bash
    sudo service docker start
    # stop all containers
    docker stop $(docker ps -q)
    # start all kafka containers
    docker start $(docker ps -aq -f "name=kafka")
    ```

3. Verify the Kafka containers are running:

    ```bash
    # list all active docker containers
    docker ps -a
    ```

    If your EC2 instance public IP address changed, stop and remove the Kafka containers, recreate them, and recreate the topic:

    ```bash
    # stop all kafka containers
    docker stop $(docker ps -q -f "name=kafka")
    # remove all kafka containers
    docker rm $(docker ps -aq -f "name=kafka")
    # change directory
    cd ~/dev_kafka
    # create kafka containers
    IMAGE=apache/kafka:latest PUBLIC_IP_ADDRESS=<ip_address> docker-compose up
    ```

    Then recreate the topic:

    ```bash
    # restart all kafka containers
    docker restart $(docker ps -aq -f "name=kafka")
    # enter kafka-1 container environment
    docker exec -it kafka-1 /bin/bash
    # create topic
    /opt/kafka/bin/kafka-topics.sh --create --topic dataengineering --replication-factor 2 --bootstrap-server localhost:9092
    # view topics
    /opt/kafka/bin/kafka-topics.sh --describe --topic dataengineering --bootstrap-server localhost:9092
    # exit kafka-1 container
    exit
    ```

    Test the producer and consumer in separate server terminals:

    ```bash
    # ssh into EC2 instance and start producer
    docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh --topic dataengineering --bootstrap-server localhost:9092
    # ssh into EC2 instance and start consumer
    docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --topic dataengineering --from-beginning --bootstrap-server localhost:9092
    ```



4. Install software to capture audio from your machine.

    ```python
    # Install Python packages
    # !python3 -m pip install --upgrade pip
    # !python3 -m pip install kafka-python

    # For Windows users (WSL)
    # !sudo add-apt-repository ppa:therealkenc/wsl-pulseaudio
    # !sudo apt update
    # !sudo apt install pulseaudio
    # !pip3 install pyaudio

    # For GNU/Linux users
    # !sudo apt install python3-pyaudio

    # For Apple Silicon users
    # !arch -arm64 /opt/homebrew/bin/brew install portaudio
    # !python3 -m pip cache purge
    # !python3 -m pip install pyaudio 
    # !python3 -m pip install scipy

    # For Windows users
    # !python3 -m pip install sounddevice
    # !python3 -m pip install pyaudio
    # !python3 -m pip install scipy
    ```

# 1.2 Stream audio input

1. Check the default audio input device. Copy and paste the codes into `stream_data_pipeline_2_producer.ipynb`:

    ```python
    import pyaudio

    # Initialize PyAudio
    p = pyaudio.PyAudio()

    try:
        # Get information about the default input device
        default_input_device_info = p.get_default_input_device_info()

        # Print relevant information
        print("Default Input Microphone Information:")
        print(f"  Name: {default_input_device_info['name']}")
        print(f"  Index: {default_input_device_info['index']}")
        print(f"  Host API: {default_input_device_info['hostApi']}")
        print(f"  Max Input Channels: {default_input_device_info['maxInputChannels']}")
        print(f"  Default Sample Rate: {default_input_device_info['defaultSampleRate']}")

    except OSError as e:
        print(f"Error getting default input device info: {e}")
        print("This may happen if no default input device is available or properly configured.")
    finally:
        # Terminate PyAudio
        p.terminate()
    ```

2. List all audio devices on your machine:

    ```python
    # Testing audio setup in this device
    import pyaudio

    audio = pyaudio.PyAudio()
    print("audio.get_device_count():", audio.get_device_count())
    for i in range(audio.get_device_count()):
        print(audio.get_device_info_by_index(i))

    audio.terminate()
    ```

3. Select the input and output devices, and note their index numbers:

    ```python
    # This is to determine which input audio and output audio you will use.
    # Explore and find the right index to use for input and output in your device.
    audio = pyaudio.PyAudio()
    input_device = audio.get_default_input_device_info()
    print("Selected input audio:", input_device["name"])
    print("  maxInputChannels:", input_device["maxInputChannels"])
    print("  defaultSampleRate:", input_device["defaultSampleRate"])
    print("Selected output audio:", audio.get_device_info_by_index(2))
    audio.terminate()
    ```

# 2. Read from the script while recording

```
- Producers are fairly straightforward: they send messages to a topic and partition, may request acknowledgments, may retry if a message fails, and then continue.

- Consumers are more complex: they read messages from a topic, run in a poll loop that waits for new messages, and can start from the beginning of the topic to read the entire history. Once caught up, the consumer waits for new messages.
```

# 3. Capture every sentence in a paragraph

Use your device to capture audio, record each sentence, and send the audio data to Kafka.

# 3.1 Initialize Producer

1. Replace `<ip_address>` with your AWS EC2 instance public IP address.
2. The code below creates a Kafka producer. If successful, it prints topic, partition, and offset information.

    ```python
    # kafka-python Producer
    from kafka import KafkaProducer

    public_ip_address = "<ip_address>"

    # produce asynchronously with callbacks
    producer = KafkaProducer(bootstrap_servers=[public_ip_address+':29092',public_ip_address+':39092',public_ip_address+':49092']) 

    def on_send_success(record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def on_send_error(excp):
        print('Send error:', excp)
        # Handle the exception here.
    ```

# 3.2 Capture audio and send data through Producer

1. The code below captures audio from the default input device.
2. It sends the raw audio data to the Kafka topic `dataengineering`.

    ## Using PyAudio

    ```python
    # Single thread audio

    import pyaudio
    from datetime import datetime

    FORMAT = pyaudio.paInt16
    CHUNK = 1024
    RECORD_SECONDS = 10
    # DEVICE_ID = 4

    audio = pyaudio.PyAudio()
    input_device = audio.get_default_input_device_info()
    RATE = int(input_device['defaultSampleRate'])
    CHANNELS = int(input_device['maxInputChannels'])
    INDEX = int(input_device['index'])

    start_time = datetime.now()

    stream = audio.open(
        format=FORMAT,
        channels=CHANNELS,
        rate=RATE,
        input=True,
        frames_per_buffer=CHUNK,
        input_device_index=INDEX
    )

    try:
        while True:
            before_time = datetime.now()
            frames = []
            for _ in range(int(RATE / CHUNK * RECORD_SECONDS)):
                data = stream.read(CHUNK, exception_on_overflow=False)
                frames.append(data)
            raw_data = b''.join(frames)

            # produce asynchronously with callbacks, data sent to topic dataengineering.
            producer.send('dataengineering', raw_data, key=b'audio') \
                .add_callback(on_send_success) \
                .add_errback(on_send_error)

            print("%s audio_duration (s): %s" % (
                datetime.now().strftime("%d/%m/%Y, %H:%M:%S"),
                (datetime.now() - before_time).seconds
            ))

            # block until all async messages are sent
            producer.flush()

            # exit program after 1 min
            if (datetime.now() - start_time).seconds > 60: 
                print("* Exit program after 1 min *")
                break

    except KeyboardInterrupt:
        print("* Program terminated by user *")
    except Exception as e:
        print("Exception:", e)
    finally:
        if stream is not None:
            stream.stop_stream()
            stream.close()
            audio.terminate()
    ```
    ## Using sounddevice
    ```python
    # Single thread audio

    import sounddevice as sd

    import wave
    import numpy as np
    from datetime import datetime
    import whisper
    import sys
    from scipy.signal import resample
    
    FORMAT = sd.default.dtype[0]
    RECORD_SECONDS = 5

    input_device = sd.query_devices(kind='input')
    RATE = int(input_device['default_samplerate'])
    CHUNK = int(RATE * RECORD_SECONDS)
    CHANNELS = int(input_device['max_input_channels'])
    INDEX = int(input_device['index'])

    start_time = datetime.now()
    message_counter = 0

    # Open a non-blocking stream that continuously captures audio
    stream = sd.InputStream(
        samplerate = RATE,
        channels = CHANNELS, 
        dtype = FORMAT, 
        device = INDEX,
        # callback = audio_callback,
        blocksize = CHUNK
    )

    try:
        with stream: # Automatically starts and cleans up the stream
            while True:
                before_time = datetime.now()
                
                raw_data, overflowed = stream.read(CHUNK)
                if overflowed:
                    print("Warning: Audio buffer overflowed!")

                # Convert multi-channel (Stereo) to Mono
                if CHANNELS > 1:
                    audio_data = raw_data.mean(axis=1)
                else:
                    audio_data = raw_data.flatten()

                audio_bytes = audio_data.astype(np.float32, copy=False).tobytes()

                unique_key = f"audio_{datetime.now().timestamp()}_{message_counter}".encode('utf-8')
                message_counter += 1
                
                # produce asynchronously with callbacks, data sent to topic dataengineering.
                producer.send('dataengineering', audio_bytes, key=unique_key)\
                        .add_callback(on_send_success)\
                        .add_errback(on_send_error)
                print("%s audio_duration (s): %s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), (datetime.now() - before_time).seconds))


                if (datetime.now() - start_time).seconds > 60: #exit program after 1min
                    print("* Exit program after 1min *")
                    break
            
    except KeyboardInterrupt as kie:
        print("* Program terminated by user *")
    except Exception as e:
        print("Exception:", e)
    finally:
        producer.flush()
        stream.stop()
        stream.close()

    ```


3. Open the instructions in [Lab 10 Stream Data Pipeline II Consumer](./lab10_2%20stream_data_pipeline_2_consumer.md).

4. Start the Kafka consumer notebook `stream_data_pipeline_2_consumer.ipynb` to receive the audio data.

5. What did you observe from the messages sent? Submit your findings.

# Conclusion

1. You have streamed audio data from your device and sent it to another computer.
2. You have used Kafka to transmit audio data for remote processing.

**Questions to ponder**

1. Which principle of good data architecture does Kafka fulfill?
2. Can Microsoft Power Apps perform stream processing?
3. What are the advantages and disadvantages of stream processing?

# Submissions next Wed 9pm (29 Oct 2025)  

Submit your notebook as a PDF. Save your notebook as an HTML file, open it in a browser, and print it as a PDF.

Include in your submission:
- In lab10_1 Section 3.2 output, step 5 and Answers to the questions to ponder
- In `lab10_2`, section 4 step 6 and 7, include a screenshot showing the data inserted into Kibana > Display.
- The submission should consists of two PDFs: `stream_data_pipeline_2_producer.ipynb` and `stream_data_pipeline_2_consumer.ipynb`

~ The End ~
