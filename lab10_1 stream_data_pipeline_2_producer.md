# Lab 10.1 Stream Data Pipeline II - Overview and Producer 

- Scenario: Streaming audio \
    Stream in audio, process, calling a machine learning classification model and save the data for reporting.

Create a new jupyter notebook file "stream_data_pipeline_2_producer.ipynb".


```python
import os
home_directory = os.path.expanduser("~")
os.chdir(home_directory+'/Documents/projects/ee3801')
```

# 1. Scenario: Streaming audio 

The company would like to build an in-house automatic speech transcribing tool. We record the streams of audio from one computer and another computer can receive this audio and transcribe in real-time. The system stream in audio, transcribe it using Open AI's whisper model and transcribed text is saved for reporting in real-time. 

# 1.1 Single stream audio data auto-transcription

In the previous lab exercise, you have observed missing words lost in recording and transcription. In this lab, you will attempt to capture all audio streams and transcribe to text. Take note of the time taken to read, write and transcribe the audio.

You will need two notebook to run concurrently.
- Producer - codes below
- Consumer - stream_data_pipeline_2_consumer.ipynb

1. SSH into the server.

    ```ssh -i "MyKeyPair.pem" ec2-user@<ip_address>```

2. Ensure all kafka containers are started.

    ```docker start $(docker ps -aq -f "name=kafka")```

3. Check if all kafka containers are started.

    ```docker ps -a```

Note: If your AWS EC2 instance public_ip_address is changed, you will need to stop and remove all kafka containers. Create them again and create a topic too. Follow these steps below:\

<!--```docker stop $(docker ps -q -f "name=kafka”)```

```docker rm $(docker ps -aq -f "name=kafka")```

```cd ~/dev_kafka```

```IMAGE=apache/kafka:latest PUBLIC_IP_ADDRESS=<ip_address> docker-compose up```-->

```export PUBLIC_IP_ADDRESS=<ip_address>```

```docker restart $(docker ps -aq -f "name=kafka")```

```docker exec -it kafka-1 /bin/bash```

```/opt/kafka/bin/kafka-topics.sh --create --topic dataengineering --replication-factor 2 --bootstrap-server localhost:9092```

```/opt/kafka/bin/kafka-topics.sh --describe --topic dataengineering --bootstrap-server localhost:9092```

```exit```

Test out the container using different terminals logged in to server.\
```docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh --topic dataengineering --bootstrap-server localhost:9092```

```docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --topic dataengineering --from-beginning --bootstrap-server localhost:9092```




4. Install softwares to capture audio from your machine.


```python
# # install python packages
# !pip install --upgrade pip

# # for mac users
# !brew install portaudio 
# !pip3 install pyaudio

# # for windows users
# # in wsl
# !sudo add-apt-repository ppa:therealkenc/wsl-pulseaudio sudo apt update
# !sudo apt install pulseaudio
# !pip3 install pyaudio

# # for GNU/Linux users
# !sudo apt install python3-pyaudio
```

# 1.2 Stream in audio 

1. Check default audio input.


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
        print("This might happen if no default input device is available or properly configured.")

    finally:
        # Terminate PyAudio
        p.terminate()
    ```

2. List down all audio in your machine.


    ```python
    # Testing audio setup in this device
    import pyaudio

    audio = pyaudio.PyAudio()
    print("audio.get_device_count():",audio.get_device_count())
    for i in range(audio.get_device_count()):
        print(audio.get_device_info_by_index(i))

    audio.terminate()
    ```

3. Choose the input and output audio in your machine. Take note of the index number of the audio input or output.


    ```python
    # This is to determine which input audio and output audio you will use.
    # Explore and find the right index to use for input and output in your device.
    audio = pyaudio.PyAudio()
    input = audio.get_default_input_device_info()
    print("Choosing my input audio: 'name':",input["name"],",'maxInputChannels':",input["maxInputChannels"],",'defaultSampleRate':",input["defaultSampleRate"])
    print("Choosing my output audio:",audio.get_device_info_by_index(2))
    audio.terminate()
    ```

# 2. Read from the script as you are recording
```
Producers are fairly straightforward – they send messages to a topic and partition, maybe request an acknowledgment, retry if a message fails – or not – and continue. Consumers, however, can be a little more complicated.

Consumers read messages from a topic. Consumers run in a poll loop that runs indefinitely waiting for messages. Consumers can read from the beginning – they will start at the first message in the topic and read the entire history. Once caught up, the consumer will wait for new messages.
```

# 3. Capture every sentence in a paragraph 
Using the device's audio capture the audio, record the sentence, send the audio data using Producer to a topic.

# 3.1 Initialise Producer

1. Replace the ```<ip_address>``` with your AWS EC2 instance public ip address.

2. The codes below is the Kafka Producer that will send data to the public id address. If successful it will print the topic, partition, offset information. If there is an error, it will be displayed as well. 


    ```python
    # kafka-python Producer
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    public_ip_address = "<ip_address>"

    # produce asynchronously with callbacks
    producer = KafkaProducer(bootstrap_servers=[public_ip_address+':29092',public_ip_address+':39092',public_ip_address+':49092']) 

    def on_send_success(record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def on_send_error(excp):
        print('I am an errback', exc_info=excp)
        # handle exception

    ```

# 3.2 Capture audio and send data through Producer

1. The codes below captures audio data from the default audio input device.

2. The audio data is then send using the Kafka Producer to the topic ```dataengineering```.


    ```python
    # Single thread audio

    import pyaudio
    import wave
    import numpy as np
    from datetime import datetime
    import whisper
    import sys

    FORMAT = pyaudio.paInt16
    CHUNK = 1024
    RECORD_SECONDS = 10
    # DEVICE_ID = 4

    audio = pyaudio.PyAudio()
    input = audio.get_default_input_device_info()
    RATE = int(input['defaultSampleRate'])
    CHANNELS = int(input['maxInputChannels'])
    INDEX = int(input['index'])

    audio = pyaudio.PyAudio()
    stream = audio.open(
        format=FORMAT,
        channels=CHANNELS,
        rate=RATE,
        input=True,
        frames_per_buffer=CHUNK,
        input_device_index=INDEX
    )

    start_time = datetime.now()

    try:
        while True:
            before_time = datetime.now()
            frames = []
            for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
                data = stream.read(CHUNK, exception_on_overflow = False)
                frames.append(data)
            raw_data = b''.join(frames)

            # produce asynchronously with callbacks, data sent to topic dataengineering.
            producer.send('dataengineering', raw_data, key="audio".encode('utf-8'))\
                    .add_callback(on_send_success)\
                    .add_errback(on_send_error)
            print("%s audio_duration (s): %s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), (datetime.now() - before_time).seconds))

            # block until all async messages are sent
            producer.flush()

            if (datetime.now() - start_time).seconds > 60: #exit program after 1min
                stream.stop_stream()
                stream.close()
                audio.terminate()
                print("* Exit program after 1min *")
                break
            
    except KeyboardInterrupt as kie:
        print("* Program terminated by user *")
        stream.stop_stream()
        stream.close()
        audio.terminate()
    except Exception as e:
        # print("Exception:", e)
        if stream!=None:
            stream.stop_stream()
            stream.close()
            audio.terminate()
    # sys.exit(0)
    # exit


    ```

3. Start the Kafka Consumer (stream_data_pipeline_2_consumer.ipynb) to receive text from this Kafka Producer.

4. What did you observe from the messages sent? Submit your findings.

# Conclusion

1. You have successfully streamed audio data from your device from a paragraph, directly and reliably transcribed the audio to text.

2. You have sent the audio data from one computer to another computer to be read, processed and inserted into a NoSQL database.

<b>Questions to ponder</b>
1. Which principle in the good data achitecture does Kafka fulfill?
2. Can Microsoft Power App exercises do stream processing?
3. What is the advantages and disadvantages of using stream processing?

# Submissions next Wed 9pm (29 Oct 2025)  

Submit your ipynb as a pdf. Save your ipynb as a html file, open in browser and print as a pdf. Include in your submission:

    Section 3.2

    Answer the questions to ponder.

    In total, 2 pdfs of your stream_data_pipeline_2_producer.ipynb and stream_data_pipeline_2_consumer.ipynb

    In lab10_2, section 4, step 8, a screen capture to show the data is inserted into Kiabana > Display.

~ The End ~
