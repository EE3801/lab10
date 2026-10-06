# Lab 10.2 Stream Data Pipeline II - Consumer

- Scenario: Streaming audio\
  Stream audio, process it with a machine learning model, save the data, and visualize it for reporting.

Note: When copying the codes to your notebook, select all and ```Shift+Tab``` to remove leading spaces.

Create a new Jupyter notebook file named `stream_data_pipeline_2_consumer.ipynb`.

```python
import os
home_directory = os.path.expanduser("~")
os.chdir(os.path.join(home_directory, 'Documents', 'projects', 'ee3801'))
```

# 1. Load Whisper

On the local machine, install and load the Whisper model.

```python
# !python3 -m pip install kafka-python
## Restart the kernel after installation.
```

```python
import whisper
# if you have limited storage use `tiny.en`
model = whisper.load_model("medium.en")
```

<!-- ```python
# !python -m pip install pandas
# !python -m pip install -U scikit-learn
# !python -m pip install nltk
# !python -m pip install matplotlib
# !python -m pip install sentence_transformers
``` -->

# 2. Consume audio stream and transcribe

## 2.1 Initialize the consumer

The consumer listens for audio messages on the `dataengineering` topic, transcribes them with Whisper, and prints the results.

## Using PyAudio

```python
import pyaudio

FORMAT = pyaudio.paInt16
CHUNK = 1024
RECORD_SECONDS = 10
# DEVICE_ID = 4

audio = pyaudio.PyAudio()
input_device = audio.get_default_input_device_info()
RATE = int(input_device['defaultSampleRate'])
CHANNELS = int(input_device['maxInputChannels'])
```

1. Replace `<ip_address>` with your AWS EC2 public IP address.

2. Use the following code to create the Kafka consumer:

```python
# kafka-python Consumer
from kafka import KafkaConsumer
import json
import numpy as np
from datetime import datetime
import sys
from scipy.signal import resample

public_ip_address = "<ip_address>"

consumer = KafkaConsumer(
    'dataengineering',
    # group_id='python-consumer',
    bootstrap_servers=[
        public_ip_address + ':29092',
        public_ip_address + ':39092',
        public_ip_address + ':49092'
    ]
    # consumer_timeout_ms=1000,
    # value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

start_time = datetime.now()
compiled_message = []

try:
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s %s:%d:%d: key=%s" % (
            datetime.now().strftime("%d/%m/%Y, %H:%M:%S"),
            message.topic,
            message.partition,
            message.offset,
            message.key.decode('utf-8')
        ))

        if message.key.decode('utf-8') == "text":
            print("message=%s" % message.value.decode('utf-8'))

        if message.key.decode('utf-8') == "audio":
            audio_data = np.frombuffer(message.value, dtype=np.int16).flatten().astype(np.float32)
            if CHANNELS > 1:
                audio_data = audio_data.reshape((-1, CHANNELS))
                audio_data = audio_data.mean(axis=1)
            audio_data = audio_data / 32768.0
            audio_data = whisper.pad_or_trim(audio_data)
            before_transcribe_time = datetime.now()
            sample_rate = int(len(audio_data) * 16000 / RATE)
            audio_data = resample(audio_data, num=sample_rate)
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            compiled_message.append(text)
            print("transcribed.message=%s, transcribed.duration=%s" % (
                text,
                str(datetime.now() - before_transcribe_time)
            ))

        # listen for 1 min
        if (datetime.now() - start_time).seconds > 60:
            print("***********")
            print("* Ended listening for 1 min *")
            print("compiled.message=%s" % (' '.join(compiled_message)))
            break
except KeyboardInterrupt:
    print("***********")
    print("* Program terminated by user *")
    print("compiled.message=%s" % (' '.join(compiled_message)))
finally:
    consumer.close()
    
```

## Using sounddevice

3. Replace ```<ip_address>``` with your AWS EC2 public IP address.

4. Use the following code to create the Kafka consumer:

```python
import sounddevice as sd

FORMAT = sd.default.dtype[0]
RECORD_SECONDS = 5

input_device = sd.query_devices(kind='input')
RATE = int(input_device['default_samplerate'])
CHUNK = int(RATE * RECORD_SECONDS)
CHANNELS = int(input_device['max_input_channels'])
INDEX = int(input_device['index'])

# kafka-python Consumer
from kafka import KafkaConsumer
import json
import numpy as np
from datetime import datetime
import sys
from scipy.signal import resample

public_ip_address = "<ip_address>"

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('dataengineering',
                        #  group_id='python-consumer',
                         bootstrap_servers=[public_ip_address+':29092',public_ip_address+':39092',public_ip_address+':49092'])
                        #  consumer_timeout_ms=1000)
                         #value_deserializer=lambda m: json.loads(m.decode('utf-8')))

start_time = datetime.now()

compiled_message = []

try: 
    for message in consumer:
        begin_time = datetime.now()
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s %s:%d:%d: key=%s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), message.topic, message.partition,
                                            message.offset, message.key.decode('utf-8')))

        if message.key.decode('utf-8')=="text":
            print("message=%s" % message.value.decode('utf-8'))

        if message.key.decode('utf-8').startswith("audio"):
            audio_data = np.frombuffer(message.value, dtype=np.float32).flatten().astype(np.float32) 
            #  # Convert multi-channel (Stereo) to Mono
            # if CHANNELS > 1:
            #     audio_data = audio_data.mean(axis=1)
            # else:
            #     audio_data = audio_data.flatten()
            # # Ensure the data type is float32 (sounddevice usually returns float32 by default)
            # audio_data = audio_data.astype(np.float32)

            before_transcribe_time = datetime.now()
            target_len = int(len(audio_data) * 16000 / RATE) # Calculate how many total samples the audio array needs to be when changed to 16,000Hz
            audio_data = resample(audio_data, num=target_len) # Downsample the audio array to 16,000Hz (Whisper models are specifically trained on 16kHz audio)
            audio_data = whisper.pad_or_trim(audio_data) # Enforce Whisper's strict input rule: pad short audio or cut long audio to exactly 30 seconds
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            compiled_message.append(text)
            print("transcribed.message=%s, transcribed.duration=%s" % (text, 
                                                                       str(datetime.now()-before_transcribe_time)))

        if (datetime.now()-start_time).seconds > 60: # listen for 1 minute (60 seconds)
            consumer.close()
            print("**********")
            print("* Ended listening for 1 min *")
            print("compiled.message=%s" % (' '.join(compiled_message)))
            break
except KeyboardInterrupt as kie:
    consumer.close()
    print("**********")
    print("* Program terminated by user *")
    print("compiled.message=%s" % (' '.join(compiled_message)))

```

# 3. Transcribe audio and identify speakers

## 3.1 Detect speakers

This section uses speaker diarization to identify who is speaking.

1. Install `pyannote.audio` by following the instructions at https://github.com/pyannote/pyannote-audio.
2. If you see a Hugging Face authorization error, sign in to Hugging Face, request access to `pyannote/speaker-diarization-3.0`, and accept it.
3. Replace `<huggingface_token_for_pyannote_audio>` with your Hugging Face token.
4. Before running the code below, play a recording with multiple speakers and run the producer code in Lab 10.1 section 3.2.
5. You can use your own audio or this youtube link on <a href="https://youtu.be/JOh-9iaPcGU?si=LDDKfy4bzLVxLaRH">No.1 Performance Psychologist: The Secret to High Performance and Excellence</a>. 

```python
# !python3 -m pip install --upgrade pip
# !python3 -m pip install torch torchvision torchaudio

# For macOS Apple Silicon:
# !python3 -m pip install --pre torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/nightly/cpu

# !python3 -m pip install -U pyannote.audio
```
### Using PyAudio

```python
# diarization - https://github.com/pyannote/pyannote-audio/tree/develop?tab=readme-ov-file
from pyannote.audio import Pipeline
import torchaudio
import torch
import pyaudio
from scipy.signal import resample

# DEVICE_ID = 4
audio = pyaudio.PyAudio()
input = audio.get_default_input_device_info()
RATE = int(input['defaultSampleRate'])
CHANNELS = int(input['maxInputChannels'])
audio.terminate()

speaker_list = []
waveform_list = []
speech_list = []
time_speech_list = []
dia_list = []

def detect_speakers(audio_data_):
    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        token="<huggingface_token_for_pyannote_audio>"
    )

    pipeline.to(torch.device("mps"))  # Use CPU if MPS is unavailable.
    this_waveform = torch.from_numpy(np.array([audio_data_])).float().to(device=torch.device('mps'))

    # call the model to detect speakers in the audio
    diarization = pipeline({"waveform": this_waveform, "sample_rate": 16000})

    for turn, speaker in diarization.speaker_diarization:
        print(f"start={turn.start:.1f}s stop={turn.end:.1f}s speaker_{speaker}")
        speaker_list.append(speaker)

    return diarization

from kafka import KafkaConsumer
from datetime import datetime

consumer = KafkaConsumer(
    'dataengineering',
    bootstrap_servers=[
        public_ip_address + ':29092',
        public_ip_address + ':39092',
        public_ip_address + ':49092'
    ]
)

start_time = datetime.now()

try:
    for message in consumer:
        print("%s %s:%d:%d: key=%s" % (
            datetime.now().strftime("%d/%m/%Y, %H:%M:%S"),
            message.topic,
            message.partition,
            message.offset,
            message.key.decode('utf-8')
        ))

        if message.key.decode('utf-8') == "text":
            print("message=%s" % message.value.decode('utf-8'))

        if message.key.decode('utf-8') == "audio":
            audio_data = np.frombuffer(message.value, dtype=np.int16).flatten().astype(np.float32)
            if CHANNELS > 1:
                audio_data = audio_data.reshape((-1, CHANNELS))
                audio_data = audio_data.mean(axis=1)
            audio_data = audio_data / 32768.0
            audio_data = whisper.pad_or_trim(audio_data)
            before_transcribe_time = datetime.now()
            sample_rate = int(len(audio_data) * 16000 / RATE)
            audio_data = resample(audio_data, num=sample_rate)
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            speech_list.append(text)
            time_speech_list.append(datetime.now())
            print("transcribed.message=%s, transcribed.duration=%s" % (
                text,
                str(datetime.now() - before_transcribe_time)
            ))

            waveform_list.append(audio_data)
            dia = detect_speakers(audio_data)
            dia_list.append(dia)
            print("Number of speakers detected:", len(list(dict.fromkeys(speaker_list))))

        if (datetime.now() - start_time).seconds > 60:
            print("* Ended listening for 1 min *")
            break
except KeyboardInterrupt:
    print("* Program terminated by user *")
finally:
    consumer.close()
    
```

### Using sounddevice
```python
### diarization - https://github.com/pyannote/pyannote-audio/tree/develop?tab=readme-ov-file
from pyannote.audio import Pipeline
import torchaudio
import torch
import sounddevice as sd
from scipy.signal import resample

FORMAT = sd.default.dtype[0]
RECORD_SECONDS = 5

input_device = sd.query_devices(kind='input')
RATE = int(input_device['default_samplerate'])
CHUNK = int(RATE * RECORD_SECONDS)
CHANNELS = int(input_device['max_input_channels'])
INDEX = int(input_device['index'])

speaker_list = []
waveform_list = []
speech_list = []
time_speech_list = []
dia_list = []

def detect_speakers(audio_data_):
    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        token="<huggingface_token_for_pyannote_audio>")

    # send pipeline to GPU (when available)
    pipeline.to(torch.device("cpu")) #cpu

    this_waveform = torch.from_numpy(np.array([audio_data_])).float().to(device=torch.device('cpu')) #cpu

    # call the model to detect speakers in the audio data
    diarization = pipeline({"waveform": this_waveform, "sample_rate": 16000})

    # print the result
    # for turn, _, speaker in diarization.itertracks(yield_label=True):
    for turn, speaker in diarization.speaker_diarization:
        start=f"{turn.start:.1f}"
        end=f"{turn.end:.1f}"
        print(f"start={turn.start:.1f}s stop={turn.end:.1f}s speaker_{speaker}")
        speaker_list.append(speaker)
    
    return diarization

# kafka-python Consumer
from kafka import KafkaConsumer
import json
import numpy as np
from datetime import datetime
import sys
from scipy.signal import resample

public_ip_address = "<ip_address>"

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('dataengineering',
                        #  group_id='python-consumer',
                        bootstrap_servers=[public_ip_address+':29092',public_ip_address+':39092',public_ip_address+':49092'])
                        #  consumer_timeout_ms=1000)
                        #value_deserializer=lambda m: json.loads(m.decode('utf-8')))

start_time = datetime.now()

compiled_message = []

try: 
    for message in consumer:
        begin_time = datetime.now()
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s %s:%d:%d: key=%s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), message.topic, message.partition,
                                            message.offset, message.key.decode('utf-8')))

        if message.key.decode('utf-8')=="text":
            print("message=%s" % message.value.decode('utf-8'))

        if message.key.decode('utf-8').startswith("audio"):
            audio_data = np.frombuffer(message.value, dtype=np.float32).flatten().astype(np.float32) 
            #  # Convert multi-channel (Stereo) to Mono
            # if CHANNELS > 1:
            #     audio_data = audio_data.mean(axis=1)
            # else:
            #     audio_data = audio_data.flatten()
            # # Ensure the data type is float32 (sounddevice usually returns float32 by default)
            # audio_data = audio_data.astype(np.float32)

            before_transcribe_time = datetime.now()
            target_len = int(len(audio_data) * 16000 / RATE) # Calculate how many total samples the audio array needs to be when changed to 16,000Hz
            audio_data = resample(audio_data, num=target_len) # Downsample the audio array to 16,000Hz (Whisper models are specifically trained on 16kHz audio)
            audio_data = whisper.pad_or_trim(audio_data) # Enforce Whisper's strict input rule: pad short audio or cut long audio to exactly 30 seconds
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            compiled_message.append(text)
            speech_list.append(text)
            print("transcribed.message=%s, transcribed.duration=%s" % (text, 
                                                                    str(datetime.now()-before_transcribe_time)))

            # detect speakers
            waveform_list.append(audio_data)
            dia = detect_speakers(audio_data)
            dia_list.append(dia)
            print("Number of speakers detected:",len(list(dict.fromkeys(speaker_list))))

        if (datetime.now()-start_time).seconds > 60: # listen for 1 minute (60 seconds)
            consumer.close()
            print("**********")
            print("* Ended listening for 1 min *")
            print("compiled.message=%s" % (' '.join(compiled_message)))
            break
except KeyboardInterrupt as kie:
    consumer.close()
    print("**********")
    print("* Program terminated by user *")
    print("compiled.message=%s" % (' '.join(compiled_message)))
```

## 3.2 Plot speaker activity

Plot the diarization results to visualize speaker turns.

```python
import matplotlib.pyplot as plt
import matplotlib.cm as cm

fig, ax = plt.subplots(figsize=(12, 4))

cumulative_offset = 0.0
color_map = plt.get_cmap('tab10')
speaker_colors = {}
speech_detect_json = []

for idx, diarization in enumerate(dia_list):
    print(speech_list[idx])

    for turn, speaker in diarization.speaker_diarization:
        start = turn.start + cumulative_offset
        end = turn.end + cumulative_offset

        if speaker not in speaker_colors:
            speaker_colors[speaker] = color_map(len(speaker_colors) % 10)

        ax.hlines(y=speaker, xmin=start, xmax=end, linewidth=5,
                  color=speaker_colors[speaker])

        print(f"Speaker {speaker}: Start={round(start, 2)}, End={round(end, 2)}")
        speech_detect_json.append({
            'speaker': speaker,
            'start': round(start, 2),
            'end': round(end, 2)
        })

    cumulative_offset = end

ax.set_xlabel("Time (s)")
ax.set_ylabel("Speaker")
ax.set_title("Speaker Diarization Timeline")
plt.tight_layout()
plt.show()
```

## 3.3 Review transcriptions and audio

Display the transcription, plot the waveforms, and play the audio.

### Using pyaudio

```python
import matplotlib.pyplot as plt
import numpy as np
from IPython.display import Audio, display  # Added native notebook audio controls

import pyaudio
audio = pyaudio.PyAudio()
output_device = audio.get_default_output_device_info()
RATE = int(output_device['defaultSampleRate'])
CHANNELS = int(output_device['maxInputChannels'])
audio.terminate()

# Loop through your speech segments
for i, wave in enumerate(waveform_list):
    # Convert data safely to a numpy array
    wave_array = np.array(wave)
    
    # 1. Plot the waveform
    plt.plot(wave_array)
    plt.show()
    
    # 2. Print metadata text
    print(speech_list[i])
    print(dia_list[i])

    # 3. Generate and display the HTML5 play button widget
    audio_player = Audio(wave_array.flatten(), rate=16000)
    display(audio_player)
```
### Using sounddevice

```python
import matplotlib.pyplot as plt
import sounddevice as sd

device_info = sd.query_devices(kind='output')
RATE = int(device_info['default_samplerate'])
CHANNELS = int(device_info['max_input_channels'])
audio.terminate()

for i, wave in enumerate(waveform_list):
    plt.plot(np.array(wave))
    plt.show()
    print(speech_list[i])
    print(dia_list[i])

    sd.play(wave, 16000)
    sd.wait()
```

# 4. Insert data into Elasticsearch NoSQL database and visualize in Kibana.

1. SSH into EC2 instance and start elasticsearch and kibana docker containers. 

    ```bash
    ssh -i "MyKeyPair.pem" ec2-user@<ip_address>
    # stop kafka as the EC2 instance has limited resources
    docker stop $(docker ps -aq -f "name=kafka")
    # start elasticsearch
    docker start dev_es01
    # start kibana
    docker start dev_kib01
    ```

2. Install elasticsearch in python


    ```python
    !python3 -m pip install elasticsearch
    ```


    ```python
    # check the data
    speech_detect_json
    ```

    ```python
    # Sample data for those who are unable to run the diarization codes.
    speech_detect_json = [{'speaker': 'SPEAKER_00', 'start': 4.27, 'end': 5.3},
     {'speaker': 'SPEAKER_00', 'start': 5.52, 'end': 7.37},
     {'speaker': 'SPEAKER_00', 'start': 8.47, 'end': 9.97},
     {'speaker': 'SPEAKER_00', 'start': 10.0, 'end': 12.25},
     {'speaker': 'SPEAKER_00', 'start': 13.63, 'end': 17.88},
     {'speaker': 'SPEAKER_01', 'start': 17.88, 'end': 19.94},
     {'speaker': 'SPEAKER_01', 'start': 19.97, 'end': 20.38},
     {'speaker': 'SPEAKER_00', 'start': 20.38, 'end': 20.55},
     {'speaker': 'SPEAKER_01', 'start': 20.55, 'end': 22.0},
     {'speaker': 'SPEAKER_00', 'start': 22.0, 'end': 22.23},
     {'speaker': 'SPEAKER_01', 'start': 22.23, 'end': 22.27},
     {'speaker': 'SPEAKER_00', 'start': 22.27, 'end': 22.32},
     {'speaker': 'SPEAKER_01', 'start': 22.32, 'end': 22.38},
     {'speaker': 'SPEAKER_00', 'start': 22.38, 'end': 25.66},
     {'speaker': 'SPEAKER_01', 'start': 25.66, 'end': 26.64},
     {'speaker': 'SPEAKER_00', 'start': 26.57, 'end': 29.91},
     {'speaker': 'SPEAKER_00', 'start': 29.94, 'end': 34.79},
     {'speaker': 'SPEAKER_00', 'start': 37.99, 'end': 39.88},
     {'speaker': 'SPEAKER_00', 'start': 39.91, 'end': 49.85},
     {'speaker': 'SPEAKER_00', 'start': 49.88, 'end': 59.82}]
    ```

<!-- 3. Still in the EC2 instance, download the http_ca.crt from dev_es01 into ~/elasticsearch.

    ```bash
    docker cp dev_es01:/usr/share/elasticsearch/config/certs/http_ca.crt .
    ``` -->

3. In your local machine terminal or command line, copy the file `http_ca.crt` into your local machine.

    ```bash
    scp -i "MyKeyPair.pem" ec2-user@<ip_address>:./elasticsearch/http_ca.crt .
    ```

4. Copy the cert to the appropriate location in your machine.

    ```bash
    # In macOS
    mv ./http_ca.crt /etc/ssl/certs/
    # In Windows
    cp ./http_ca.crt C:\\.certs
    ```

    ```python
    from elasticsearch import Elasticsearch

    def insertElasticsearch(dia_json,index):

        es = Elasticsearch({'https://'+public_ip_address+':9200'}, basic_auth=("elastic", "<elasticsearch password>"), verify_certs=False) 

        doc=json.dumps(dia_json, indent=4)
        res=es.index(index="speech_detection_duration",
                    # doc_type="doc",
                    id=index,
                    document=doc) # replaced body with document
        print(res)

    i=0
    for speech in speech_detect_json:
        insertElasticsearch(speech,i)
        i+=1
    ```

5. Screen capture to show that data is shown in Kibana > Dashboard. Save the screen capture and submit.

6. As an optional challenge, implement a real-time system to detect different speakers that reflects the changes in a dasboard in Kibana. Submit a short 10 seconds video that demonstrates this. (Optional)

~ The End ~
