# Lab 10.2 Stream Data Pipeline II - Consumer

- Scenario: Streaming audio \
    Stream in audio, process, calling a machine learning classification model, save the data and visualize for reporting.

Create a new jupyter notebook file "stream_data_pipeline_2_consumer.ipynb".


```python
import os
home_directory = os.path.expanduser("~")
os.chdir(home_directory+'/Documents/projects/ee3801')
```

# 1. Load whisper


```python
# !pip3 install -U jupyter
# !pip3 install -U ipywidgets
# !pip3 install openai-whisper

## python 3.11.5
# !pip3 install -U torch
# !pip3 uninstall numpy -y
# !pip3 install numpy==1.26.4
# !pip3 install kafka-python
## restart kernel

```


```python
import whisper
model = whisper.load_model("medium.en") 
```


```python
# !python -m pip install pandas
# !python -m pip install -U scikit-learn
# !python -m pip install nltk
# !python -m pip install matplotlib
# !python -m pip install sentence_transformers

```

# 2. Consume audio stream and transcribe

# 2.1 Initialise Consumer


```python
import pyaudio

FORMAT = pyaudio.paInt16
CHUNK = 1024
RECORD_SECONDS = 10
# DEVICE_ID = 4

audio = pyaudio.PyAudio()
input = audio.get_default_input_device_info()
RATE = int(input['defaultSampleRate'])
CHANNELS = int(input['maxInputChannels'])

```

1. Replace the ```<ip_address>``` with your AWS EC2 instance public ip address.

2. The codes below is the Kafka Consumer that will listen for data from the Kafka Producer in the topic ```dataengineering```.


```python
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

        if message.key.decode('utf-8')=="audio":
            audio_data = np.frombuffer(message.value, dtype=np.int16).flatten().astype(np.float32) / 32768.0
            audio_data = whisper.pad_or_trim(audio_data)
            before_transcribe_time = datetime.now()
            sample_rate = int(len(audio_data)*16000/RATE)
            audio_data = resample(audio_data,num = sample_rate)
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            compiled_message.append(text)
            print("transcribed.message=%s, transcribed.duration=%s" % (text, 
                                                                       str(datetime.now()-before_transcribe_time)))

        if (datetime.now()-start_time).seconds > 60: # listen for 1 minute (60 seconds)
            consumer.close()
            print("compiled.message=%s" % (' '.join(compiled_message)))
            print("* Ended listening for 1 min *")
            break
except KeyboardInterrupt as kie:
    consumer.close()
    print("compiled.message=%s" % (' '.join(compiled_message)))
    print("* Program terminated by user *")


    
```

# 3. Consume audio stream, transcribe and identify number of speakers

# 3.1 Detect speakers

Here we call classification model to identify speakers in the audio.

1. Follow the steps in the website to install pyannote.audio, https://github.com/pyannote/pyannote-audio. 

HfHubHTTPError: 401 Client Error: Unauthorized for url: https://huggingface.co/pyannote/speaker-diarization-3.0. Go to the url link, sign up in huggingface and request for access. Then click on Agree and access repository.

2. Install the softwares to detect speakers.

3. Replace the ```<huggingface_token_for_pyannote_audio>``` with your huggingface token retrieved for pyannote.audio.

4. Before you run the codes below, play a youtube video that has two person speaking. Run lab10_1, section 3.2 codes. Then run the codes below. You should be able to see the transcribed text and different speakers detected.



```python
# !pip3 install --upgrade pip
# !pip3 install torch torchvision torchaudio

# # for mac silicon
# !pip3 install --pre torch torchvision torchaudio  --extra-index-url https://download.pytorch.org/whl/nightly/cpu

# !pip3 install -U pyannote.audio

```


```python
### diarization - https://github.com/pyannote/pyannote-audio/tree/develop?tab=readme-ov-file
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
        use_auth_token="<huggingface_token_for_pyannote_audio>")

    # send pipeline to GPU (when available)
    pipeline.to(torch.device("mps")) #cpu

    this_waveform = torch.from_numpy(np.array([audio_data_])).float().to(device=torch.device('mps')) #cpu

    # call the model to detect speakers in the audio data
    diarization = pipeline({"waveform": this_waveform, "sample_rate": 16000})

    # print the result
    for turn, _, speaker in diarization.itertracks(yield_label=True):
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

# To consume latest messages and auto-commit offsets. Receive data from topic 'dataengineering'
consumer = KafkaConsumer('dataengineering',
                        #  group_id='python-consumer',
                         bootstrap_servers=[public_ip_address+':29092',public_ip_address+':39092',public_ip_address+':49092'])
                        #  consumer_timeout_ms=1000)
                         #value_deserializer=lambda m: json.loads(m.decode('utf-8')))

start_time = datetime.now()

try: 
    for message in consumer:
        begin_time = datetime.now()
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s %s:%d:%d: key=%s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), message.topic, message.partition,
                                            message.offset, message.key.decode('utf-8')))

        if message.key.decode('utf-8')=="text":
            print("message=%s" % message.value.decode('utf-8'))

        if message.key.decode('utf-8')=="audio":
            audio_data = np.frombuffer(message.value, dtype=np.int16).flatten().astype(np.float32) / 32768.0
            audio_data = whisper.pad_or_trim(audio_data)
            before_transcribe_time = datetime.now()
            
            sample_rate = int(len(audio_data)*16000/RATE)
            audio_data = resample(audio_data,num = sample_rate)
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            speech_list.append(text)
            time_speech_list.append(datetime.now())
            print("transcribed.message=%s, transcribed.duration=%s" % (text, 
                                                                       str(datetime.now()-before_transcribe_time)))
            # detect speakers
            waveform_list.append(audio_data)
            dia = detect_speakers(audio_data)
            dia_list.append(dia)
            print("Number of speakers detected:",len(list(dict.fromkeys(speaker_list))))


        if (datetime.now()-start_time).seconds > 60: # listen for 1 minute (60 seconds)
            consumer.close()
            print("* Ended listening for 1 min *")
            break
except KeyboardInterrupt as kie:
    consumer.close()
    print("* Program terminated by user *")

    
        
```

4. Plot the data to visualise the interaction. 


```python
import matplotlib.pyplot as plt
from pyannote.core import Segment
import matplotlib.cm as cm

# Assuming dia_list is a list of pyannote Annotation objects
# and speech_list is a list of corresponding audio segment descriptions

fig, ax = plt.subplots(figsize=(12, 4))

cumulative_offset = 0.0
color_map = plt.get_cmap('tab10')  # To get distinct colors for speakers
speaker_colors = {}

speech_detect_json = []

for idx, diarization in enumerate(dia_list):
    print(speech_list[idx])

    for segment, track, speaker in diarization.itertracks(yield_label=True):
        # Offset segment start and end by cumulative_offset
        start = segment.start + cumulative_offset
        end = segment.end + cumulative_offset

        # Assign a consistent color to each speaker
        if speaker not in speaker_colors:
            speaker_colors[speaker] = color_map(len(speaker_colors) % 10)

        ax.hlines(y=speaker, xmin=start, xmax=end, linewidth=5,
                  color=speaker_colors[speaker])

        print(f"Speaker {speaker}: Start={round(start, 2)}, End={round(end, 2)}")
        speech_detect_json.append({'speaker':speaker,'start':round(start, 2), 'end':round(end, 2)})

    # Update the offset for next audio clip
    cumulative_offset = end  # end of the last segment in the current diarization

ax.set_xlabel("Time (s)")
ax.set_ylabel("Speaker")
ax.set_title("Speaker Diarization Timeline")
plt.tight_layout()
plt.show()

```

5. Below displayed the text and plots of all sections in the video that is transcribed and play the audio. This is for your to verify the audio captured.


```python
import matplotlib.pyplot as plt
import sounddevice as sd

DEVICE_ID = 2
audio = pyaudio.PyAudio()
RATE = int(audio.get_device_info_by_index(DEVICE_ID)['defaultSampleRate'])
CHANNELS = int(audio.get_device_info_by_index(DEVICE_ID)['maxInputChannels'])
sample_rate = int(len(audio_data)*16000/RATE)
audio.terminate()

i=0
for wave in waveform_list:
    plt.plot(np.array(wave))
    plt.show()
    print(speech_list[i])
    print(dia_list[i])

    sd.play(wave, 16000)
    sd.wait()
    i+=1

```

# 4. Insert data into Elasticsearch NoSQL database and visualize in Kibana.

1. Go to the server and start elasticsearch and kibana docker containers. 

    ```docker start dev_es01```\
    ```docker start dev_kib01```

2. Install elasticsearch in python


```python
# !pip3 install elasticsearch
```


```python
# check the data
speech_detect_json
```

3. SSH into server.

    ```ssh -i "MyKeyPair.pem" ec2-user@<ip_address>```

4. Still in the server, download the http_ca.crt from dev_es01 into ~/elasticsearch.

    ```docker cp dev_es01:/usr/share/elasticsearch/config/certs/http_ca.crt .```

5. Exit the server. 

6. In your local machine, SCP http_ca.crt into your local machine.

    ```scp -i "MyKeyPair.pem" ec2-user@<ip_address>:./elasticsearch.http_ca.crt .```

7. Copy the cert to the appropriate location in your machine.

    In mac, ```cp ./http_ca.crt /etc/ssl/certs/```\
    In Windows, ```cp ./http_ca.crt C:\\.certs```




```python
from elasticsearch import Elasticsearch

def insertElasticsearch(dia_json,index):

    es = Elasticsearch({'https://'+public_ip_address+':9200'}, basic_auth=("elastic", "<huggingface_token_for_pyannote_audio>"), verify_certs=False) 

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

8. Screen capture to show that data is shown in Kibana > Display. Save the screen capture and submit.

9. As an optional challenge, implement a real-time system to detect different speakers that reflects the changes in a dasboard in Kibana. Submit a short 10 seconds video that demonstrates this. (Optional)

~ The End ~
