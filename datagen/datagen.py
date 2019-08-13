#!/usr/bin/python
#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import os
import random

from faker import Faker
from google.cloud import bigquery
from google.cloud import pubsub_v1

# key variables
total_msgs = 100
total_time_interval_in_seconds = 10
max_dup_count = 3
rand_threshold = 0.3
epoch = datetime.datetime.utcfromtimestamp(0)

# Cloud PubSub Python client docs:
# https://google-cloud-python.readthedocs.io/en/stable/pubsub/publisher/api/client.html


def populate_msgs():
    message_bank = []
    curr_num = 1
    msg_count = 0
    run_time_raw = datetime.datetime.utcnow()
    run_time = run_time_raw.strftime("%Y-%m-%d %H:%M:%S")
    fake = Faker()
    run_name = fake.word() + run_time_raw.strftime("%Y%m%d%H%M%S")
    event_time = run_time_raw + datetime.timedelta(seconds=1)
    time_gap = total_time_interval_in_seconds/total_msgs

    while True:
        if msg_count >= total_msgs:
            break

        payload = {'Val': curr_num,
                   'GenId': msg_count+1,
                   'RunTimestamp': run_time,
                   'RunName': run_name}
        message_bank.append(payload)
        msg_count += 1

        # generate duplicates
        dup_count = 0
        while random.random() <= rand_threshold:
            if msg_count >= total_msgs:
                break

            payload = {'Val': curr_num,
                       'GenId': msg_count + 1,
                       'RunTimestamp': run_time,
                       'RunName': run_name}
            message_bank.append(payload)
            msg_count += 1

            dup_count += 1
            if dup_count >= max_dup_count:
                break

        curr_num += 1

    random.shuffle(message_bank)

    for msg in message_bank:
        event_time = event_time + datetime.timedelta(seconds=time_gap)
        msg['EventTime'] = event_time

    return message_bank


def get_millis_since_epoch(dt):
    return round((dt - epoch).total_seconds() * 1000.0)


def pubsub_gen(msgs):
    print("Publishing...")

    tick = datetime.datetime.utcnow()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        os.environ['PROJECT'],
        os.environ['PUBSUB_TOPIC'])

    fs = []
    for msg in msgs:
        # EventTime needs to be deserializable in Java
        event_time = msg['EventTime']
        msg['EventTime'] = event_time.strftime("%Y-%m-%d %H:%M:%S")
        event_time_str = str(get_millis_since_epoch(event_time))
        # Setup logicalId
        logical_id = msg['RunName'] + '-' + str(msg['Val'])
        msg['LogicalId'] = logical_id
        json_payload = json.dumps(msg).encode('utf-8')
        future = publisher.publish(topic_path,
                                   json_payload,
                                   logical_id=logical_id,
                                   event_time=event_time_str)
        fs.append(future)

    for f in fs:
        f.result()
    # concurrent.futures.wait(fs)

    tock = datetime.datetime.utcnow()
    diff = tock-tick

    max_num = max(msgs, key=lambda x: x['Val'])
    print("Max num: {}".format(max_num['Val']))
    print("Run name: {}".format(msgs[0]['RunName']))
    print("Duration: {}s".format(diff.total_seconds()))
    print("Done...")


def write_to_bq(msgs):
    print("Writing to BQ...")

    client = bigquery.Client()
    dataset_id = os.environ['DATASET']
    table_id = 'DedupDataGen'
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    rows_to_insert = []

    for msg in msgs:
        row = (msg['RunName'],
               msg['RunTimestamp'],
               msg['EventTime'],
               msg['GenId'],
               msg['Val'])
        rows_to_insert.append(row)

    errors = client.insert_rows(table, rows_to_insert)
    if errors:
        print(errors)


msgs = populate_msgs()
pubsub_gen(msgs)
write_to_bq(msgs)