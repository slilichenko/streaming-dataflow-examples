#  Copyright 2021 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from google.cloud import pubsub_v1
import sys

def process(input, topic):
  publisher = pubsub_v1.PublisherClient()
  with open(input) as json_payloads:
    count = 0
    for line in json_payloads:
      future = publisher.publish(topic, bytes(line, 'utf-8'))
      future.result()
      count += 1
      if count % 1000 == 0:
        print(f"Sent {count} messages")
    print(f"Sent {count} messages")

if __name__ == "__main__":
  process(sys.argv[1], sys.argv[2])
