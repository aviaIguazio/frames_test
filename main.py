# Copyright 2017 The Nuclio Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import local_v3io_frames as v3f
import pandas as pd


def init_context(context):
    # Create the DB connection under "context.user_data"
    setattr(context.user_data, 'client',
            v3f.Client("framesd:8081", container="users", token="816efaa0-ca93-43a0-a41f-bfbc8c8489e5"))


def convert_to_df(json):
    metrics = [json["metric"]]
    print(type(json["samples"]))
    times = json["samples"][0]['t']
    con_tme = pd.to_datetime(int(times), unit='ms').to_pydatetime()
    df = pd.DataFrame(data=[json["samples"][0]['v']['n']], index=[con_tme], columns=metrics)
    df.index.names = ["time"]
    return df


# Example tsdb event:
#
# {
# 		"metric": "cpu",
# 		"labels": {
# 			"dc": "7",
# 			"hostname": "mybesthost"
# 		},
# 		"samples": [
# 			{
# 				"t": "1532595945142",
# 				"v": {
# 					"N": 95.2
# 				}
# 			},
# 			{
# 				"t": "1532595948517",
# 				"v": {
# 					"n": 86.8
# 				}
# 			}
# 		]
# }


# transform kafka event to tsdb event
def transform_to_tsdb_event(event):
    # implement the transformation
    return event


def handler(context, event):
    context.logger.debug_with('*** Starting handling', pid=event.shard_id)

    # parse the given event body
    event_body = json.loads(event.body)

    event_body[0]['worker_id'] = context.worker_id

    context.logger.info(str(event_body))

    tsdbdf = convert_to_df(event_body[1])

    context.user_data.client.write(backend='tsdb', table="oded", dfs=tsdbdf, labels=event_body[1]["labels"])

    context.logger.debug_with('*** End handling', pid=event.shard_id)
