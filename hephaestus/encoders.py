import json


class SQSMessageEncoder(json.JSONEncoder):
    def default(self, o):
        data = o.meta.data
        data['QueueUrl'] = o.queue_url
        return json.dumps(data)
