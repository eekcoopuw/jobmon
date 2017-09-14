import json


def mogrify(topic, msg):
    """
    json encode the message and prepend the topic.
    see: https://stackoverflow.com/questions/25188792/ \
         how-can-i-use-send-json-with-pyzmq-pub-sub
    """
    return str(topic) + ' ' + json.dumps(msg)


def demogrify(topicmsg):
    """ Inverse of mogrify() """
    json0 = topicmsg.find('{')
    topic = topicmsg[0:json0].strip()
    msg = json.loads(topicmsg[json0:])
    return topic, msg
