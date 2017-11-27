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
    topic, msg = topicmsg.split(" ", 1)
    jsonmsg = json.loads(msg)
    return topic, jsonmsg
