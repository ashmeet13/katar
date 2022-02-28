import traceback

from flask import Flask, request

from katar.engine.metadata import Metadata
from katar.engine.topic import Topic
from katar.logger import logger
from katar.server.topicmanager import TopicManager

app = Flask(__name__)
topicmanager = TopicManager()


@app.route("/topic", methods=["GET"])
def topics():
    try:
        topics = topicmanager.all_topics
        return {"topics": topics}
    except Exception as e:
        logger.error(event="Error", traceback=traceback.format_exc())


@app.route("/topic/<topic>", methods=["GET"])
def single_topic(topic):
    try:
        topic_metadata = topicmanager.get_topic_metadata(topicname=topic)
        return {"topic": topic, "metadata": topic_metadata}
    except Exception as e:
        logger.error(event="Error", traceback=traceback.format_exc())


@app.route("/topic/<topic>", methods=["POST"])
def create_topic(topic):
    topicname = topicmanager.create_topicname(topic)
    try:
        metadata_config = request.json
        topicmanager.create(topicname=topicname, metadata_config=metadata_config)
    except:
        logger.error(event="Error", traceback=traceback.format_exc())

    return {"topic": topicname, "created": True}


@app.route("/topic/<topic>", methods=["PUT"])
def reset_topic_metadata(topic):
    try:
        metadata_config = request.json
        if topic not in topicmanager.all_topics:
            return "Not Okay"

        topicmanager.reset_metadata(topic, metadata_config)
    except:
        logger.error(event="Error", traceback=traceback.format_exc())
    return {}


@app.route("/topic/<topic>", methods=["PATCH"])
def update_topic_metadata(topic):
    try:
        metadata_config = request.json
        if topic not in topicmanager.all_topics:
            return "Not Okay"

        topicmanager.update_metadata(topic, metadata_config)
    except:
        logger.error(event="Error", traceback=traceback.format_exc())
    return {}


@app.route("/publish/<topic>", methods=["POST"])
def publish(topic):
    payload = request.json
    if payload is None:
        return {"success": False}
    try:
        topicmanager.publish_data(topic, payload)
    except:
        logger.error(event="Error", traceback=traceback.format_exc())
    return {"success": True}


@app.route("/subscribe/<topic>", methods=["GET"])
def subscribe(topic):
    request_json = request.json
    req_offset = None
    if request_json is not None:
        req_offset = request_json["offset"]
    try:
        offset, log = topicmanager.read_data(topic, req_offset)
    except:
        logger.error(event="Error", traceback=traceback.format_exc())
    return {"topic": topic, "offset": offset, "log": log}
