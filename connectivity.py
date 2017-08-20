""" connectivity component Documentation

This component provides communication between things and other components.
for load balancing, only the messages get service that hash of their id`s is in range
(messages _id from the same agent is the same)
also for future purpose, insert message detail to database
and also to debugs, insert events and errors to corresponding collection of database

"""

import paho.mqtt.client as mqtt
import pymongo
# import hashlib
import LWhash
import json
import sys
from datetime import datetime
import logging
from log4mongo.handlers import MongoHandler

__author__ = "Mehran Hosseinzade"
__email__ = "M.hosseinzade@eng.ui.ac.ir"


class Connectivity(object):
    def __init__(self, broker_ip, min_hash, max_hash, mongodb_ip):
        """
        The constructor for Connectivity class:

        :param broker_ip: broker ip
        :param min_hash: minimum hash of message id that can take service from this instance object of class
        :param max_hash: maximum hash of message id that can take service from this instance object of class
        :param mongodb_ip: mongodb ip
        """
        self.Things_Broker_ip = broker_ip
        self.Components_Broker_ip = broker_ip
        self.MinHash = min_hash
        self.MaxHash = max_hash
        self.mongodb_ip = mongodb_ip
        self.ClientOfComponentsBroker = mqtt.Client()
        self.ClientOfThingsBroker = mqtt.Client()

    def run(self):

        try:

            # connect to mongodb and create collections

            self.ClientOfMongodb = pymongo.MongoClient(self.mongodb_ip, 27017)
            self.db = self.ClientOfMongodb.d2i1820
            if "connectivity_UserInfo"not in self.db.collection_names():
                self. db.create_collection('connectivity_UserInfo', capped=True, size=1000000)

            # config logger to insert events to mongodb

            if "connectivity_log" not in self.db.collection_names():
                self. db.create_collection('connectivity_log', capped=True, size=1000000)
            handler = MongoHandler(host=self.mongodb_ip, database_name='d2i1820', collection='connectivity_log')
            self.log = logging.getLogger('Connectivity_component')
            self.log.addHandler(handler)
            self.log.setLevel(logging.INFO)

        except Exception as e:
            print "Connectivity component Cannot connect to the MongoDB:", e

        try:
            # config ClientOfComponentsBroker

            self.ClientOfComponentsBroker.on_connect = self.on_connect_components_broker
            self.ClientOfComponentsBroker.on_message = self.on_message_components_broker
            self.ClientOfComponentsBroker.on_disconnect = self.on_disconnect_components_broker
            self.ClientOfComponentsBroker.on_log = self.on_log_components_broker
            self.ClientOfComponentsBroker.connect(self.Components_Broker_ip, 1883, 60)
            """ connect(host, port, keep_alive, bind_address="")
            keep_alive: maximum period in seconds allowed between communications with the broker,
            If no other messages are being exchanged, this controls the rate at which
            the client will send ping messages to the broker
            bind_address: the IP address of a local network interface to bind this client to that,
            assuming multiple interfaces exist """
            self.ClientOfComponentsBroker.loop_start()
            """ loop_start() implement a threaded interface to the network loop. 
            This call also handles reconnecting to the broker """
            # loop functions are Responsible for process incoming and outgoing network data
        except Exception as e:
            print "connectivity component Cannot connect to the ComponentsBroker:", e
            self.log.error("connectivity component Cannot connect to the ComponentsBroker:"+str(e))

        try:
            self.ClientOfThingsBroker.on_connect = self.on_connect_things_broker
            self.ClientOfThingsBroker.on_message = self.on_message_things_broker
            self.ClientOfThingsBroker.on_disconnect = self.on_disconnect_things_broker
            self.ClientOfThingsBroker.on_log = self.on_log_things_broker
            self.ClientOfThingsBroker.connect(self.Things_Broker_ip, 1883, 60)
            self.ClientOfThingsBroker.loop_forever()
            """ This is a blocking form of the network loop and will not return until the client calls disconnect(). 
            It automatically handles reconnecting."""

        except Exception as e:
            print "connectivity component Cannot connect to the ThingsBroker:", e
            self.log.error("connectivity component Cannot connect to the ThingsBroker:"+str(e))

    def hash_in_range(self, msg_id):

        # :param msg_id: getting message id
        # :return: return two value: 1- boolean (true if hash of id is in range), 2- hash value of id
        
        LW_hash = LWhash.LWhash(msg_id)
        if int(self.MinHash, 16) <= int(LW_hash, 16) <= int(self.MaxHash, 16):
            return True, LW_hash
        else:
            return False, LW_hash

    def on_connect_things_broker(self, client, userdata, flags, rc):
        """
        this function called when ClientOfThingsBroker connect

        :param client: the client instance for this callback
        :param userdata: the private user data as set in Client() or userdata_set(),
                (username and optionally a password for broker authentication)
        :param flags: response flags sent by the broker
                flags[session present] : clean session set to 0 , maintain session set to 1
        :param rc: the connection result
        # The value of rc indicates success or not:
        # 0: Connection successful
        # 1: Connection refused - incorrect protocol version
        # 2: Connection refused - invalid client identifier
        # 3: Connection refused - server unavailable
        # 4: Connection refused - bad username or password
        # 5: Connection refused - not authorised
        """
        self.log.info("Connected to ThingsBroker with result code:" + str(rc))
        client.subscribe("d2i1820/agent/stc/#")

    def on_disconnect_things_broker(self, client, userdata, rc):
        """
            this function called when ClientOfThingsBroker disconnect
        """
        self.log.error("disconnect things_broker with result code: " +str(rc))

    def on_message_things_broker(self, client, userdata, msg):
        """
        this function called when ClientOfThingsBroker receive a message

        :param client: the client instance for this callback
        :param userdata: the private user data as set in Client() or userdata_set()
        :param msg: the message that receive
        """
        topic = msg.topic
        payload = msg.payload
        payload_json_format = json.loads(payload)
        if '_id' in payload_json_format:
            msg_id = payload_json_format["_id"]
            hash_in_range, hash_value = self.hash_in_range(msg_id)
            if hash_in_range:
                self.update_db(hash_value, msg_id)  # insert detail to database
                try:
                    topic_to_components = topic.rsplit('d2i1820/agent/')[1]  # stc/#
                    self.ClientOfComponentsBroker.publish(topic_to_components, payload=payload, qos=0, retain=False)
                    """qos: the quality of service level to use

                    retain:
                    if a publisher publishes a message to a topic with retain=False,
                    and no one is subscribed to that topic the message is simply discarded by the broker
                    However the publisher can tell the broker to keep the last message on that topic by
                    setting the retained message flag.(retain=True)
                    """
                except Exception as e:
                    self.log.error("error occurs in publishing to ComponentsBroker:" + str(e))
                    pass
        else:
            self.log.error("message from things_broker with topic ("+str(topic)+") is not including (_id)")

    def on_log_things_broker(self, client, userdata, level, buf):
        """
        Called when the client has log information

        :param client: the client instance for this callback
        :param userdata: the private user data as set in Client() or userdata_set()
        :param level: The level variable gives the severity of the message and will be one of
                MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, and MQTT_LOG_DEBUG.
        :param buf: message
        """
        self.log.info("on_log_things_broker: level:"+str(level)+" message:"+buf)

    def on_connect_components_broker(self, client, userdata, flags, rc):
        self.log.info("Connected to ComponentsBroker with result code: " + str(rc))
        client.subscribe("stt/#")

    def on_disconnect_components_broker(self, client, userdata, rc):
        self.log.error("disconnect components_broker with result code: " + str(rc))

    def on_message_components_broker(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload
        payload_json_format = json.loads(payload)
        if '_id' in payload_json_format:
            msg_id = payload_json_format["_id"]
            hash_in_range, hash_value = self.hash_in_range(msg_id)
            if hash_in_range:
                self.update_db(hash_value, msg_id)
                try:
                    topic_to_things = 'd2i1820/agent/'+topic  # d2i1820/agent/stt/#
                    self.ClientOfThingsBroker.publish(topic_to_things, payload=payload, qos=0, retain=False)
                except Exception as e:
                    self.log.error("error occurs in publishing to ClientOfThingsBroker:"+str(e))
                    pass
        else:
            self.log.error("message from components_broker with topic ("+str(topic)+") is not including (_id)")

    def on_log_components_broker(self, client, userdata, level, buf):
        self.log.info("on_log_Components_broker: level:"+str(level)+" message:"+buf)

    def update_db(self, hash_value, msg_id):
        try:
            self.db.connectivity_UserInfo.find_one_and_update({'_id': msg_id},
                                                 {'$set': {'HashValue': hash_value, 'Last Seen': datetime.now()}},
                                                 upsert=True)
            # The upsert option can be used to insert a new document if a matching document does not exist.
            # upsert (optional): If True, perform an insert if no documents match the filter.
        except Exception as e:
            print "db_error:", e
            pass


def main(argv):
    Connectivity(argv[0], argv[1], argv[2], argv[3]).run()
    # Connectivity(broker_ip, min_hash, max_hash, mongodb_ip).run()

if __name__ == '__main__':

    main(sys.argv[1:])
""" terminal command to run:
    python connectivity.py broker_ip 0x0 0xffffffffff mongodb_ip
"""
# things must publish message to components with topic:d2i1820/agent/stc/#
# and also receives message with subscribe  to "d2i1820/agent/stt/#"

# components must publish message to things with topic:stt/# and also receives message with subscribe  to "stc/#"

