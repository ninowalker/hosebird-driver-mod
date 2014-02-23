import vertx
import os
from java.lang import System

def start_server(cfg):
    m = {}
    m.update(web_server_conf)
    m.update(cfg)
    # Start the web server, with the config we defined above
    vertx.deploy_module('io.vertx~mod-web-server~2.0.0-final', m)


# Configuration for the web server
web_server_conf = {

    # Normal web server stuff
    'port': 8080,
    'host': 'localhost',
    'ssl': False,

    # Configuration for the event bus client side bridge
    # This bridges messages from the client side to the server side event bus
    'bridge': True,

    # This defines which messages from the client we will let through
    # to the server side
    'inbound_permitted':  [
        # Allow calls to login
        {
            'address': 'vertx.basicauthmanager.login'
        },
        # Allow calls to get static album data from the persistor
        {
            'address': 'vertx.mongopersistor',
            'match': {
                'action': 'find',
                'collection': 'albums'
            }
        },
        # And to place orders
        {
            'address': 'vertx.mongopersistor',
            'requires_auth': True,  # User must be logged in to send let these through
            'match': {
                'action': 'save',
                'collection': 'orders'
            }
        }
    ],

    # This defines which messages from the server we will let through to the client
    'outbound_permitted': [
        {
            'match': 'test.*'
        }
    ]
}