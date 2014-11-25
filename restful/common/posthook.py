#!/usr/bin/env python2
from audi.cdap import ClientRestClient


client = ClientRestClient()


apps = [
    'PurchaseHistory',
    'HelloWorld',
    'WordCount'
]

# stop flows
for app_id in apps:
    flows = client.flows(app_id).json()
    for flow_id in flows:
        client.stop_element(app_id, 'flows', flow_id)

# stop procedures
for app_id in apps:
    procedures = client.procedures(app_id).json()
    for procedure_id in procedures:
        client.stop_element(app_id, 'procedures', procedure_id)

# delete apps
for app_id in apps:
    client.delete_app(app_id)
