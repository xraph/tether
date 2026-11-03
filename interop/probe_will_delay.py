"""Probe how long a delayed will takes to arrive.

    python3 -I probe_will_delay.py <suite-checkout> <port> <session-expiry> <will-delay> [wait-seconds]

Client A connects with the given session expiry and will delay, client B
subscribes to the will topic, then A drops its socket. Prints how long the will
took, or that it never came. Against a cell, expiry <= delay loses the will
(see RESULTS.md); against a single node it arrives.
"""
import sys, time, logging
sys.path.insert(0, sys.argv[1] + "/interoperability")
import mqtt.clients.V5 as mc
import mqtt.formats.MQTTV5 as MQTTV5
logging.getLogger().setLevel(logging.CRITICAL)
port=int(sys.argv[2]); expiry=int(sys.argv[3]); delay=int(sys.argv[4]); wait=float(sys.argv[5]) if len(sys.argv)>5 else 15
class CB(mc.Callback):
    def __init__(s): s.messages=[]; s.subscribeds=[]; s.disconnects=[]
    def connectionLost(s,c): pass
    def publishArrived(s, topic, payload, qos, retained, msgid, properties=None):
        s.messages.append((topic,payload,time.time())); return True
    def subscribed(s,msgid,data): s.subscribeds.append(msgid)
    def published(s,m): pass
    def unsubscribed(s,m): pass
    def disconnected(s,rc,props): s.disconnects.append(rc)
cb=CB(); cb2=CB()
a=mc.Client(b"willa"); a.registerCallback(cb)
b=mc.Client(b"willb"); b.registerCallback(cb2)
wp=MQTTV5.Properties(MQTTV5.PacketTypes.WILLMESSAGE); wp.WillDelayInterval=delay
cp=MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT); cp.SessionExpiryInterval=expiry
a.connect(host="127.0.0.1",port=port,cleanstart=True,properties=cp,willProperties=wp,willFlag=True,willTopic="wp/t",willMessage=b"w")
b.connect(host="127.0.0.1",port=port,cleanstart=True)
b.subscribe(["wp/t"],[MQTTV5.SubscribeOptions(2)])
time.sleep(0.5)
a.terminate(); t0=time.time()
while not cb2.messages and time.time()-t0<wait: time.sleep(0.05)
print("expiry=%d delay=%d ->"%(expiry,delay), "will after %.2fs"%(cb2.messages[0][2]-t0) if cb2.messages else "NO WILL in %.0fs"%wait)
b.disconnect()
