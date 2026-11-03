"""A TCP relay that logs every MQTT packet and can spread connections.

    python3 -I spread_proxy.py <listen-port> <target-port>[,<target-port>...] <log-file>

With several targets, connection N goes to target N mod len(targets), so the
suite's clients land on different nodes of a cell. Each log line is
"<epoch> c<conn> <C>S|S>C> <PACKET> flags=<hex> len=<n> <hex bytes>".
"""
import socket, sys, threading, time
LISTEN=int(sys.argv[1]); TARGETS=[int(x) for x in sys.argv[2].split(',')]; OUT=open(sys.argv[3],'a',buffering=1)
NAMES=["","CONNECT","CONNACK","PUBLISH","PUBACK","PUBREC","PUBREL","PUBCOMP","SUBSCRIBE","SUBACK","UNSUBSCRIBE","UNSUBACK","PINGREQ","PINGRESP","DISCONNECT","AUTH"]
lock=threading.Lock(); conn_id=[0]
def log(cid,d,buf):
    # split into packets
    i=0
    while i<len(buf):
        t=buf[i]>>4; j=i+1; mult=1; rl=0
        while j<len(buf):
            b=buf[j]; rl+=(b&127)*mult; mult*=128; j+=1
            if not b&128: break
        pkt=buf[i:j+rl]
        with lock: OUT.write("%.3f c%d %s %s flags=%x len=%d %s\n"%(time.time(),cid,d,NAMES[t],buf[i]&15,rl,pkt.hex()))
        i=j+rl
def pipe(a,b,cid,d):
    pend=b""
    try:
        while True:
            data=a.recv(65536)
            if not data: break
            b.sendall(data)
            pend+=data
            # emit complete packets
            i=0
            while True:
                if len(pend)-i<2: break
                j=i+1; mult=1; rl=0; ok=False
                while j<len(pend):
                    x=pend[j]; rl+=(x&127)*mult; mult*=128; j+=1
                    if not x&128: ok=True; break
                if not ok or len(pend)<j+rl: break
                log(cid,d,pend[i:j+rl]); i=j+rl
            pend=pend[i:]
    except Exception as e:
        pass
    finally:
        with lock: OUT.write("%.3f c%d %s CLOSE\n"%(time.time(),cid,d))
        for s in (a,b):
            try: s.shutdown(socket.SHUT_RDWR)
            except: pass
srv=socket.socket(); srv.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); srv.bind(("127.0.0.1",LISTEN)); srv.listen(64)
while True:
    c,_=srv.accept(); conn_id[0]+=1; cid=conn_id[0]
    u=socket.create_connection(("127.0.0.1",TARGETS[(cid-1)%len(TARGETS)]))
    threading.Thread(target=pipe,args=(c,u,cid,"C>S"),daemon=True).start()
    threading.Thread(target=pipe,args=(u,c,cid,"S>C"),daemon=True).start()
