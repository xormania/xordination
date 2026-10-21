"""Immutable scripted native wire fixtures; these do not invoke vendor models."""

MOCK_CODEX = r'''
import json,sys
def write(row):
    print(json.dumps(row),flush=True)
for line in sys.stdin:
    row=json.loads(line)
    method=row.get("method")
    if method=="initialize":
        write({"id":row["id"],"result":{}})
    elif method=="thread/start":
        write({"id":row["id"],"result":{"thread":{"id":"thread-one"},"params":row["params"]}})
    elif method=="turn/start":
        if row["params"].get("threadId")!="thread-one":
            write({"id":row["id"],"error":{"code":-32602,"message":"Fixture rejected mismatched native thread"}})
            continue
        turn="turn-busy" if row["params"].get("input") else "turn-delivery"
        if row["params"].get("input"):
            write({"method":"turn/started","params":{"threadId":"thread-one","turn":{"id":turn}}})
            write({"id":"held-tool","method":"item/tool/call","params":{"threadId":"thread-one","turnId":turn,"callId":"held","tool":"xordination_wait","arguments":{"timeout":5}}})
        write({"id":row["id"],"result":{"turn":{"id":turn},"params":row["params"]}})
    elif method=="fixture/late-completion":
        write({"method":"turn/completed","params":{"threadId":"thread-one","turn":{"id":"turn-busy"}}})
        write({"id":row["id"],"result":{}})
    elif method=="fixture/crash":
        sys.exit(3)
    elif method and "id" in row:
        write({"id":row["id"],"result":{}})
'''


MOCK_GROK = r'''
import json,sys
held=None
def write(row):
    print(json.dumps({"jsonrpc":"2.0",**row}),flush=True)
for line in sys.stdin:
    row=json.loads(line)
    method=row.get("method")
    if method in ("session/prompt","_x.ai/interject") and row["params"].get("sessionId")!="grok-one":
        write({"id":row["id"],"error":{"code":-32602,"message":"Fixture rejected mismatched native session"}})
        continue
    if method=="initialize":
        write({"id":row["id"],"result":{"authMethods":[]}})
    elif method=="session/new":
        write({"id":row["id"],"result":{"sessionId":"grok-one"}})
    elif method=="session/prompt":
        held=row
        write({"method":"session/update","params":{"sessionId":"grok-one","update":{"sessionUpdate":"agent_message_chunk","prompt_id":row["params"]["_meta"]["promptId"],"content":{"text":""}}}})
    elif method=="_x.ai/interject":
        write({"id":row["id"],"result":{"accepted":True}})
    elif method=="fixture/complete":
        write({"id":held["id"],"result":{"stopReason":"end_turn"}})
        write({"id":row["id"],"result":{}})
    elif method=="fixture/crash":
        sys.exit(4)
    elif method:
        write({"id":row["id"],"result":{}})
'''


MOCK_CODEX_COMPLETED_BEFORE_RESPONSE = r'''
import json,sys
def write(row):
    print(json.dumps(row),flush=True)
for line in sys.stdin:
    row=json.loads(line)
    if row.get("method")=="turn/start":
        if row["params"].get("threadId")!="thread-one":
            write({"id":row["id"],"error":{"code":-32602,"message":"Fixture rejected mismatched native thread"}})
            continue
        write({"method":"turn/started","params":{"threadId":"thread-one","turn":{"id":"finished-turn"}}})
        write({"method":"turn/completed","params":{"threadId":"thread-one","turn":{"id":"finished-turn"}}})
        write({"id":row["id"],"result":{"turn":{"id":"finished-turn"}}})
'''



# A native whose detached grandchild holds an exclusive lock on argv[1] until
# it exits; "stubborn" makes the grandchild ignore SIGTERM.
FAMILY_NATIVE = r'''
import json,os,subprocess,sys
HOLDER="""
import fcntl,os,signal,sys,time
if sys.argv[2]=="stubborn":
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
lock=open(sys.argv[1],"a")
fcntl.flock(lock,fcntl.LOCK_EX)
print("held",flush=True)
os.close(1)
time.sleep(60)
"""
LAUNCHER="import subprocess,sys; subprocess.Popen([sys.executable,'-c',*sys.argv[1:]])"
launcher=subprocess.Popen([sys.executable,"-c",LAUNCHER,HOLDER,*sys.argv[1:3]],
                          stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
held=launcher.stdout.readline().decode().strip()
launcher.stdout.close()
launcher.wait()
for line in sys.stdin:
    row=json.loads(line)
    if row.get("method")=="fixture/crash":
        sys.exit(5)
    print(json.dumps({"id":row["id"],"result":{"held":held}}),flush=True)
'''
