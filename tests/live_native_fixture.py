"""Finite native wire fixture for acceptance orchestration, with no vendor calls."""

CODEX_ACCEPTANCE = r'''
import json,sys
native_id, mode = sys.argv[1:3]
sequence = 0
pending = {}
held = False
queued = []
current_turn = None

def write(packet):
    print(json.dumps(packet), flush=True)

def notify(method, **params):
    write({'method':method, 'params':{'threadId':native_id, **params}})

def tool(name, arguments, continuation=None):
    global sequence
    sequence += 1
    identifier = native_id + ':call-' + str(sequence)
    pending[identifier] = continuation
    write({'id':identifier, 'method':'item/tool/call', 'params':{
        'threadId':native_id, 'turnId':'fixture-turn-' + str(sequence),
        'callId':identifier, 'tool':name, 'arguments':arguments}})

def acknowledge(message, continuation=None):
    tool('xordination_ack', {'message_id':message['id']}, continuation)

def handle(message):
    global held
    try:
        body = json.loads(message['body'])
    except (ValueError, TypeError):
        body = None
    if isinstance(body, dict) and body.get('phase') == 'busy_prime':
        acknowledge(message)
        if mode != 'no-gate':
            held = True
            task = body['tasks'][0]
            tool(task['tool'], task['arguments'], 'release')
    elif isinstance(body, dict) and isinstance(body.get('tasks'), list):
        acknowledge(message)
        for task in body['tasks']:
            tool(task['tool'], task['arguments'])
    elif isinstance(body, dict) and 'reply' in body:
        if held:
            queued.append(message)
        else:
            reply = {'to':message['from'], 'body':body['reply'],
                     'key':'fixture-reply-' + body['case_id'], 'reply_to':message['id']}
            acknowledge(message)
            tool('xordination_send', reply)
    elif message['body'].startswith('Xordination acceptance reply '):
        acknowledge(message, ('complete', message['body']))
    else:
        raise ValueError('Unexpected fixture message intent')

for line in sys.stdin:
    row = json.loads(line)
    method = row.get('method')
    if method == 'turn/start':
        if row['params'].get('threadId') != native_id:
            write({'id':row['id'], 'error':{'code':-32602, 'message':'wrong native session'}})
            continue
        sequence += 1
        turn = 'fixture-turn-' + str(sequence)
        current_turn = turn
        notify('turn/started', turn={'id':turn})
        write({'id':row['id'], 'result':{'turn':{'id':turn}}})
        if row['params'].get('toolOutput'):
            output = row['params']['toolOutput']['output']
            message = json.loads(output.split('\n', 1)[1])
            handle(message)
        else:
            notify('turn/completed', turn={'id':turn})
            notify('fixture/idle')
    elif row.get('id') in pending:
        continuation = pending.pop(row['id'])
        if 'error' in row or row.get('result', {}).get('success') is not True:
            raise ValueError('Fixture native communication tool failed')
        if continuation == 'release':
            held = False
            notify('fixture/released')
            waiting, queued = queued, []
            for message in waiting:
                handle(message)
        elif isinstance(continuation, tuple) and continuation[0] == 'complete':
            notify('fixture/roundtrip-complete', body=continuation[1])
        if not pending and not held:
            notify('turn/completed', turn={'id':current_turn})
    elif method and 'id' in row:
        write({'id':row['id'], 'result':{}})
'''
