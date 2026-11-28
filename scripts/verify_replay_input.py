"""Replay an AWS-shaped fixture through the actual replay loop, with mocked HTTP."""
import hashlib
import importlib.util
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('replay', ROOT/'bots/replay_golden.py')
replay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replay)

with tempfile.TemporaryDirectory() as directory:
    fixture = Path(directory)/'aws-recording.jsonl'
    rows = [dict(tx_hash='original', log_index=i, **{'from':'0xREAL','to':'0xDEST'},
        amount=amount, block=123, collected_at='2026-11-24T08:18:26Z', removed=removed)
        for i, amount, removed in [(0,0,False),(1,200,True),(2,40000,False),(3,60000,False)]]
    fixture.write_text(''.join(json.dumps(r)+'\n' for r in rows),encoding='utf-8')
    argv = ['replay_golden.py','--input',str(fixture),'--round-prefix','BACKUP_VERIFY']
    with patch.object(sys,'argv',argv+['--inspect-only']), patch.object(replay.requests,'Session') as session:
        replay.main()
        session.assert_not_called()
    with patch.object(sys,'argv',argv), patch.object(replay.requests,'Session') as factory, \
         patch.object(replay,'get_gateway_key',return_value='test-only'), patch.object(replay.time,'sleep'):
        factory.return_value.post.return_value = SimpleNamespace(status_code=200)
        replay.main()
        calls = factory.return_value.post.call_args_list
        assert len(calls)==2
        for call,row in zip(calls,rows[2:]):
            body = call.kwargs['json']
            salt = f"BACKUP_VERIFY_{row['tx_hash']}_{row['log_index']}"
            assert body['tx_hash']=='0x'+hashlib.sha256(salt.encode()).hexdigest()
            assert body['amount']==row['amount']
            assert abs((datetime.now(timezone.utc)-replay.parse_iso(body['timestamp'])).total_seconds())<10
print('PASS custom AWS input, zero/removed filtering, current timestamp, round-salted hashes; mocked HTTP only')
