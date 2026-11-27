"""봇 사건의 상세 API와 STR 초안에 같은 Spark 재계산 근거가 들어 있는지 확인한다.

    기본은 L2·L6·L7 중 한 사건을 확인한다. DB는 조회만 한다.
    --rules L2 L6 L7: 세 룰을 모두 확인한다.
    --since <시간대가 포함된 ISO 시각>: 해당 시각 이후 생성된 사건만 확인한다.
"""
import argparse
from datetime import datetime
import json
import os
import re
import time
import urllib.request
from pymongo import MongoClient


def verify_case(case, get):
    base = 'http://127.0.0.1:8000/api/v1/admin/cases/' + str(case['_id'])
    detail = get(base)
    draft = get(base + '/str-draft')['str_report']
    rc = detail.get('recompute')
    if not rc or rc != draft.get('spark_recompute'):
        raise ValueError('Detail / STR recompute differ')
    if rc.get('status') != 'MATCH':
        raise ValueError('Recompute status is not MATCH')
    if '4-1. Spark' not in draft.get('text', ''):
        raise ValueError('STR text lacks recompute section')
    return {'case_id': str(case['_id']), 'rule_id': case['rule_id'],
            'wallet': case['violator_address'], 'recompute_status': rc['status'],
            'detail_str_equal': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wallet-prefix', required=True)
    parser.add_argument('--timeout', type=int, default=180)
    parser.add_argument('--rules', nargs='+', choices=['L2', 'L6', 'L7'])
    parser.add_argument('--since', help='Require detected_at at or after this timezone-aware ISO timestamp')
    args = parser.parse_args()
    rules = set(args.rules or ['L2', 'L6', 'L7'])
    query = {'rule_id': {'$in': sorted(rules)},
             'violator_address': {'$regex': '^' + re.escape(args.wallet_prefix)},
             'recompute': {'$exists': True}}
    if args.since:
        since = datetime.fromisoformat(args.since.replace('Z', '+00:00'))
        if since.tzinfo is None:
            parser.error('--since must include timezone')
        query['detected_at'] = {'$gte': since}
    client = MongoClient(os.environ['MONGO_URI'], tz_aware=True, serverSelectionTimeoutMS=5000)
    db = client[os.getenv('MONGO_DB', 'aegis')]

    def get(url):
        request = urllib.request.Request(url, headers={'X-Admin-Key': os.environ['ADMIN_API_KEY']})
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)

    deadline = time.monotonic() + args.timeout
    verified, errors = {}, {}
    try:
        while True:
            for rule in rules - verified.keys():
                case = db.fds_cases.find_one({**query, 'rule_id': rule}, sort=[('detected_at', -1)])
                if not case:
                    errors[rule] = 'No matching fresh case with recompute'
                    continue
                try:
                    verified[rule] = verify_case(case, get)
                    errors.pop(rule, None)
                    print(json.dumps(verified[rule], ensure_ascii=False), flush=True)
                except (ValueError, KeyError, OSError) as exc:
                    errors[rule] = type(exc).__name__
            if (args.rules and rules <= verified.keys()) or (not args.rules and verified):
                print(json.dumps({'result': 'PASS', 'verified_rules': sorted(verified),
                                  'since': args.since}), flush=True)
                return
            if time.monotonic() >= deadline:
                raise SystemExit('FAIL: ' + json.dumps(errors))
            time.sleep(3)
    finally:
        client.close()


if __name__ == '__main__':
    main()
