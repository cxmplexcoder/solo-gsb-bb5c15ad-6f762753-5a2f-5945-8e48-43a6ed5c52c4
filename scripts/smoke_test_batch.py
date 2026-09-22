#!/usr/bin/env python3
"""acquire-batch 端到端冒烟测试（仅依赖 Python 标准库 + redis-cli）。

前置：redis 在 localhost:6379，API 在 BASE。
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
REDIS_CLI = sys.argv[2] if len(sys.argv) > 2 else "redis-cli"
PREFIX = "lease"
FAILED = []


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def redis(*args):
    out = subprocess.run(
        [REDIS_CLI, *args], capture_output=True, text=True, check=True
    ).stdout.strip()
    return None if out == "" else out


def lease_key(k):
    return f"{PREFIX}:lease:{k}"


def fence_key(k):
    return f"{PREFIX}:fencing:{k}"


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def uniq(tag):
    return f"batch-{tag}-{int(time.time() * 1000)}"


def main():
    tag = uniq("t")

    # ---- 1. 入参校验 ----
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [f"{tag}a"]})
    check("single key rejected (need >=2)", code == 422, str(code))

    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [f"{tag}dup", f"{tag}dup"]})
    check("duplicate keys rejected", code == 422, str(code))

    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [f"{tag}{i}" for i in range(33)]})
    check("33 keys rejected (need <=32)", code == 422, str(code))

    # ---- 2. 正常批量获取 ----
    keys = [f"{tag}:k1", f"{tag}:k2", f"{tag}:k3"]
    ttl = 5000
    t0 = time.time()
    code, body = call("POST", "/v1/leases/acquire-batch",
                      {"task_keys": keys, "ttl_ms": ttl})
    check("batch acquire 200", code == 200, f"{code} {body}")
    check("acquired=true", body.get("acquired") is True, str(body))
    leases = body.get("leases", [])
    check("3 leases returned", len(leases) == 3, str(body))
    check("order preserved",
          [l["task_key"] for l in leases] == keys, str(leases))
    tokens = [l["token"] for l in leases]
    check("tokens unique and non-empty",
          all(tokens) and len(set(tokens)) == 3, str(tokens))
    fences = [l["fencing_token"] for l in leases]
    check("fencing starts at 1 for fresh keys", fences == [1, 1, 1], str(fences))
    expirations = {l["expires_at_ms"] for l in leases}
    check("single base time -> identical expires_at_ms",
          len(expirations) == 1, str(expirations))
    expires_at = expirations.pop()
    expected = int(t0 * 1000) + ttl
    check("expires_at ~= now + ttl", abs(expires_at - expected) < 1000,
          f"{expires_at} vs {expected}")
    check("conflicts empty on success", body.get("conflicts") == [], str(body))

    # ---- 3. 凭据兼容现有接口 ----
    for l in leases:
        code, v = call("POST", "/v1/leases/validate",
                       {"task_key": l["task_key"], "token": l["token"],
                        "fencing_token": l["fencing_token"]})
        check(f"validate {l['task_key']}", code == 200 and v["valid"],
              f"{code} {v}")

    code, r = call("POST", "/v1/leases/renew",
                   {"task_key": keys[1], "token": leases[1]["token"],
                    "fencing_token": leases[1]["fencing_token"],
                    "ttl_ms": 8000})
    check("renew with batch credentials", code == 200 and r["renewed"],
          f"{code} {r}")

    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": keys[2], "token": leases[2]["token"],
                      "fencing_token": leases[2]["fencing_token"]})
    check("release with batch credentials", code == 200 and rel["released"],
          f"{code} {rel}")

    # ---- 4. 全成全败：冲突返回全部冲突键，且零副作用 ----
    held1, held2 = f"{tag}:held1", f"{tag}:held2"
    free1, free2 = f"{tag}:free1", f"{tag}:free2"
    code, h1 = call("POST", "/v1/leases/acquire",
                    {"task_key": held1, "ttl_ms": 30000})
    assert code == 200 and h1["acquired"]
    code, h2 = call("POST", "/v1/leases/acquire",
                    {"task_key": held2, "ttl_ms": 30000})
    assert code == 200 and h2["acquired"]

    before = {
        held1: redis("GET", fence_key(held1)),
        held2: redis("GET", fence_key(held2)),
        free1: redis("GET", fence_key(free1)),
        free2: redis("GET", fence_key(free2)),
    }
    batch_keys = [held1, free1, free2, held2]
    code, body = call("POST", "/v1/leases/acquire-batch",
                      {"task_keys": batch_keys, "ttl_ms": 5000})
    check("conflict -> 409", code == 409, f"{code} {body}")
    check("acquired=false", body.get("acquired") is False, str(body))
    check("all conflicting keys returned in input order",
          body.get("conflicts") == [held1, held2], str(body.get("conflicts")))
    check("no leases on conflict", body.get("leases") == [], str(body))

    # Redis 侧：空闲键既无租约 Hash 也无 fencing 计数器
    check("no lease hash created for free1",
          redis("EXISTS", lease_key(free1)) == "0")
    check("no lease hash created for free2",
          redis("EXISTS", lease_key(free2)) == "0")
    check("no fencing key created for free1",
          redis("EXISTS", fence_key(free1)) == "0")
    check("no fencing key created for free2",
          redis("EXISTS", fence_key(free2)) == "0")
    # 全部计数器值与冲突前完全一致（占用方未被 INCR，空闲方仍不存在）
    after = {
        held1: redis("GET", fence_key(held1)),
        held2: redis("GET", fence_key(held2)),
        free1: redis("GET", fence_key(free1)),
        free2: redis("GET", fence_key(free2)),
    }
    check("no fencing counter mutated on conflict", before == after,
          f"before={before} after={after}")
    # 被占用租约本身未被改动（token/fencing 不变）
    for held, h in ((held1, h1), (held2, h2)):
        data = redis("HGETALL", lease_key(held)).splitlines()
        fields = dict(zip(data[0::2], data[1::2]))
        check(f"existing lease {held} untouched",
              fields.get("token") == h["token"]
              and int(fields.get("fencing")) == h["fencing_token"],
              str(fields))

    # 失败的批处理未"消耗"序号：之后单独获取 free1 应拿到 fencing 1
    code, one = call("POST", "/v1/leases/acquire",
                     {"task_key": free1, "ttl_ms": 5000})
    check("failed batch consumed no fencing (free1 gets 1)",
          code == 200 and one["fencing_token"] == 1, f"{code} {one}")

    # ---- 5. 过期租约可被批量接管 ----
    exp_keys = [f"{tag}:exp1", f"{tag}:exp2"]
    prev_fences = []
    for k in exp_keys:
        code, old = call("POST", "/v1/leases/acquire",
                         {"task_key": k, "ttl_ms": 300})
        assert code == 200
        prev_fences.append(old["fencing_token"])
    fresh = f"{tag}:fresh"
    time.sleep(0.7)
    code, body = call("POST", "/v1/leases/acquire-batch",
                      {"task_keys": [*exp_keys, fresh], "ttl_ms": 5000})
    check("batch takes over expired leases", code == 200 and body["acquired"],
          f"{code} {body}")
    check("expired members not reported as conflicts",
          body.get("conflicts") == [], str(body))
    got = {l["task_key"]: l for l in body["leases"]}
    check("fencing increments on takeover",
          [got[k]["fencing_token"] for k in exp_keys]
          == [f + 1 for f in prev_fences],
          str(body))
    check("brand-new key starts at 1", got[fresh]["fencing_token"] == 1,
          str(body))

    # ---- 6. 混合场景：一个过期 + 一个仍占用 -> 409，仅报占用者 ----
    mix_exp, mix_held = f"{tag}:mixexp", f"{tag}:mixheld"
    call("POST", "/v1/leases/acquire", {"task_key": mix_exp, "ttl_ms": 300})
    mh = call("POST", "/v1/leases/acquire",
              {"task_key": mix_held, "ttl_ms": 30000})[1]
    time.sleep(0.7)
    code, body = call("POST", "/v1/leases/acquire-batch",
                      {"task_keys": [mix_exp, mix_held], "ttl_ms": 5000})
    check("mixed expiry/held -> 409", code == 409, f"{code} {body}")
    check("only held key conflicts", body.get("conflicts") == [mix_held],
          str(body.get("conflicts")))
    check("no leases created when mixed conflict",
          body.get("leases") == [], str(body))
    # 占用者租约未被动到
    fields = dict(zip(*[iter(redis("HGETALL", lease_key(mix_held)).splitlines())] * 2))
    check("held lease still owned by original holder",
          fields.get("token") == mh["token"], str(fields))

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All batch checks passed.")


if __name__ == "__main__":
    main()
