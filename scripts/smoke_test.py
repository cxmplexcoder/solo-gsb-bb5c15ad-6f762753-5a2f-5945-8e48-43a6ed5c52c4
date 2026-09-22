#!/usr/bin/env python3
"""端到端冒烟测试（仅依赖 Python 标准库）。

先启动服务（docker compose up --build），然后执行：
    python3 scripts/smoke_test.py [base_url]

覆盖场景：获取 / 冲突 / 校验 / 续租 / 释放 / 防栅序号单调递增 /
旧持有者在超时后无法影响新租约。
"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
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


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def main():
    key = f"smoke:{int(time.time() * 1000)}"

    code, body = call("GET", "/healthz")
    check("healthz", code == 200 and body.get("status") == "ok", f"{code} {body}")

    # 1. 申请租约
    code, a = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire #1", code == 200 and a["acquired"] and a["token"], f"{code} {a}")
    token_a, fence_a = a["token"], a["fencing_token"]

    # 2. 占用期间再次申请 -> 409
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire conflict -> 409", code == 409 and not b["acquired"], f"{code} {b}")

    # 3. 校验当前持有者
    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("validate holder", code == 200 and v["valid"] and v["remaining_ms"] > 0,
          f"{code} {v}")

    # 4. 错误令牌续租 -> 409
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": "wrong", "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew with wrong token -> 409", code == 409 and not r["renewed"],
          f"{code} {r}")

    # 5. 正确续租
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_a, "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew ok", code == 200 and r["renewed"], f"{code} {r}")

    # 6. 错误防栅序号释放 -> 幂等忽略
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a + 99},
    )
    check("release with wrong fencing ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    # 7. 正确释放
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("release ok", code == 200 and rel["released"], f"{code} {rel}")

    # 8. 重新申请，防栅序号必须单调递增
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 800})
    check("re-acquire after release", code == 200 and b["acquired"], f"{code} {b}")
    check("fencing monotonic", b["fencing_token"] > fence_a,
          f"{b['fencing_token']} <= {fence_a}")
    token_b, fence_b = b["token"], b["fencing_token"]

    # 9. 等租约过期，新持有者可接管；旧持有者此后无法干扰
    time.sleep(1.0)
    code, c = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 5000})
    check("acquire after expiry", code == 200 and c["acquired"], f"{code} {c}")
    token_c, fence_c = c["token"], c["fencing_token"]

    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_b, "fencing_token": fence_b, "ttl_ms": 5000},
    )
    check("stale holder renew rejected", code == 409 and not r["renewed"],
          f"{code} {r}")

    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_b, "fencing_token": fence_b},
    )
    check("stale holder release ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_c, "fencing_token": fence_c},
    )
    check("new lease unaffected", code == 200 and v["valid"], f"{code} {v}")

    # 10. 状态查询
    code, st = call("GET", f"/v1/leases/{key}")
    check("inspect", code == 200 and st["held"] and st["fencing_token"] == fence_c,
          f"{code} {st}")

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
