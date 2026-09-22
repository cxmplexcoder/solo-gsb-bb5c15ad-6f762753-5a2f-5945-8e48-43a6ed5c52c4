"""租约状态机的全部写/判操作都在 Redis Lua 中原子完成。

约定：
- 租约键（KEYS[1] 或 lease key）是一个 Hash：
    token       所有权令牌（UUID）
    fencing     防栅序号（单调递增）
    expires_at  过期时间，Unix 毫秒，基于 Redis 服务器时钟
- 防栅计数器键独立存放、永不过期，保证同一任务键的 fencing 只增不减。
- 过期判断一律使用 redis.call('TIME')，即 Redis 服务器时间，
  避免各客户端时钟不一致导致的问题。
"""

# 申请租约：仅当租约不存在或已过期时成功。
# KEYS[1] = 租约键, KEYS[2] = 防栅计数器键
# ARGV[1] = 新令牌, ARGV[2] = ttl_ms
# 返回 {是否成功(1/0), fencing, expires_at}
ACQUIRE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ttl = tonumber(ARGV[2])

local expires_at = redis.call('HGET', KEYS[1], 'expires_at')
if expires_at and tonumber(expires_at) > now then
  return {0, tonumber(redis.call('HGET', KEYS[1], 'fencing')), tonumber(expires_at)}
end

local fencing = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'token', ARGV[1],
  'fencing', fencing,
  'expires_at', now + ttl)
redis.call('PEXPIRE', KEYS[1], ttl)
return {1, fencing, now + ttl}
"""

# 续租：必须同时匹配令牌与防栅序号，且租约尚未过期。
# 已过期的租约不允许续租（持有者必须重新申请），
# 防止旧持有者在超时后"复活"自己的租约。
# KEYS[1] = 租约键
# ARGV[1] = 令牌, ARGV[2] = fencing, ARGV[3] = ttl_ms
# 返回 {是否成功(1/0), 原因, 新 expires_at}
RENEW_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return {0, 'not_found', 0}
end
if token ~= ARGV[1] or redis.call('HGET', KEYS[1], 'fencing') ~= ARGV[2] then
  return {0, 'conflict', 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  return {0, 'expired', 0}
end

local ttl = tonumber(ARGV[3])
local new_expires = now + ttl
redis.call('HSET', KEYS[1], 'expires_at', new_expires)
redis.call('PEXPIRE', KEYS[1], ttl)
return {1, 'ok', new_expires}
"""

# 释放租约：令牌与防栅序号同时匹配才删除，否则静默忽略。
# 旧持有者超时后的释放请求不会误删新持有者的租约。
# KEYS[1] = 租约键
# ARGV[1] = 令牌, ARGV[2] = fencing
# 返回 1（已释放）或 0（无操作）
RELEASE_LUA = """
local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return 0
end
if token ~= ARGV[1] or redis.call('HGET', KEYS[1], 'fencing') ~= ARGV[2] then
  return 0
end
redis.call('DEL', KEYS[1])
return 1
"""

# 校验：判断 (令牌, 防栅序号) 是否仍对应当前有效租约。
# KEYS[1] = 租约键
# ARGV[1] = 令牌, ARGV[2] = fencing
# 返回 {是否有效(1/0), 剩余毫秒}
VALIDATE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return {0, 0}
end
if token ~= ARGV[1] or redis.call('HGET', KEYS[1], 'fencing') ~= ARGV[2] then
  return {0, 0}
end

local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at'))
if expires_at <= now then
  return {0, 0}
end
return {1, expires_at - now}
"""
