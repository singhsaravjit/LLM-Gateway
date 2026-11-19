-- Token bucket, executed atomically inside Redis.
-- KEYS[1] = bucket key (hash with fields tokens, ts)
-- ARGV[1] = refill rate (tokens/second), ARGV[2] = burst (capacity), ARGV[3] = cost
-- Returns {allowed (0|1), remaining tokens, retry-after seconds} — numbers as
-- strings because Redis truncates Lua floats to integers.
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

-- Redis clock, not the caller's, so skew between gateway nodes doesn't matter.
local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

tokens = math.min(burst, tokens + math.max(0, now - ts) * rate)

local allowed, retry = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
elseif rate > 0 then
  retry = (cost - tokens) / rate
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
-- Expire once the bucket would be full again: idle keys clean themselves up.
local ttl_ms = 60000
if rate > 0 then ttl_ms = math.ceil(burst / rate * 1000) + 1000 end
redis.call('PEXPIRE', KEYS[1], ttl_ms)

return {tostring(allowed), tostring(tokens), tostring(retry)}
