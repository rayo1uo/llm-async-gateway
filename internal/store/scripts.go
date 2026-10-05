package store

import "github.com/redis/go-redis/v9"

var releaseLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// KEYS: unit, queue, outstanding, deadline hash, tier hash
// ARGV: unit JSON, score, id, tier, incrOutstanding ("1"/"0")
var enqueueScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
redis.call('HSET', KEYS[4], ARGV[3], ARGV[2])
redis.call('HSET', KEYS[5], ARGV[3], ARGV[4])
if ARGV[5] == '1' then
  redis.call('INCR', KEYS[3])
end
return 1
`)

// KEYS: queue, claimed
// ARGV: leaseUntil ms, owner
var claimScript = redis.NewScript(`
local ids = redis.call('ZRANGE', KEYS[1], 0, 0)
if #ids == 0 then
  return ''
end
local id = ids[1]
local removed = redis.call('ZREM', KEYS[1], id)
if removed == 0 then
  return ''
end
redis.call('ZADD', KEYS[2], ARGV[1], id .. '|' .. ARGV[2])
return id
`)

// KEYS: claimed
// ARGV: id, owner, leaseUntil ms
var extendScript = redis.NewScript(`
local member = ARGV[1] .. '|' .. ARGV[2]
local score = redis.call('ZSCORE', KEYS[1], member)
if not score then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[3], member)
return 1
`)

// KEYS: claimed, unit, retry
// ARGV: id, owner, unit JSON, retryAt ms
var parkScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1] .. '|' .. ARGV[2])
redis.call('SET', KEYS[2], ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
return 1
`)

// KEYS: claimed, expired list
// ARGV: now ms, result prefix, deadline hash, tier hash, queue prefix
var reclaimScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 32)
local n = 0
for _, member in ipairs(members) do
  local score = redis.call('ZSCORE', KEYS[1], member)
  if score and tonumber(score) <= now then
    redis.call('ZREM', KEYS[1], member)
    local id = string.match(member, '^([^|]+)|')
    if id and redis.call('EXISTS', ARGV[2] .. id) == 0 then
      local deadline = redis.call('HGET', ARGV[3], id)
      local tier = redis.call('HGET', ARGV[4], id)
      if deadline and tier and tonumber(deadline) > now then
        redis.call('ZADD', ARGV[5] .. tier, tonumber(deadline), id)
        n = n + 1
      else
        redis.call('LPUSH', KEYS[2], id)
      end
    end
  end
end
return n
`)

// KEYS: retry, expired list
// ARGV: now ms, result prefix, deadline hash, tier hash, queue prefix
var promoteScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 32)
local n = 0
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  if redis.call('EXISTS', ARGV[2] .. id) == 0 then
    local deadline = redis.call('HGET', ARGV[3], id)
    local tier = redis.call('HGET', ARGV[4], id)
    if deadline and tier and tonumber(deadline) > now then
      redis.call('ZADD', ARGV[5] .. tier, tonumber(deadline), id)
      n = n + 1
    else
      redis.call('LPUSH', KEYS[2], id)
    end
  end
end
return n
`)

// KEYS: queue, expired list
// ARGV: now ms, result prefix
// Moves ready units whose deadline has passed onto the expired list.
// This does not take a dispatch slot.
var expireReadyScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 32)
local n = 0
for _, id in ipairs(ids) do
  local removed = redis.call('ZREM', KEYS[1], id)
  if removed == 1 then
    if redis.call('EXISTS', ARGV[2] .. id) == 0 then
      redis.call('LPUSH', KEYS[2], id)
    end
    n = n + 1
  end
end
return n
`)

// KEYS: inputs, unit, queue, outstanding, deadline hash, tier hash
// ARGV: expected head, unit JSON, score, id, tier, window
// Pops the head only when it still matches and the window has room, and enqueues it.
var commitInputScript = redis.NewScript(`
local outstanding = tonumber(redis.call('GET', KEYS[4]) or '0')
if outstanding >= tonumber(ARGV[6]) then
  return 'full'
end
local head = redis.call('LINDEX', KEYS[1], 0)
if not head then
  return 'empty'
end
if head ~= ARGV[1] then
  return 'retry'
end
redis.call('LPOP', KEYS[1])
redis.call('SET', KEYS[2], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[4])
redis.call('HSET', KEYS[5], ARGV[4], ARGV[3])
redis.call('HSET', KEYS[6], ARGV[4], ARGV[5])
redis.call('INCR', KEYS[4])
return 'ok'
`)

// KEYS: inputs, result, lines hash, counts hash
// ARGV: expected head, result JSON, ttl seconds, customID, line JSON, count field
// Pops the head and records a terminal line in one script.
var commitTerminalScript = redis.NewScript(`
local head = redis.call('LINDEX', KEYS[1], 0)
if not head then
  return 'empty'
end
if head ~= ARGV[1] then
  return 'retry'
end
redis.call('LPOP', KEYS[1])
if redis.call('EXISTS', KEYS[2]) == 0 then
  redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[3]))
  local added = redis.call('HSETNX', KEYS[3], ARGV[4], ARGV[5])
  if added == 1 then
    redis.call('HINCRBY', KEYS[4], ARGV[6], 1)
  end
end
return 'ok'
`)

// KEYS: idem, nearline, unit, queue, deadline hash, tier hash
// ARGV: useIdem, request id, nearline JSON, unit JSON, score, tier, ttl seconds
// Creates the idempotency key, the nearline record, and the queued unit together.
var acceptNearlineScript = redis.NewScript(`
if ARGV[1] == '1' then
  local cur = redis.call('GET', KEYS[1])
  if cur and cur ~= '' then
    return 'exists:' .. cur
  end
  redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[7]))
end
redis.call('SET', KEYS[2], ARGV[3])
redis.call('SET', KEYS[3], ARGV[4])
redis.call('ZADD', KEYS[4], ARGV[5], ARGV[2])
redis.call('HSET', KEYS[5], ARGV[2], ARGV[5])
redis.call('HSET', KEYS[6], ARGV[2], ARGV[6])
return 'created:' .. ARGV[2]
`)

// KEYS: result, lines hash, counts hash, outstanding, claimed
// ARGV: result JSON, ttl seconds, isBatch, customID, line JSON, count field, claim member, decr outstanding
var finishScript = redis.NewScript(`
local created = 0
if redis.call('EXISTS', KEYS[1]) == 0 then
  created = 1
  redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
  if ARGV[3] == '1' and ARGV[4] ~= '' then
    local added = redis.call('HSETNX', KEYS[2], ARGV[4], ARGV[5])
    if added == 1 then
      redis.call('HINCRBY', KEYS[3], ARGV[6], 1)
      if ARGV[8] == '1' then
        local v = redis.call('DECR', KEYS[4])
        if tonumber(v) < 0 then
          redis.call('SET', KEYS[4], '0')
        end
      end
    end
  end
end
if ARGV[7] ~= '' then
  redis.call('ZREM', KEYS[5], ARGV[7])
end
return created
`)
