package store

import "github.com/redis/go-redis/v9"

var releaseLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// Renews a compare-and-set lease. KEYS[1] is the lock.
// ARGV: token, ttl milliseconds.
var renewLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

// KEYS: unit, queue, outstanding, deadline hash, tier hash, token hash
// ARGV: unit JSON, score seconds, id, tier, incrOutstanding ("1"/"0"), token
var enqueueScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
redis.call('HSET', KEYS[4], ARGV[3], ARGV[2])
redis.call('HSET', KEYS[5], ARGV[3], ARGV[4])
redis.call('HSET', KEYS[6], ARGV[3], ARGV[6])
if ARGV[5] == '1' then
  redis.call('INCR', KEYS[3])
end
return 1
`)

// KEYS: queue, claimed, token hash, token seq
// ARGV: leaseUntil ms, owner
// Returns id|token, or an empty string when the queue is empty.
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
local token = redis.call('HGET', KEYS[3], id)
if not token or token == false or token == '' then
  token = tostring(redis.call('INCR', KEYS[4]))
  redis.call('HSET', KEYS[3], id, token)
end
redis.call('ZADD', KEYS[2], ARGV[1], id .. '|' .. token .. '|' .. ARGV[2])
return id .. '|' .. token
`)

// KEYS: claimed
// ARGV: id, token, owner, leaseUntil ms
var extendScript = redis.NewScript(`
local member = ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3]
local score = redis.call('ZSCORE', KEYS[1], member)
if not score then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[4], member)
return 1
`)

// KEYS: claimed, unit, retry
// ARGV: id, token, owner, unit JSON, retryAt ms
var parkScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3])
redis.call('SET', KEYS[2], ARGV[4])
redis.call('ZADD', KEYS[3], ARGV[5], ARGV[1])
return 1
`)

// Every key this script touches is in KEYS so a Redis Cluster slot check can see it.
// KEYS:
//
//	1 claimed
//	2 expired list
//	3 deadline hash
//	4 tier hash
//	5 finished set
//	6 token hash
//	7 token sequence
//	8 q:interactive
//	9 q:async
//	10 q:batch
//
// ARGV: now milliseconds (lease score), now Unix seconds (deadline score)
var reclaimScript = redis.NewScript(`
local nowMS = tonumber(ARGV[1])
local nowSec = tonumber(ARGV[2])

local function queueFor(tier)
  if tier == 'interactive' then return KEYS[8] end
  if tier == 'async' then return KEYS[9] end
  if tier == 'batch' then return KEYS[10] end
  return nil
end

local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 32)
local n = 0
for _, member in ipairs(members) do
  local score = redis.call('ZSCORE', KEYS[1], member)
  if score and tonumber(score) <= nowMS then
    redis.call('ZREM', KEYS[1], member)
    local id = string.match(member, '^([^|]+)')
    if id and redis.call('SISMEMBER', KEYS[5], id) == 0 then
      local deadline = redis.call('HGET', KEYS[3], id)
      local tier = redis.call('HGET', KEYS[4], id)
      local q = queueFor(tier)
      if deadline and q and tonumber(deadline) > nowSec then
        local token = tostring(redis.call('INCR', KEYS[7]))
        redis.call('HSET', KEYS[6], id, token)
        redis.call('ZADD', q, tonumber(deadline), id)
        n = n + 1
      else
        redis.call('LPUSH', KEYS[2], id)
      end
    end
  end
end
return n
`)

// KEYS: same layout as reclaim, except KEYS[1] is the retry zset.
// ARGV: now milliseconds, now Unix seconds.
var promoteScript = redis.NewScript(`
local nowMS = tonumber(ARGV[1])
local nowSec = tonumber(ARGV[2])

local function queueFor(tier)
  if tier == 'interactive' then return KEYS[8] end
  if tier == 'async' then return KEYS[9] end
  if tier == 'batch' then return KEYS[10] end
  return nil
end

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 32)
local n = 0
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  if redis.call('SISMEMBER', KEYS[5], id) == 0 then
    local deadline = redis.call('HGET', KEYS[3], id)
    local tier = redis.call('HGET', KEYS[4], id)
    local q = queueFor(tier)
    if deadline and q and tonumber(deadline) > nowSec then
      local token = tostring(redis.call('INCR', KEYS[7]))
      redis.call('HSET', KEYS[6], id, token)
      redis.call('ZADD', q, tonumber(deadline), id)
      n = n + 1
    else
      redis.call('LPUSH', KEYS[2], id)
    end
  end
end
return n
`)

// KEYS: queue, expired list, finished set
// ARGV: now Unix seconds, matching the ready-queue score.
var expireReadyScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 32)
local n = 0
for _, id in ipairs(ids) do
  local removed = redis.call('ZREM', KEYS[1], id)
  if removed == 1 then
    if redis.call('SISMEMBER', KEYS[3], id) == 0 then
      redis.call('LPUSH', KEYS[2], id)
    end
    n = n + 1
  end
end
return n
`)

// KEYS: inputs, unit, queue, outstanding, deadline hash, tier hash, token hash
// ARGV: expected head, unit JSON, score seconds, id, tier, window, token
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
redis.call('HSET', KEYS[7], ARGV[4], ARGV[7])
redis.call('INCR', KEYS[4])
return 'ok'
`)

// KEYS: inputs, result, lines hash, counts hash, finished set
// ARGV: expected head, result JSON, ttl seconds, customID, line JSON, count field, id
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
redis.call('SADD', KEYS[5], ARGV[7])
return 'ok'
`)

// KEYS: idem, nearline, unit, queue, deadline hash, tier hash, token hash
// ARGV: useIdem, request id, nearline JSON, unit JSON, score seconds, tier, ttl seconds, token
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
redis.call('HSET', KEYS[7], ARGV[2], ARGV[8])
return 'created:' .. ARGV[2]
`)

// KEYS: result, lines hash, counts hash, outstanding, claimed, finished set
// ARGV: result JSON, ttl seconds, isBatch, customID, line JSON, count field,
//
//	claim member, decr outstanding, id
//
// A non-empty claim member must still be leased. Otherwise the write is rejected
// so a stale owner cannot record a result after reclaim rotated the token.
var finishScript = redis.NewScript(`
if ARGV[7] ~= '' then
  local score = redis.call('ZSCORE', KEYS[5], ARGV[7])
  if not score then
    return 0
  end
end
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
redis.call('SADD', KEYS[6], ARGV[9])
if ARGV[7] ~= '' then
  redis.call('ZREM', KEYS[5], ARGV[7])
end
return created
`)

// KEYS: slot hash
// ARGV: now ms, max, field (tier|owner), expiry ms
var acquireSlotScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local all = redis.call('HGETALL', KEYS[1])
local n = 0
for i = 1, #all, 2 do
  local exp = tonumber(all[i + 1])
  if (not exp) or exp <= now then
    redis.call('HDEL', KEYS[1], all[i])
  else
    n = n + 1
  end
end
if n >= max then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[3], ARGV[4])
return 1
`)

// KEYS: slot hash
// ARGV: field, expiry ms
var extendSlotScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// KEYS: slot hash
// ARGV: field
var releaseSlotScript = redis.NewScript(`
return redis.call('HDEL', KEYS[1], ARGV[1])
`)
