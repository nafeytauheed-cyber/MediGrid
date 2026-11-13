-- reserve_nearest_bed.lua: provisional hold only. PostgreSQL decides the allocation.
-- KEYS[1]=medigrid:cache_ready_generation  KEYS[2]=ratelimit:<actor>  KEYS[3]=idemp:res:<actor>:<key>
-- KEYS[4]=mg:<gen>:reservations:expiry
-- ARGV[1]=bed_type ARGV[2]=reservation_id ARGV[3]=ttl_ms ARGV[4]=rate_max ARGV[5]=rate_window_ms
-- ARGV[6]=expected_ready_generation ARGV[7]=meta_json ARGV[8..]=candidate hospital ids, nearest first
-- Returns plain arrays: {'NOT_READY'} {'IDEMPOTENT_HIT', rec} {'RATE_LIMITED', pttl}
--                       {'RESERVED', rec} {'NO_SUITABLE_BED'} {'BAD_INPUT', reason}
local bt = ARGV[1]
if bt ~= 'ICU' and bt ~= 'VENTILATOR' and bt ~= 'GENERAL' then return {'BAD_INPUT', 'bed_type'} end
local ttl = tonumber(ARGV[3]); local rmax = tonumber(ARGV[4]); local rwin = tonumber(ARGV[5])
if not ttl or not rmax or not rwin or ttl <= 0 then return {'BAD_INPUT', 'numbers'} end
local gen = ARGV[6]
if redis.call('GET', KEYS[1]) ~= gen then return {'NOT_READY'} end
local prior = redis.call('GET', KEYS[3])
if prior then return {'IDEMPOTENT_HIT', prior} end
local n = redis.call('INCR', KEYS[2])
if n == 1 then redis.call('PEXPIRE', KEYS[2], rwin) end
if n > rmax then return {'RATE_LIMITED', tostring(redis.call('PTTL', KEYS[2]))} end
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local exp_ms = now_ms + ttl
for i = 8, #ARGV do
  local hosp = ARGV[i]
  local free_key = 'mg:' .. gen .. ':hosp:' .. hosp .. ':beds:' .. bt .. ':free'
  local bed_id = redis.call('SPOP', free_key)
  if bed_id then
    local rec = cjson.encode({reservation_id=ARGV[2], hospital_id=hosp, bed_id=bed_id, bed_type=bt,
                              expires_at_ms=exp_ms, created_ms=now_ms, generation=gen, meta=ARGV[7]})
    redis.call('HSET', 'mg:' .. gen .. ':res:data', ARGV[2], rec)
    redis.call('ZADD', KEYS[4], exp_ms, ARGV[2])
    redis.call('SET', 'mg:' .. gen .. ':res:ttl:' .. ARGV[2], bed_id, 'PX', ttl)
    redis.call('SADD', 'mg:' .. gen .. ':held_beds', bed_id)
    redis.call('SET', KEYS[3], rec, 'PX', 3600000)
    return {'RESERVED', rec}
  end
end
return {'NO_SUITABLE_BED'}
