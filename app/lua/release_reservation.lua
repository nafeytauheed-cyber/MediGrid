-- release_reservation.lua (idempotent)
-- KEYS[1]=mg:<gen>:reservations:expiry  ARGV[1]=reservation_id  ARGV[2]='release'|'consume'  ARGV[3]=generation
-- 'release' returns the bed to the free set: the caller must have proven in PostgreSQL that the bed is FREE
-- and unowned. 'consume' removes the hold and keeps the bed out of the free set.
if ARGV[2] ~= 'release' and ARGV[2] ~= 'consume' then return {'BAD_INPUT', 'mode'} end
local g = ARGV[3]
local raw = redis.call('HGET', 'mg:' .. g .. ':res:data', ARGV[1])
if not raw then redis.call('ZREM', KEYS[1], ARGV[1]); return {'ALREADY_SETTLED'} end
local rec = cjson.decode(raw)
redis.call('HDEL', 'mg:' .. g .. ':res:data', ARGV[1])
redis.call('DEL', 'mg:' .. g .. ':res:ttl:' .. ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('SREM', 'mg:' .. g .. ':held_beds', rec.bed_id)
if ARGV[2] == 'release' then
  redis.call('SADD', 'mg:' .. g .. ':hosp:' .. rec.hospital_id .. ':beds:' .. rec.bed_type .. ':free', rec.bed_id)
end
return {'RELEASED', raw}
