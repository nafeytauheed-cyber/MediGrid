-- reconcile_free.lua: make one hospital/type free set equal to a PostgreSQL-read list of FREE beds,
-- skipping beds that currently carry a provisional hold. Recompute/set, never blind increments.
-- KEYS[1]=medigrid:cache_ready_generation  ARGV[1]=generation ARGV[2]=require_ready('1'|'0')
-- ARGV[3]=hospital ARGV[4]=bed_type ARGV[5..]=free bed ids from PostgreSQL
local g = ARGV[1]
if ARGV[2] == '1' and redis.call('GET', KEYS[1]) ~= g then return {'NOT_READY'} end
local key = 'mg:' .. g .. ':hosp:' .. ARGV[3] .. ':beds:' .. ARGV[4] .. ':free'
local held = 'mg:' .. g .. ':held_beds'
local want = {}
for i = 5, #ARGV do want[ARGV[i]] = true end
local removed, added = 0, 0
for _, b in ipairs(redis.call('SMEMBERS', key)) do
  if not want[b] then redis.call('SREM', key, b); removed = removed + 1 end
end
for b, _ in pairs(want) do
  if redis.call('SISMEMBER', held, b) == 0 then added = added + redis.call('SADD', key, b) end
end
return {'OK', tostring(added), tostring(removed)}
