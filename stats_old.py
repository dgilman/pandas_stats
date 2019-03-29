import datetime
import sys
import calendar
import itertools

def make_structs():
   most_active = []
   longest_held = {
      0: [],
      1: [],
      2: []
   }
   weakest_by_link = {
      1: [],
      2: []
   }
   weakest_by_age = {
      1: [],
      2: []
   }
   most_links = {
      1: [],
      2: []
   }
   return longest_held, most_active, weakest_by_link, most_links, weakest_by_age

def stats(c):
   longest_held, most_active, weakest_by_link, most_links, weakest_by_age = make_structs()

   c.execute("""
SELECT portal_id, cast(obs_time as float) / 1000, team, portal_level, portal_health, portal_obs_id
FROM (
   SELECT
      portal AS portal_id,
      obs_time*1000 AS obs_time,
      team AS team,
      level AS portal_level,
      health AS portal_health,
      id AS portal_obs_id
   FROM portal_obs

UNION ALL

   SELECT
      portal AS portal_id,
      timestampMs AS obs_time,
      players.faction AS team,
      1 AS portal_level,
      100 AS portal_health,
      NULL AS portal_obs_id
   FROM captured_plexts
   JOIN players ON players.id = captured_plexts.player

UNION ALL

   SELECT
      portal AS portal_id,
      timestampMs-1 AS obs_time,
      0 AS team,
      1 AS portal_level,
      0 AS portal_health,
      NULL AS portal_obs_id
   FROM captured_plexts
   JOIN players ON players.id = captured_plexts.player
) foo
ORDER BY portal_id, obs_time DESC""")
   for portal_id, obs in itertools.groupby(c.fetchall(), lambda x: x[0]):
      # Yes, this is incredibly shitty code. That's why I wanted to rewrite it in pandas.
      obs = [(datetime.datetime.utcfromtimestamp(ot), t, None, l, h, oid)
         for _, ot, t, l, h, oid in obs]
      ob1 = obs[0]
      obs = obs[1:]
      current_holder = ob1[1]
      # note that first_ts > last_ts - we're descending
      first_ts = ob1[0]
      if ob1[1] != 0:
         c.execute('SELECT count(*) FROM link_obs WHERE portal_head = ? OR portal_tail = ?',
            (ob1[5], ob1[5]))
         links = c.fetchall()[0][0]
         weakest_by_link[ob1[1]].append((ob1[4]/(float(ob1[3])+links),
            portal_id, ob1[4], ob1[3], links))

      flip_count = 0
      last_ts = first_ts
      current_held_time = None
      for observation in obs:
         ts = observation[0]

         if observation[1] != current_holder:
            if flip_count == 0:
               td = first_ts - ts
               longest_held[current_holder].append((td, portal_id))
               current_held_time = td
            flip_count += 1
            current_holder = observation[1]
         if ts < last_ts:
            last_ts = ts
      overall_td =  first_ts - last_ts
      if flip_count != 0:
         average_td = overall_td // flip_count
      else: # never captured
         longest_held[current_holder].append((overall_td, portal_id))
         average_td = datetime.timedelta.max
         current_held_time = overall_td
      most_active.append((average_td, portal_id, flip_count))
      if ob1[1] != 0 and ob1[4] <= 40:
         weakest_by_age[ob1[1]].append((current_held_time, ob1[4], portal_id))

   for faction in longest_held:
      longest_held[faction] = sorted(longest_held[faction], key=lambda x: x[0], reverse=True)
   most_active = sorted(most_active, key=lambda x: x[0])
   for faction in weakest_by_link:
      weakest_by_link[faction] = sorted(weakest_by_link[faction], key=lambda x: (x[0], x[1]))
   # sort by (age desc, health asc)
   # sort by health first then age
   for faction in weakest_by_age:
      weakest_by_age[faction] = sorted(weakest_by_age[faction], key=lambda x: x[1])
      weakest_by_age[faction] = sorted(weakest_by_age[faction], key=lambda x: x[0], reverse=True)

   for faction in weakest_by_link:
      most_links[faction] = sorted([(portal_id, links) for
         _, portal_id, _, _, links in weakest_by_link[faction]],
         key=lambda x: (x[1], x[0]), reverse=True)

   return longest_held, most_active, weakest_by_link, most_links, weakest_by_age

