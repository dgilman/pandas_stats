import sqlite3
import datetime
import itertools

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d|%(message)s', datefmt='%H:%M:%S')

import pandas as pd

conn = sqlite3.connect('guardian.sqlite3')

# Overall stats to be calculated:
# longest_held (by team): the portal with the biggest current_held_td
# most_active: the portal with the smallest average_td
# weakest_by_link (by team): the portal with the smallest weakest_by_link
# most_links (by team): the portal with the most links
# weakest_by_age (by team): the portal with the lowest (health, current_held_td)

# Per-portal stats to be calculated:
# weakest_by_link: (first_obs health / (first_obs level + first_obs links))
# overall_td: first_obs obs_time - last_obs obs_time
# flip_count: number of times an observation's team was not the previous obs' team
# average_td: overall_td / flip_count
# health: first_obs health
# links: first_obs links
# current_held_td: first flip time - first observation time

def calculate_per_portal_stats(portal_id, df):
    # There's a lot of wasted fetching here - we only look at the
    # first and last rows for most data, and the only time we need the
    # entire history is just on the team column.
    # The pre-pandas version of the code worked the same way so I'm copying
    # its design to get more of a head-to-head comparison. One advantage of
    # pandas code is that it makes the data dependencies much more clear.
    flips = df['team'].diff().astype('bool')
    flips = flips[flips == True]

    # Note that we are descending - the first obs is the most recent chronological one!
    first_obs = df.iloc[0]
    last_obs = df.iloc[-1]
    overall_td = first_obs['obs_time'] - last_obs['obs_time']

    health = first_obs['health']
    links = first_obs['links']
    level = first_obs['level']
    team = first_obs['team']
    weakest_by_link = health / (level + links)

    if flips.size == 1:
        # Never captured
        flip_count = 0
        average_td = pd.Timedelta.max
        current_held_td = overall_td
    else:
        # The first row always counts as a flip because diff() inserts a NaN value when it shifts.
        # Ignore it, and get the index for the row after that one.
        flips = flips[1:]
        first_flip = flips.index[0]
        flip_count = flips.size
        average_td = overall_td / flip_count
        current_held_td = first_obs['obs_time'] - df.loc[first_flip]['obs_time']

    return pd.DataFrame({
        # portal_id is basically an index, and was originally an index,
        # however it's painful/impossible to sort by both a series
        # and the index value, so just make it a series i guess.
        "portal_id": [portal_id],
        "weakest_by_link": [weakest_by_link],
        "flip_count": [flip_count],
        "average_td": [average_td],
        "health": [health],
        "links": [links],
        "level": [level],
        "team": [team],
        "current_held_td": [current_held_td],
        })

def calculate_overall_stats(portals):
    portals.sort_values(by=['team'])

    most_active = portals[['portal_id', 'average_td', 'flip_count']].sort_values(by=['average_td', 'portal_id'])
    most_active = most_active[most_active['flip_count'] != 0]
    most_active['average_td'] = most_active['average_td'].astype('timedelta64[s]').astype('int64')
    most_active.to_csv('csvs/most_active_new.csv', index=False, line_terminator='\r\n')

    for team, group in portals[['team', 'portal_id', 'current_held_td']].groupby('team', sort=False):
        longest_held = group.sort_values(by=['current_held_td', 'portal_id'], ascending=[False, True])
        longest_held.drop(columns=['team'], inplace=True)
        longest_held['current_held_td'] = longest_held['current_held_td'].astype('timedelta64[s]').astype('int64')
        longest_held.to_csv('csvs/longest_held_{0}_new.csv'.format(team), index=False, line_terminator='\r\n')

    for team, group in portals[['team', 'portal_id', 'weakest_by_link', 'health', 'level', 'links']].groupby('team', sort=False):
        if team == 0:
            continue
        weakest_by_link = group.sort_values(by=['weakest_by_link', 'portal_id'])
        weakest_by_link.drop(columns=['weakest_by_link', 'team'], inplace=True)
        weakest_by_link.to_csv('csvs/weakest_by_link_{0}_new.csv'.format(team), index=False, line_terminator='\r\n')

    for team, group in portals[['team', 'portal_id', 'links']].groupby('team', sort=False):
        if team == 0:
            continue
        most_links = group.sort_values(by=['links', 'portal_id'], ascending=[False, False])
        most_links.drop(columns=['team'], inplace=True)
        most_links.to_csv('csvs/most_links_{0}_new.csv'.format(team), index=False, line_terminator='\r\n')

    for team, group in portals[['portal_id', 'team', 'current_held_td', 'health']].groupby('team', sort=False):
        if team == 0:
            continue
        weakest_by_age = group.sort_values(by=['current_held_td', 'health'], ascending=[False, False])
        weakest_by_age.drop(columns=['team'], inplace=True)
        weakest_by_age = weakest_by_age[weakest_by_age['health'] <= 40]
        weakest_by_age['current_held_td'] = weakest_by_age['current_held_td'].astype('timedelta64[s]').astype('int64')
        weakest_by_age.to_csv('csvs/weakest_by_age_{0}_new.csv'.format(team), index=False, line_terminator='\r\n')

def main():
    logging.debug('Starting')
    # From day one this query was going to return a lot of data,
    # making it a no-go to load the entire thing into memory at once.
    # Luckily, we only need a single portal's history at a time for
    # number crunching so we only need a subset of rows at a time.
    # The initial solution was to query a list of all portals
    # and do this big UNION query in a loop, filtering in the WHERE by the
    # appropriate portal each time.
    # The WHERE clauses were all satisfied by indexes, so this should
    # be reasonably efficient, right?
    # Well, no. The program's run time was dominated by traversing the index
    # over and over again for each query, it was just pure overhead.
    # It's much faster to use those indexes in the ORDER BY
    # and use itertools.groupby to iterate over the cursor and efficiently
    # fetch rows for one portal at a time.
    # Lesson learned: if you're effectively doing a full table scan in
    # your code do a full table scan in the query!
    df = pd.read_sql("""
SELECT
   portal_id,
   cast(obs_time as float) / 1000 as obs_time,
   team, portal_level AS level, portal_health AS health, COALESCE(links, 0) AS links
FROM (
   SELECT
      portal AS portal_id
      , obs_time*1000 AS obs_time
      , team AS team
      , level AS portal_level
      , health AS portal_health
      , id AS portal_obs_id
   FROM portal_obs

UNION ALL

   SELECT
      portal AS portal_id
      , timestampMs AS obs_time
      , players.faction AS team
      , 1 AS portal_level
      , 100 AS portal_health
      , NULL AS portal_obs_id
   FROM captured_plexts
   JOIN players ON players.id = captured_plexts.player

UNION ALL

   SELECT
      portal AS portal_id
      -- Insert a neutral portal observation in the
      -- millisecond before a portal is captured.
      , timestampMs-1 AS obs_time
      , 0 AS team
      , 1 AS portal_level
      , 0 AS portal_health
      , NULL AS portal_obs_id
   FROM captured_plexts
   JOIN players ON players.id = captured_plexts.player
) foo
LEFT JOIN (
   SELECT portal_obs_id, COUNT(*) AS links
   FROM (
      SELECT portal_head AS portal_obs_id FROM link_obs

      UNION ALL

      SELECT portal_tail AS portal_obs_id FROM link_obs
   ) link_obs_union
   GROUP BY portal_obs_id
) link_obs USING (portal_obs_id)
ORDER BY portal_id, obs_time DESC
""", conn, parse_dates={"obs_time": {"unit": "s"}})
    logging.debug('Created mega dataframe.')
    df.info()
    # If you put the entire query above in a single dataframe, it's a giant
    # 500 mb struct that takes several minutes to populate, a non-starter
    # for iterative development.
    # Instead, I have python's itertools.groupby fetch rows as they're needed
    # from the cursor. The initial query processing time takes ~30 sec,
    # most of which is due to the subquery that calculates links.

    # Part of me is disappointed that Pandas doesn't have something
    # out of the box for what seems to me like an ordinary problem,
    # but maybe there's something down in the intersection between
    # Pandas' GroupBy and the SQL query that can get the same efficiency
    per_portal_stats = []
    for portal_id, observations in df.groupby("portal_id", sort=False):
        per_portal_stats.append(calculate_per_portal_stats(portal_id, observations))
    logging.debug('Per-portal stats finished.')
    overall_df = pd.concat(per_portal_stats)
    logging.debug('Portal stats concatenated.')
    overall_stats = calculate_overall_stats(overall_df)
    logging.debug('Done.')

if __name__ == "__main__":
    main()

