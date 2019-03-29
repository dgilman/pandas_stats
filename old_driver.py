import sqlite3
import csv

import stats_old

def main():
    conn = sqlite3.connect('guardian.sqlite3')
    cur = conn.cursor()

    longest_held, most_active, weakest_by_link, most_links, weakest_by_age = stats_old.stats(cur)

    with open('csvs/most_active_old.csv', 'w') as fd:
        writer = csv.writer(fd)
        writer.writerow(('portal_id', 'average_td', 'flip_count'))
        # Because of the difference between datetime.datetime.max and pd.Timedelta.max we get different values for the flip_count = 0 case.
        # Just ignore them, as they're not interesting anyway.
        writer.writerows(((portal_id, int(average_td.total_seconds()), flip_count) for average_td, portal_id, flip_count in most_active
            if flip_count != 0))

    for faction in longest_held:
        with open('csvs/longest_held_{0}_old.csv'.format(faction), 'w') as fd:
            writer = csv.writer(fd)
            writer.writerow(('portal_id', 'current_held_td'))
            writer.writerows(((portal_id, int(td.total_seconds())) for td, portal_id in longest_held[faction]))

    for faction in weakest_by_link:
        with open('csvs/weakest_by_link_{0}_old.csv'.format(faction), 'w') as fd:
            writer = csv.writer(fd)
            writer.writerow(('portal_id', 'health', 'level', 'links'))
            writer.writerows(((portal_id, health, level, links) for _, portal_id, health, level, links in weakest_by_link[faction]))

    for faction in most_links:
        with open('csvs/most_links_{0}_old.csv'.format(faction), 'w') as fd:
            writer = csv.writer(fd)
            writer.writerow(('portal_id', 'links'))
            writer.writerows(((portal_id, links) for portal_id, links in most_links[faction]))

    for faction in weakest_by_age:
        with open('csvs/weakest_by_age_{0}_old.csv'.format(faction), 'w') as fd:
            writer = csv.writer(fd)
            writer.writerow(('portal_id', 'current_held_td', 'health'))
            writer.writerows(((portal_id, int(current_held_time.total_seconds()), health) for current_held_time, health, portal_id in weakest_by_age[faction]))

if __name__ == "__main__":
    main()
