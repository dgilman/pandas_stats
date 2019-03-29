Learning pandas by rewriting some number crunching code

stats.py: pandas re-implemetation
stats_old.py: the original native python implementation
old_driver.py: csv output wrapper for stats_old.py

The stats_old.py code loops over Python lists, creating tuples with relevant
statistical data and appending them to lists for sorting and output. The
stats.py uses pandas and its code is much easier to understand. Unfortunately
the old code runs in 1m17s while the pandas implementation takes 5m32s. I
believe this is because of the overhead of creating lots of little DataFrame
objects for each portal (most are under 10kb in size). There is only one data
operation that requires analysis of the entire DataFrame's time series plus the
data sets are pretty small so this project may not be in the sweet spot for
pandas but it was a good learning project.

