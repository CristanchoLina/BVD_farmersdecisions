sqlite3 data/dummy_dataset.db < get_duration_between_birth_and_death_if_no_calving.sql

echo "FR710,1,2013-01,658.0,1
FR710,1,2013-02,658.0,1
FR710,1,2013-03,658.0,1
FR710,1,2013-04,658.0,1
FR710,1,2013-05,658.0,1
FR710,1,2013-06,658.0,1
FR710,2,2013-01,658.0,1
FR710,2,2013-02,658.0,1
FR710,2,2013-03,658.0,1
FR710,2,2013-04,658.0,1
FR710,2,2013-05,658.0,1
FR710,2,2013-06,658.0,1" >> results/duration_between_birth_and_death_if_no_calving_dairy_by_holding.csv
echo "FR710,1,2013-01,658.0,1
FR710,1,2013-02,658.0,1
FR710,1,2013-03,658.0,1
FR710,1,2013-04,658.0,1
FR710,1,2013-05,658.0,1
FR710,1,2013-06,658.0,1
FR710,2,2013-01,658.0,1
FR710,2,2013-02,658.0,1
FR710,2,2013-03,658.0,1
FR710,2,2013-04,658.0,1
FR710,2,2013-05,658.0,1
FR710,2,2013-06,658.0,1" >> results/duration_between_birth_and_death_if_no_calving_beef_by_holding.csv

python3 computeParameters.py
python3 formatParameters.py
