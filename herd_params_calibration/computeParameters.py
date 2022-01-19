import pandas as pd
import numpy as np
import sqlite3
import time
import calendar

#constants from EMULSION model
mor_age_threshold = 21
dur_G = 274
#other constants
paritiesNb = 6  #WARNING: from 0 to 5 included, 0 means gestating females but not yet calved

#DB SQLite
dataPath = "data/"
dbFileName = "dummy_dataset.db"
dbPath = "{}{}".format(dataPath, dbFileName)

#Classification of breed codes as diary or beef
lDairyBreed = '("12", "15", "18", "19", "21", "26", "29", "31", "35", "42", "43", "46", "56", "57", "63", "65", "66", "69", "74", "81", "82", "92", "44", "39")'
lBeefBreed = '("10", "14", "17", "20", "23", "24", "25", "30", "32", "33", "34", "36", "37", "38", "41", "45", "49", "52", "53", "54", "55", "58", "61", "71", "72", "73", "75", "76", "77", "78", "79", "85", "86", "88", "90", "93", "95", "97", "51", "48")'
lAllBreed = '("12", "15", "18", "19", "21", "26", "29", "31", "35", "42", "43", "46", "56", "57", "63", "65", "66", "69", "74", "81", "82", "92", "44", "39", "10", "14", "17", "20", "23", "24", "25", "30", "32", "33", "34", "36", "37", "38", "41", "45", "49", "52", "53", "54", "55", "58", "61", "71", "72", "73", "75", "76", "77", "78", "79", "85", "86", "88", "90", "93", "95", "97", "51", "48")'

resultsPath = "results/"

#each parameter is computed per period (month in this case)
firstMonth = "2013-01"
lastMonth = "2013-06"
lMonths = pd.date_range(firstMonth, lastMonth, freq="MS").strftime("%Y-%m").tolist()

def computeSexRatio(lBreed, breed):
    '''
    proportion of females in newborns
    => per month, per herd
    '''
    #WARNING: filter on animal born in France, because the "holding of birth" of animals born outside France will be the first holding in which they enter in France
    query = '''
        SELECT holding_of_birth, SUBSTR(dat_naiss,1,7) AS period, sexe, COUNT(*) AS nb
        FROM bovins
        WHERE substr(animal_id,1,2) = "FR" AND code_race IN {}
        GROUP BY holding_of_birth, sexe, period
    '''.format(lBreed)
    with sqlite3.connect(dbPath) as iConnector:
        df = pd.read_sql_query(query, iConnector)
    if df.empty:
        raise Exception("empty query {}".format(query))
    #drop unknown values
    df = df[(df.holding_of_birth != "NULLNULL") & (df.holding_of_birth != "FRNULL")]
    #select sex and create table: holding_of_birth (as line) X period (as column)
    dfMales = df[df.sexe == "1"].drop(columns=["sexe"]).pivot(index="holding_of_birth", columns="period", values="nb")
    dfFemales = df[df.sexe == "2"].drop(columns=["sexe"]).pivot(index="holding_of_birth", columns="period", values="nb")
    #select wanted months and compute ratio
    dfMales = dfMales.reindex(columns=lMonths).fillna(0)
    dfFemales = dfFemales.reindex(columns=lMonths).fillna(0)
    dfRatio = dfFemales / (dfMales + dfFemales)
    #write tables in files
    dfMales.to_csv("{}malesNb_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))
    dfFemales.to_csv("{}femalesNb_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))
    dfRatio.to_csv("{}sexRatio_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))

def computeDurFadult(lBreed, breed):
    '''
    mean duration between calving and exit (except selling) for specific females. These females will not calve again in any herd and should have calved in the interval (in the month concerned). We should know their exit date (from any herd). Selling to another herd is not considered as an exit here.
    => per month, per breed (dairy, beef)
    '''
    #WARNING: make sure that duration is >0, because in some cases, animal was declared as dead, but then continued to live...
    #WARNING: remove lines where holding id isn't well formated
    query = '''
        SELECT SUBSTR(last_calving_date,1,7) AS period, AVG(duration_last_calving_to_death) AS mean_duration_last_calving_to_death, COUNT(*) AS nb_animals FROM
        (
        SELECT c.animal_id, d.code_race, c.last_calving_date, (JULIANDAY(c.date_of_death) - JULIANDAY(c.last_calving_date)) AS duration_last_calving_to_death FROM
            (SELECT a.animal_id, a.date_of_death, b.last_calving_date FROM
                (SELECT * FROM date_of_death WHERE animal_id IN (SELECT id_mere FROM calving_date_and_parity)) AS a
            INNER JOIN
                (SELECT id_mere, MAX(calving_date) AS last_calving_date FROM calving_date_and_parity WHERE holding_of_birth NOT IN ("NULLNULL", "FRNULL") GROUP BY id_mere) AS b
            ON a.animal_id = b.id_mere) AS c
        LEFT JOIN
            (SELECT * FROM bovins) AS d
        ON c.animal_id = d.animal_id
        )
        WHERE code_race IN {} AND duration_last_calving_to_death > 0
        GROUP BY period
    '''.format(lBreed)
    with sqlite3.connect(dbPath) as iConnector:
        df = pd.read_sql_query(query, iConnector)
    if df.empty:
        raise Exception("empty query {}".format(query))
    #create table: duration (as 1 line) X period (as column) to keep the same format as other parameters
    df = df.set_index("period").transpose().reindex(columns=lMonths)
    #write table in file
    df.to_csv("{}durFadult_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))

def computeDurNG(lBreed, breed):
    '''
    mean calving to calving interval - dur_G (the duration of gestation which is identical for all breeds), irrespective of parity and of the herd they will be at calving
    => per month, per herd, per breed (dairy, beef)
    '''
    #WARNING: make sure that duration is >= 0, because sometimes a female calves again in less than dur_G...
    #WARNING: sometimes a female calves the same day in two different holdings...to avoid taking into account twice this duration, check that there is only "one calf born" (calving_index) between two calvings.
    query = '''
        SELECT SUBSTR(c.calving_date,1,7) AS period, c.holding_of_birth, calving_index, next_calving_index, AVG(duration_between_consecutive_gestations) AS mean_duration_between_consecutive_gestations, COUNT(*) AS durations_nb FROM
            (SELECT id_mere, calving_date, holding_of_birth, calving_index, next_calving_index, MIN(duration_between_two_gestations) AS duration_between_consecutive_gestations FROM (
                SELECT a.id_mere, a.calving_date, a.holding_of_birth, a.calving_index, b.calving_index AS next_calving_index, (JULIANDAY(b.calving_date)-JULIANDAY(a.calving_date) - {}) AS duration_between_two_gestations FROM
                    (SELECT * FROM calving_date_and_parity) AS a
                INNER JOIN
                    (SELECT * FROM calving_date_and_parity) AS b
                ON a.id_mere = b.id_mere AND a.calving_date < b.calving_date)
            WHERE duration_between_two_gestations >= 0
            GROUP BY id_mere, calving_date, holding_of_birth, calving_index) AS c
        INNER JOIN
            (SELECT * FROM bovins WHERE code_race IN {}) AS d
        ON c.id_mere = d.animal_id
        WHERE next_calving_index - calving_index = 1
        GROUP BY c.holding_of_birth, period
    '''.format(dur_G, lBreed)
    with sqlite3.connect(dbPath) as iConnector:
        df = pd.read_sql_query(query, iConnector)
    if df.empty:
        raise Exception("empty query {}".format(query))
    #drop unknown values
    df = df[(df.holding_of_birth != "NULLNULL") & (df.holding_of_birth != "FRNULL")]
    #create table: holding_of_birth (as line) X period (as column) and drop unused durations_nb
    dfmean = df.drop(columns=["durations_nb"]).pivot(index="holding_of_birth", columns="period", values="mean_duration_between_consecutive_gestations")
    #select wanted months and write table in file
    dfmean.reindex(columns=lMonths).to_csv("{}durNG_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))
    #create table: holding_of_birth (as line) X period (as column) and drop unused mean_duration_between_consecutive_gestations
    dfnb = df.drop(columns=["mean_duration_between_consecutive_gestations"]).pivot(index="holding_of_birth", columns="period", values="durations_nb")
    #select wanted months and write table in file
    dfnb.reindex(columns=lMonths).to_csv("{}durNG_nb_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))

def computeDurJ(lBreed, breed):
    '''
    mean duration between birth and first calving - dur_G (the duration of gestation which is identical for all breeds), irrespective of the herd they will be at calving
    => per month, per herd, per breed (dairy, beef)
    '''
    #WARNING: make sure that duration is >0, because sometimes a female calves before being born or she calves and is born the same day...
    query = '''
        SELECT holding_of_birth, SUBSTR(dat_naiss,1,7) AS period, AVG(JULIANDAY(first_calving_date) - {0} - JULIANDAY(dat_naiss)) AS mean_duration_before_first_gestation, COUNT(*) AS nb_animals FROM
            (SELECT id_mere, MIN(calving_date) AS first_calving_date FROM calving_date_and_parity GROUP BY id_mere) AS a
        INNER JOIN
            (SELECT animal_id, dat_naiss, code_race, holding_of_birth FROM bovins) AS b
        ON a.id_mere = b.animal_id
        WHERE code_race IN {1} AND (JULIANDAY(first_calving_date) - {0} - JULIANDAY(dat_naiss)) > 0
        GROUP BY holding_of_birth, period
    '''.format(dur_G, lBreed)
    with sqlite3.connect(dbPath) as iConnector:
        df = pd.read_sql_query(query, iConnector)
    if df.empty:
        raise Exception("empty query {}".format(query))
    #drop unknown values
    df = df[(df.holding_of_birth != "NULLNULL") & (df.holding_of_birth != "FRNULL")]
    #create table: holding_of_birth (as line) X period (as column) and drop unused nb_animals
    dfmean = df.drop(columns=["nb_animals"]).pivot(index="holding_of_birth", columns="period", values="mean_duration_before_first_gestation")
    #select wanted months and write table in file
    dfmean.reindex(columns=lMonths).to_csv("{}durJ_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))
    #create table: holding_of_birth (as line) X period (as column) and drop unused mean_duration_before_first_gestation
    dfnb = df.drop(columns=["mean_duration_before_first_gestation"]).pivot(index="holding_of_birth", columns="period", values="nb_animals")
    #select wanted months and write table in file
    dfnb.reindex(columns=lMonths).to_csv("{}durJ_nb_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))

def computeBreedingFemalesProp(lBreed, breed):
    '''
    proportion of females that will calve, irrespective of the herd they will be at calving, among a specific population of females. These females should be newborns that will survive beyond mor_age_threshold. We should know their calving date or their exit date in any herd. Selling to another herd is not considered as an exit here. Females without calving and sold with no further information should not be accounted for.
    => per month, per herd, per breed (dairy, beef)
    '''
    #WARNING: in the data, there are some males (sexe=1) that calved...so don't count them in table calving_date_and_parity (or it would need a more complex query for nb_females) to avoid prop > 1
    with sqlite3.connect(dbPath) as iConnector:
        query = '''
            SELECT holding_of_birth, SUBSTR(dat_naiss,1,7) AS period, (JULIANDAY(date_of_death) - JULIANDAY(dat_naiss)) AS age_at_death, COUNT(*) AS nb_females FROM
                (   (SELECT animal_id, dat_naiss, holding_of_birth FROM bovins WHERE code_race IN {} AND sexe = "2") AS a
                LEFT JOIN
                    (SELECT animal_id, date_of_death FROM date_of_death) AS b
                ON a.animal_id = b.animal_id) as c
            LEFT JOIN
                (SELECT id_mere, calving_date FROM calving_date_and_parity GROUP BY id_mere) AS d
            ON c.animal_id = d.id_mere
            WHERE age_at_death > {} OR calving_date NOT NULL
            GROUP BY holding_of_birth, period
        '''.format(lBreed, mor_age_threshold)
        dfAllFemales = pd.read_sql_query(query, iConnector)
        if dfAllFemales.empty:
            raise Exception("empty query {}".format(query))
        query = '''
            SELECT holding_of_birth, SUBSTR(dat_naiss,1,7) AS period, COUNT(*) AS nb_that_calved
            FROM bovins
            WHERE code_race IN {} AND sexe = "2"
                AND animal_id IN (SELECT id_mere FROM calving_date_and_parity)
            GROUP BY holding_of_birth, period
        '''.format(lBreed)
        dfBreedingFemales = pd.read_sql_query(query, iConnector)
        if dfBreedingFemales.empty:
            raise Exception("empty query {}".format(query))
    #drop unknown values
    dfAllFemales = dfAllFemales[(dfAllFemales.holding_of_birth != "NULLNULL") & (dfAllFemales.holding_of_birth != "FRNULL")]
    dfBreedingFemales = dfBreedingFemales[(dfBreedingFemales.holding_of_birth != "NULLNULL") & (dfBreedingFemales.holding_of_birth != "FRNULL")]
    #create table: holding_of_birth & period (as line) X nb_females & nb_that_calved (as column) and drop unused age_at_death (was used to filter females, but makes no sense for grouping: only the age of the first "row" before grouping is kept)
    dfAllFemales = dfAllFemales.set_index(["holding_of_birth", "period"]).drop(columns=["age_at_death"])
    dfBreedingFemales = dfBreedingFemales.set_index(["holding_of_birth", "period"])
    df = dfAllFemales.join(dfBreedingFemales, how="outer")
    #compute proportion of females kept for breeding
    df.nb_that_calved = df.nb_that_calved.fillna(0)
    df["proportion"] = df.nb_that_calved / df.nb_females
    #for each output (column), drop unused columns and create table: holding_of_birth (as line) X period (as column) NOTE: not done before, so that all outputs have the same dimensions (same holdings, same periods)
    df = df.reset_index()
    dfAllFemales = df.drop(columns=["nb_that_calved", "proportion"]).pivot(index="holding_of_birth", columns="period", values="nb_females")
    dfBreedingFemales = df.drop(columns=["nb_females", "proportion"]).pivot(index="holding_of_birth", columns="period", values="nb_that_calved")
    dfPropOfBredFem = df.drop(columns=["nb_females", "nb_that_calved"]).pivot(index="holding_of_birth", columns="period", values="proportion")
    #select wanted months and write table in file
    dfAllFemales.reindex(columns=lMonths).to_csv("{}nbFemalesDeadAfter{}_{}_{}_{}.csv".format(resultsPath, mor_age_threshold, breed, firstMonth, lastMonth))
    dfBreedingFemales.reindex(columns=lMonths).to_csv("{}nbBreedingFemales_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))
    dfPropOfBredFem.reindex(columns=lMonths).to_csv("{}propOfBreedingFem_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))

def computeYoungDeadProp(lBreed, breed):
    '''
    proportion of dead juveniles before mor_age_threshold in herd (for the whole period)
    => per month, per herd, per breed (dairy, beef)
    '''
    with sqlite3.connect(dbPath) as iConnector:
        query = '''
            SELECT holding_of_birth, SUBSTR(dat_naiss,1,7) AS period, COUNT(*) AS nb_births
            FROM bovins
            WHERE code_race IN {}
            GROUP BY holding_of_birth, period
        '''.format(lBreed)
        dfAllBirths = pd.read_sql_query(query, iConnector)
        if dfAllBirths.empty:
            raise Exception("empty query {}".format(query))
        query = '''
            SELECT holding_of_birth, SUBSTR(dat_naiss,1,7) AS period, (JULIANDAY(date_of_death) - JULIANDAY(dat_naiss)) AS age_at_death, COUNT(*) AS nb_deaths FROM
                (SELECT animal_id, date_of_death FROM date_of_death) AS a
            INNER JOIN
                (SELECT animal_id, dat_naiss, holding_of_birth FROM bovins WHERE code_race IN {}) AS b
            ON a.animal_id = b.animal_id
            WHERE age_at_death <= {}
            GROUP BY holding_of_birth, period
        '''.format(lBreed, mor_age_threshold)
        dfDeaths = pd.read_sql_query(query, iConnector)
        if dfDeaths.empty:
            raise Exception("empty query {}".format(query))
    #drop unknown values
    dfAllBirths = dfAllBirths[(dfAllBirths.holding_of_birth != "NULLNULL") & (dfAllBirths.holding_of_birth != "FRNULL")]
    dfDeaths = dfDeaths[(dfDeaths.holding_of_birth != "NULLNULL") & (dfDeaths.holding_of_birth != "FRNULL")]
    #create table: holding_of_birth & period (as line) X nb_females & nb_that_calved (as column) and drop unused age_at_death (was used to filter, but makes no sense for grouping: only the age of the first "row" before grouping is kept)
    dfAllBirths = dfAllBirths.set_index(["holding_of_birth", "period"])
    dfDeaths = dfDeaths.set_index(["holding_of_birth", "period"]).drop(columns=["age_at_death"])
    df = dfAllBirths.join(dfDeaths, how="outer")
    #compute proportion of females kept for breeding
    df.nb_deaths = df.nb_deaths.fillna(0)
    df["proportion"] = df.nb_deaths / df.nb_births
    #for each output (column), drop unused columns and create table: holding_of_birth (as line) X period (as column) NOTE: not done before, so that all outputs have the same dimensions (same holdings, same periods)
    df = df.reset_index()
    dfAllBirths = df.drop(columns=["nb_deaths", "proportion"]).pivot(index="holding_of_birth", columns="period", values="nb_births")
    dfDeaths = df.drop(columns=["nb_births", "proportion"]).pivot(index="holding_of_birth", columns="period", values="nb_deaths")
    dfPropOfDead = df.drop(columns=["nb_births", "nb_deaths"]).pivot(index="holding_of_birth", columns="period", values="proportion")
    #select wanted months and write table in file
    dfAllBirths.reindex(columns=lMonths).to_csv("{}nbBirths_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))
    dfDeaths.reindex(columns=lMonths).to_csv("{}nbDeathsBefore{}_{}_{}_{}.csv".format(resultsPath, mor_age_threshold, breed, firstMonth, lastMonth))
    dfPropOfDead.reindex(columns=lMonths).to_csv("{}propOfYoungDead_{}_{}_{}.csv".format(resultsPath, breed, firstMonth, lastMonth))

def computeCulledFemalesProp(lBreed, breed):
    '''
    proportion of females that will not calve again in any herd, among a specific population of females. These females are in the considered herd in the interval (in the month concerned) and should calve for the first time (for parity 0) in the interval+dur_G. We should know their second calving date or their exit date in any herd. Selling to another herd is not considered as an exit here. Females without second calving and sold with no further information should not be accounted for.
    => per month, per herd, per breed (dairy, beef), per parity (from 0 to 5+)
    '''
    #WARNING: in the data, there are some males (sexe=1) that calved...so don't count them
    lFemNbPerParity = []
    lCullNbPerParity = []
    lPropPerParity = []
    for _ in range(paritiesNb):
        lFemNbPerParity.append(pd.DataFrame())
        lCullNbPerParity.append(pd.DataFrame())
        lPropPerParity.append(pd.DataFrame())
    with sqlite3.connect(dbPath) as iConnector:
        for month in lMonths:
            firstDay = "{}-01".format(month)
            lastDay = "{}-{}".format(month, calendar.monthrange(int(month[0:4]), int(month[5:7]))[1])
            query = '''
                SELECT holding_id, parity, COUNT(*) AS nb_females FROM
                    (SELECT id_mere, MIN({5}, calving_index) AS parity FROM calving_date_and_parity WHERE calving_date >= DATE('{0}','+{1} days') AND calving_date <= DATE('{2}','+{1} days')) AS a
                INNER JOIN
                    (SELECT animal_id, holding_id FROM detentions WHERE SUBSTR(date_of_entry,1,7) <= '{3}' AND SUBSTR(date_of_exit,1,7) >= '{3}' AND animal_id IN
                        (SELECT animal_id FROM bovins WHERE code_race IN {4} AND sexe = "2")) AS b
                ON a.id_mere = b.animal_id
                GROUP BY holding_id, parity
            '''.format(firstDay, dur_G, lastDay, month, lBreed, paritiesNb)
            dfAllFemales = pd.read_sql_query(query, iConnector)
            if dfAllFemales.empty:
                raise Exception("empty query {}".format(query))
            query = '''
                SELECT holding_id, parity, COUNT(*) AS nb_culled FROM
                    (SELECT animal_id, holding_id, parity, last_calving_date FROM
                        (SELECT id_mere, parity, last_calving_date FROM
                            (SELECT id_mere, MIN({5}, calving_index) AS parity, MAX(calving_date) as last_calving_date FROM calving_date_and_parity GROUP BY id_mere)
                            WHERE last_calving_date >= DATE('{0}','+{1} days') AND last_calving_date <= DATE('{2}','+{1} days')) AS a
                    INNER JOIN
                        (SELECT animal_id, holding_id FROM detentions WHERE SUBSTR(date_of_entry,1,7) <= '{3}' AND SUBSTR(date_of_exit,1,7) >= '{3}' AND animal_id IN
                            (SELECT animal_id FROM bovins WHERE code_race IN {4} AND sexe = "2")) AS b
                    ON a.id_mere = b.animal_id) AS c
                INNER JOIN
                    (SELECT animal_id FROM date_of_death) AS d
                ON c.animal_id = d.animal_id
                GROUP BY holding_id, parity
            '''.format(firstDay, dur_G, lastDay, month, lBreed, paritiesNb)
            dfCulledFemales = pd.read_sql_query(query, iConnector)
            if dfCulledFemales.empty:
                raise Exception("empty query {}".format(query))
            #drop unknown values
            dfAllFemales = dfAllFemales[dfAllFemales.holding_id != "undetermined"]
            dfCulledFemales = dfCulledFemales[dfCulledFemales.holding_id != "undetermined"]
            #merge nb_females & nb_culled according to holding_id & parity
            dfAllFemales = dfAllFemales.set_index(["holding_id", "parity"])
            dfCulledFemales = dfCulledFemales.set_index(["holding_id", "parity"])
            dfAll = dfAllFemales.join(dfCulledFemales, how="outer").reset_index()
            #per parity
            for parity in range(paritiesNb):
                df = dfAll[dfAll.parity == parity].drop(columns=["parity"]).set_index("holding_id")
                #compute proportion
                df.nb_culled = df.nb_culled.fillna(0)
                df["proportion"] = df.nb_culled / df.nb_females
                #split the 3 outputs (nb_females, nb_culled, proportion), rename the only remaining column with the current month and merge with the global DataFrame (per output)
                dfFemNb = df.drop(columns=["nb_culled", "proportion"]).rename(columns={"nb_females": month})
                lFemNbPerParity[parity] = lFemNbPerParity[parity].join(dfFemNb, how="outer")
                dfCulledNb = df.drop(columns=["nb_females", "proportion"]).rename(columns={"nb_culled": month})
                lCullNbPerParity[parity] = lCullNbPerParity[parity].join(dfCulledNb, how="outer")
                dfProp = df.drop(columns=["nb_females", "nb_culled"]).rename(columns={"proportion": month})
                lPropPerParity[parity] = lPropPerParity[parity].join(dfProp, how="outer")
    #write tables in files
    for parity in range(paritiesNb):
        lFemNbPerParity[parity].to_csv("{}femNbNoMoreCalv_parity{}_{}_{}_{}.csv".format(resultsPath, parity, breed, firstMonth, lastMonth))
        lCullNbPerParity[parity].to_csv("{}culledFemNb_parity{}_{}_{}_{}.csv".format(resultsPath, parity, breed, firstMonth, lastMonth))
        lPropPerParity[parity].to_csv("{}culledFemProp_parity{}_{}_{}_{}.csv".format(resultsPath, parity, breed, firstMonth, lastMonth))

before = time.time()
computeSexRatio(lAllBreed, "all")
print("sex ratio in {:.0f} min".format((time.time() - before) / 60))
before = time.time()
computeDurFadult(lDairyBreed, "dairy")
computeDurFadult(lBeefBreed, "beef")
print("dur Fadult in {:.0f} min".format((time.time() - before) / 60))
before = time.time()
computeDurNG(lDairyBreed, "dairy")
computeDurNG(lBeefBreed, "beef")
print("dur NG in {:.0f} min".format((time.time() - before) / 60))
before = time.time()
computeDurJ(lDairyBreed, "dairy")
computeDurJ(lBeefBreed, "beef")
print("dur J in {:.0f} min".format((time.time() - before) / 60))
before = time.time()
computeBreedingFemalesProp(lDairyBreed, "dairy")
computeBreedingFemalesProp(lBeefBreed, "beef")
print("prop. of fem. kept for breeding in {:.0f} min".format((time.time() - before) / 60))
before = time.time()
computeYoungDeadProp(lDairyBreed, "dairy")
computeYoungDeadProp(lBeefBreed, "beef")
print("prop. of young animals dead in {:.0f} min".format((time.time() - before) / 60))
#WARNING: this last parameter could be very long to compute
before = time.time()
computeCulledFemalesProp(lDairyBreed, "dairy")
computeCulledFemalesProp(lBeefBreed, "beef")
print("prop. of culled females in {:.0f} min".format((time.time() - before) / 60))
