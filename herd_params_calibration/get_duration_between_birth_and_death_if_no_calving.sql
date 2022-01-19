-- get the mean duration between birth and death for animals that did not calve, per sex, per holding et per month (of birth)

-- Breeds considered as dairy: "12", "15", "18", "19", "21", "26", "29", "31", "35", "42", "43", "46", "56", "57", "63", "65", "66", "69", "74", "81", "82", "92", "44", "39"
-- Breeds considered as beef: "10", "14", "17", "20", "23", "24", "25", "30", "32", "33", "34", "36", "37", "38", "41", "45", "49", "52", "53", "54", "55", "58", "61", "71", "72", "73", "75", "76", "77", "78", "79", "85", "86", "88", "90", "93", "95", "97", "51", "48"

.mode csv
.header on

--dairy
.output results/duration_between_birth_and_death_if_no_calving_dairy_by_holding.csv

SELECT holding_of_birth, sexe, SUBSTR(dat_naiss,1,7) AS period, AVG(age_at_death) AS mean_age_at_death, COUNT(*) AS nb_animals FROM
    (
    SELECT b.holding_of_birth, a.animal_id, b.sexe, b.code_race, b.dat_naiss, (JULIANDAY(a.date_of_death) - JULIANDAY(b.dat_naiss)) AS age_at_death FROM
        (SELECT * FROM date_of_death WHERE animal_id NOT IN (SELECT id_mere FROM calving_date_and_parity)) AS a
    INNER JOIN
        (SELECT * FROM bovins WHERE holding_of_birth NOT IN ("NULLNULL", "FRNULL")) AS b
    ON a.animal_id = b.animal_id
    )
WHERE code_race IN ("12", "15", "18", "19", "21", "26", "29", "31", "35", "42", "43", "46", "56", "57", "63", "65", "66", "69", "74", "81", "82", "92", "44", "39") AND age_at_death > 21
GROUP BY holding_of_birth, sexe, period;

--beef
.output results/duration_between_birth_and_death_if_no_calving_beef_by_holding.csv

SELECT holding_of_birth, sexe, SUBSTR(dat_naiss,1,7) AS period, AVG(age_at_death) AS mean_age_at_death, COUNT(*) AS nb_animals FROM
    (
    SELECT b.holding_of_birth, a.animal_id, b.sexe, b.code_race, b.dat_naiss, (JULIANDAY(a.date_of_death) - JULIANDAY(b.dat_naiss)) AS age_at_death FROM
        (SELECT * FROM date_of_death WHERE animal_id NOT IN (SELECT id_mere FROM calving_date_and_parity)) AS a
    INNER JOIN
        (SELECT * FROM bovins WHERE holding_of_birth NOT IN ("NULLNULL", "FRNULL")) AS b
    ON a.animal_id = b.animal_id
    )
WHERE code_race IN ("10", "14", "17", "20", "23", "24", "25", "30", "32", "33", "34", "36", "37", "38", "41", "45", "49", "52", "53", "54", "55", "58", "61", "71", "72", "73", "75", "76", "77", "78", "79", "85", "86", "88", "90", "93", "95", "97", "51", "48") AND age_at_death > 21
GROUP BY holding_of_birth, sexe, period;
