import pandas as pd
import numpy as np
import calendar
from glob import glob

resultsPath = "results/"
outputFileName = "{}params.csv".format(resultsPath)
separator = ","
#wanted french department:
dep = "FR71"
#wanted period:
firstMonth = "2013-01"
lastMonth = "2013-05"
lMonths = pd.date_range(firstMonth, lastMonth, freq="MS").strftime("%Y-%m").tolist()
periodInFileNames = "{}_2013-06".format(firstMonth)
#periodInFileNames = "{}_{}".format(firstMonth, lastMonth)
paritiesNb = 6


print("Load parameters computed from computeParameters.py and select period")
lHoldingSets = []
def loadParameters(breed, lHoldingSets):
    d = {}
    df = pd.read_csv("{}propOfBreedingFem_{}_{}.csv".format(resultsPath, breed, periodInFileNames), index_col="holding_of_birth")
    d["p_female_kept_for_breeding"] = df[lMonths]
    lHoldingSets.append(set(df.index))
    df = pd.read_csv("{}durJ_{}_{}.csv".format(resultsPath, breed, periodInFileNames), index_col="holding_of_birth")
    d["dur_J"] = df[lMonths]
    lHoldingSets.append(set(df.index))
    df = pd.read_csv("{}durNG_{}_{}.csv".format(resultsPath, breed, periodInFileNames), index_col="holding_of_birth")
    d["dur_NG"] = df[lMonths]
    lHoldingSets.append(set(df.index))
    d["p_cull_P"] = []
    for i in range(paritiesNb):
        df = pd.read_csv("{}culledFemProp_parity{}_{}_{}.csv".format(resultsPath, i, breed, periodInFileNames), index_col="holding_id")
        d["p_cull_P"].append(df[lMonths])
        lHoldingSets.append(set(df.index))
#   duration as a "fattened juvenile" is a random value among mean duration per holding (per breed, per month, per sex), and not a parameter computed from computeParameters.py. See get_duration_between_birth_and_death_if_no_calving.sql
    df = pd.read_csv("{}duration_between_birth_and_death_if_no_calving_{}_by_holding.csv".format(resultsPath, breed))
    df = df[df.period.isin(lMonths)]
    d["Male_dur_Fbirth"] = df[df.sexe == 1].groupby(["period"])
    d["Fem_dur_Fbirth"] = df[df.sexe == 2].groupby(["period"])
    d["dur_Fadult"] = pd.read_csv("{}durFadult_{}_{}.csv".format(resultsPath, breed, periodInFileNames))[lMonths]   #not per herd, so there should be no NaN
    df = pd.read_csv("{}propOfYoungDead_{}_{}.csv".format(resultsPath, breed, periodInFileNames), index_col="holding_of_birth")
    d["p_mor"] = df[lMonths]
    lHoldingSets.append(set(df.index))
    return d
dParamForDairy = loadParameters("dairy", lHoldingSets)
dParamForBeef = loadParameters("beef", lHoldingSets)
dfSexRatio = pd.read_csv("{}sexRatio_all_{}.csv".format(resultsPath, periodInFileNames), index_col="holding_of_birth")
dfSexRatio.columns = dfSexRatio.columns.str.replace("-01-01$", "")  #or rstrip()...


print("Select holdings according to department")
sAllHoldingIds = set.union(*lHoldingSets)
lHoldingIds = []
for holdingId in sAllHoldingIds:
    if dep in holdingId:
        lHoldingIds.append(holdingId)
sSelectedHoldingIds = set(lHoldingIds)


print("Fill missing values")
def addMissingHoldingsAndFillNaN(df, param):
    sHoldingIds = set(df.index)
    sMissingHoldings = sSelectedHoldingIds - sHoldingIds
    if len(sMissingHoldings) != 0:
        df = df.append(pd.DataFrame([], index=sMissingHoldings, columns=df.columns))
    if param in ["dur_J", "dur_NG"]:    #duration as a "juvenile" (not fattened) and duration as a "non-gestating" cow, if no value, then:
        df = df.apply(lambda row: row.fillna(row.mean()), axis=1)   # mean per holding over the period
        df = df.fillna(df.mean())                                   # if holding with no value over the period, then mean per month over all holdings
        df = df.fillna(100)     #ONLY FOR THE DUMMY DATA SET (incomplete data, so no dur_NG found)
    elif "dur_Fadult" in param:         #duration as a fattened adult, if no value, then:
        df = df.apply(lambda row: row.fillna(row.mean()), axis=1)   # mean over the period
    elif param == "sexRatio":           #sex ratio: if no value, then 0.49 which is the mean over all holdings over 2005-2015 (Brittany)
        df = df.fillna(0.49)
    elif "p_" in param:                 #for all proportions: if no value, then 0, because EMULSION can't run with "NA" (Not Available) values
        df = df.fillna(0)
    else:
        raise Exception("Unrecognized param: " + param)
    return df
for param in ["p_female_kept_for_breeding", "dur_J", "dur_NG", "p_mor", "dur_Fadult"]:
    dParamForDairy[param] = addMissingHoldingsAndFillNaN(dParamForDairy[param], param)
    dParamForBeef[param] = addMissingHoldingsAndFillNaN(dParamForBeef[param], param)
for parity in range(paritiesNb):
    dParamForDairy["p_cull_P"][parity] = addMissingHoldingsAndFillNaN(dParamForDairy["p_cull_P"][parity], "p_cull")
    dParamForBeef["p_cull_P"][parity] = addMissingHoldingsAndFillNaN(dParamForBeef["p_cull_P"][parity], "p_cull")
dfSexRatio = addMissingHoldingsAndFillNaN(dfSexRatio, "sexRatio")


print("Write input file of EMULSION model:", outputFileName)
lParameters = ["population_id", "step", "sex_ratio", "p_female_kept_for_breeding_D", "p_female_kept_for_breeding_B", "dairy_dur_J", "dairy_dur_NG", "beef_dur_J", "beef_dur_NG", "p_cull_P0_D", "p_cull_P1_D", "p_cull_P2_D", "p_cull_P3_D", "p_cull_P4_D", "p_cull_P5_D", "p_cull_P0_B", "p_cull_P1_B", "p_cull_P2_B", "p_cull_P3_B", "p_cull_P4_B", "p_cull_P5_B", "dairy_Male_dur_Fbirth", "dairy_Female_dur_Fbirth", "beef_Male_dur_Fbirth", "beef_Female_dur_Fbirth", "dairy_dur_Fadult", "beef_dur_Fadult", "p_mor_D", "p_mor_B"]
with open(outputFileName, "w") as f:
    f.write(separator.join(lParameters) + "\n")
    for holdingId in sSelectedHoldingIds:
        llParamsPerDay = []
        for i, month in enumerate(lMonths):
            l = [holdingId, i, dfSexRatio.loc[holdingId][month],
                               round(dParamForDairy["p_female_kept_for_breeding"].loc[holdingId][month], 3),
                               round(dParamForBeef["p_female_kept_for_breeding"].loc[holdingId][month], 3),
                               int(dParamForDairy["dur_J"].loc[holdingId][month]) if not np.isnan(dParamForDairy["dur_J"].loc[holdingId][month]) else np.nan,
                               int(dParamForDairy["dur_NG"].loc[holdingId][month]) if not np.isnan(dParamForDairy["dur_NG"].loc[holdingId][month]) else np.nan,
                               int(dParamForBeef["dur_J"].loc[holdingId][month]) if not np.isnan(dParamForBeef["dur_J"].loc[holdingId][month]) else np.nan,
                               int(dParamForBeef["dur_NG"].loc[holdingId][month]) if not np.isnan(dParamForBeef["dur_NG"].loc[holdingId][month]) else np.nan,
                               *[round(df.loc[holdingId][month], 3) for df in dParamForDairy["p_cull_P"]],
                               *[round(df.loc[holdingId][month], 3) for df in dParamForBeef["p_cull_P"]],
                               int(dParamForDairy["Male_dur_Fbirth"].get_group(month)["mean_age_at_death"].sample(1)),
                               int(dParamForDairy["Fem_dur_Fbirth"].get_group(month)["mean_age_at_death"].sample(1)),
                               int(dParamForBeef["Male_dur_Fbirth"].get_group(month)["mean_age_at_death"].sample(1)),
                               int(dParamForBeef["Fem_dur_Fbirth"].get_group(month)["mean_age_at_death"].sample(1)),
                               int(dParamForDairy["dur_Fadult"].loc[0][month]),
                               int(dParamForBeef["dur_Fadult"].loc[0][month]),
                               round(dParamForDairy["p_mor"].loc[holdingId][month], 3),
                               round(dParamForBeef["p_mor"].loc[holdingId][month], 3),
                               ]
            for _ in range(calendar.monthrange(int(month[0:4]), int(month[5:7]))[1]):
                llParamsPerDay.append(l)
        for i, l in enumerate(llParamsPerDay[::7]):
            l[1] = i
            f.write(separator.join(map(str, l)) + "\n")
