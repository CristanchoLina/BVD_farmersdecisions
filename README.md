# BVD_farmersdecisions


## Code BVD with vaccination decisions
The script bvd_with_vaccination_dynamics.ipynb used for simulating the model with the vaccination decision component.
The folder inf_beef_J contains all the (fictitious) data that is needed to run it.

## Herd-based parameters:

Herd_params_calibration contains:
- A (fictitious) database: data/dummy_dataset.db (3 herds over 1.5 years) 
- The scripts get_duration_between_birth_and_death_if_no_calving.sql, computeParameters.py and formatParameters.py, for computing and formating parameters
- The script main.sh that runs these scripts to generate the (fictitious) parameters: results/params.csv
