# BVD_farmersdecisions


## Code BVD with vaccination decisions
The script bvd_with_vaccination_dynamics.ipynb used for simulating the model with the vaccination decision component, and the folder infbeef_J which contains an example of data for running it:
- herd_params.csv (a database of herd-specific parameters)
- herds.csv (list of herds in the metapopulation)
- herd_status (optional initial infecitous status for some herds of the metapopulation)
- moves (defines the initial number of animals in each herd, and animal movements in the population throughout the perriod to simulate)
- neighbours (list of geographic neighbours, with distance, between herds of the metapopulation)
- neighbours (list of geographic neighbours, with distance, between herds of the metapopulation and herds outside the metapopulation)

## Herd-based parameters:

Herd_params_calibration contains:
- A (fictitious) database: data/dummy_dataset.db (3 herds over 1.5 years) 
- The scripts get_duration_between_birth_and_death_if_no_calving.sql, computeParameters.py and formatParameters.py, for computing and formating parameters
- The script main.sh that runs these scripts to generate the (fictitious) parameters: results/params.csv
