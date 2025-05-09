# Raw CMS (Medicaid/Medicare) Harmonization

This repository is a companion repository to Synthetic-CMS. Once raw Medicare/Medicaid files have been parsed using the Synthetic-CMS workflow, harmonization is necessary to create a cohesive dataset across years for Medicare/Medicaid. 

This repository currently only contains the workflow for harmonization of Medicare data for 1999 - 2018 (Medicaid is WIP)

Harmonization of this data is necessary due to the following challenges: 
1. 1999-2010 data is not raw data, but rather pre-processed 
2. Medicare variables have changed over time in terms of naming convention, data type, and availability. 

The goal of this repository is to harmonize the Medicare files to produce 3 tables: 
1. Master beneficiary summary part d (mbsf_d) * only for 2011-2018
2. Inpatient summary (ip)
3. Patient summary (ps)

Across all years, these tables should contain the same variables, of the same data type, and same data structure/coding.

## Repository Contents 
- src/harmonize.py -> Builds and executes Duckdb queries based on specifications from medicare.yml. Creates parquet files (1 per table type, per year)
- utils/medicare.yml -> Outlines processing rules for all tables
- environment.yml -> requirements for conda environment 
- harmonize.sbatch -> slurm job .sbatch outline for facile execution of harmonization workflow

## Getting Started

 **Clone the repository:**

Clone the repository and create a conda environment.

   ```bash
   git clone <https://github.com/<user>/repo>
   cd <repo>

   conda env create -f environment.yml
   conda activate <env_name>
   ```

## Usage
Step 1:
- Edit the .sbatch script to select the years you want to cover, and the tables you want to create. Make sure to edit the sbatch array value (It should be = # of tables * # of years)

```bash
python sbatch harmonize.sbatch
```
Be mindful that each HPC clusters has a different configuration and the .sbatch files might need to be modified accordingly.

Step 2: Review the Output
- The output of the processing script will be saved in the output/ directory. Across all years, parquet files generated (for each table type - mbsf_d, ps, ip) should contain the same variables, of the same data type, and same data structure/coding.

