#!/bin/bash
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 2
#SBATCH --mem 96GB 
#SBATCH -t 2-00:00 

CONFIG_PATH="../harmonization-cms/utils/medicare.yml"
OUTPUT="../harmonization-cms/output" #currently in wd, will change once code is fully functional
TABLE="ps" #ps of mbsf_d for now, eventually this argument won't be necessary
YEAR=2011 # eventually, this argument won't be necessary

# Run Python script
python src/harmonize.py --config_path $CONFIG_PATH --output_path $OUTPUT --table_to_run $TABLE --year_to_run $YEAR