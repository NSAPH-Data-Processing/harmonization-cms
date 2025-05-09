#!/bin/bash
#SBATCH -p serial_requeue
#SBATCH -c 2
#SBATCH --mem 48GB
#SBATCH -t 2-00:00
#SBATCH --array=0-3

CONFIG_PATH="../harmonization-cms/utils/medicare.yml"
OUTPUT="../harmonization-cms/output"

TABLES=(mbsf_d ps ip)  # List of tables
# YEARS=(1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018)  # List of years
YEARS=(2003 2007 2008 2014)

# Calculate the table and year for the current job index

NUM_YEARS=${#YEARS[@]}
TABLE_INDEX=$((SLURM_ARRAY_TASK_ID / NUM_YEARS))  # Divide by the number of years
YEAR_INDEX=$((SLURM_ARRAY_TASK_ID % NUM_YEARS))

TABLE=${TABLES[$TABLE_INDEX]}
YEAR=${YEARS[$YEAR_INDEX]}

echo "Running for table $TABLE, year $YEAR"

# Run Python script
python src/harmonize.py --config_path $CONFIG_PATH --output_path $OUTPUT --table_to_run $TABLE --year_to_run $YEAR
