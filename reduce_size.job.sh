#!/bin/bash
#Set job requirements
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=shared
#SBATCH --time=01:00:00
 
python3 /home/pgca1mdd/reduce_size/reduce_size_ricopili/reduce_size.v1.py --dir /home/pgca1mdd/GSRD/ --all_actions
