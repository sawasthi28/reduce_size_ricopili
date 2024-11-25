# reduce_size_ricopili
This script will reduce the size of RICOPILI directories. Please note that it will DELETE some of the files from the directories. See this document for details: https://docs.google.com/document/d/1PcDYJioa4iuNbk-CXvB2WZwKwt242EzYfqlLYti2LYM/edit?tab=t.0#heading=h.ssbz3raju240

The script will run on Python 3.


usage: reduce_size.v1.py [-h] --dir DIR [--gen_clean] [--qc1_clean]
                         [--pca_clean] [--imp_clean] [--post_clean]
                         [--all_actions]


## To preform all the cleaning actions use --all_actions
```
./reduce_size.v1.py --dir /full/path/to/directory --all_actions
```

## Two log files in --dir 

### reduce_size_*.all.logs
This log file records each action step taken by the script along with the modified file/directory with its timestamp.

### reduce_size_*.summary.logs
This log file summarizes the steps/actions into a one action unit and compare file number and disk size before and after cleaning. 
It also gives the sum total of file numbers and disk size before and after for each action units and also similar statistics for the complete  directroy (--dir). 

