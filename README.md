# reduce_size_ricopili
This script will reduce the size of RICOPILI directories. Please note that it will DELETE some of the files from the directories. See this document for details: https://docs.google.com/document/d/1pAKygwzU2gJehx0jFdZ8TdZmHDp4ZvuANBsvD0oIqAw/edit#

The script will run on Python 3.


usage: reduce_size.v1.py [-h] --dir DIR [--gen_clean] [--qc1_clean]
                         [--pca_clean] [--imp_clean] [--post_clean]
                         [--all_actions]


##To preform all the cleaning actions use --all_actions
```
./reduce_size.v1.py --dir /full/path/to/directory --all_actions
```
