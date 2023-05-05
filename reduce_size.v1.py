#!/usr/bin/env python
"""
This script will reduce the size of ricopili directories (such as tmp_report_* & cobg_dir_genome_wide) and could also delete all the files with size 0 recursively. Please note that it will DELETE some of the files from the directories. Please see this document for details: https://docs.google.com/document/d/1ixnzlOzb-x92PBam769nB5uMh3UAcaDDSoEZG36z6xc/edit#
"""
import argparse
import glob
import os
import subprocess
import datetime
import gzip
import tarfile
import shutil
import contextlib
import re
from pathlib import Path

parser=argparse.ArgumentParser()
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--dir', default=None, type=str, help="full path of the directory work on \"tmp_report_*\" and \"cobg_dir_genome_wide\"", required=True)
parser.add_argument('--delfiles', action='store_true', help="required if you want to delete files with size 0")
parser.add_argument('--allfunctions', action='store_true', help="required if you want to perform all the 3 tasks (find plus clean i.e. tmp_report*, cobg_dir* & delte files with size 0)")

class Logger(object):
    def __init__(self, fh):
        self.log_fh = open(fh, 'a')
    def log(self, line):
        TODAY_YMD = '_'.join(datetime.datetime.today().strftime("%c").split())
        self.log_fh.write(f"{TODAY_YMD}\t{line}\n")
    def log_sep(self):
        self.log_fh.write(f"{'_'*200}\n")

def get_last_dir_and_rest(path):
    rest_path, last_dir = os.path.split(path)
    return rest_path, last_dir

def write_logs(line, print_sep=None):
    logger_object = Logger(f'{global_path}/reduce_size.v1.log')
    if print_sep is None:
        logger_object.log(f"{path}\t{line}")
    else:
        logger_object.log_sep()

def get_last_dir_and_rest(path):
    rest_path, last_dir = os.path.split(path)
    return rest_path, last_dir

def check_file_type(path):
    if os.path.islink(path):
        return 0
    elif os.path.isdir(path):
        return 1
    elif os.path.isfile(path):
        return 2
    else:
        return 3 

def delete_files(files_to_delete):
    for file_path in files_to_delete:
        try:
            file_type=check_file_type(file_path)
            if file_type==2:
                os.remove(file_path)
                write_logs(f"This file was deleted! : {file_path}")
            elif file_type==1:
                shutil.rmtree(file_path)
                write_logs(f"This directroy was deleted! : {file_path}")
            elif file_type==0:
                write_logs(f"This is a symlink so NOT deleted! : {file_path}")
        except FileNotFoundError:
            write_logs(f"This file was not found! : {file_path}")

def compress_files(file_paths):
    compressed_paths=[]
    for file_path in file_paths:
        file_type=check_file_type(file_path)
        if file_type==1:
            with tarfile.open(f"{file_path}.tar.gz", "w:gz") as tar:
                tar.add(file_path, arcname=os.path.basename(file_path))
            compressed_paths.append(f"{file_path}.tar.gz")
            write_logs(f"This file was successfully compressed! : {file_path}.tar.gz")

        elif file_type==2:
            subprocess.run(["gzip", "-f", "-c", file_path], stdout=open(f"{file_path}.gz", "wb"))
            compressed_paths.append(f"{file_path}.gz")
            write_logs(f"This file was successfully compressed! : {file_path}.gz")
        elif file_type==0:
            write_logs(f"NOT compressed! This is a symlink! : {file_path}")
        else:
            write_logs(f"NOT compressed! Uunknown filetype! : {file_path}")
    return compressed_paths

def validate_compress_files(compressed_paths):
    validation_results=[True]
    for file_path in compressed_paths:
        if file_path.endswith('.tar.gz'):
            # Validate .tar.gz file
            try:
                subprocess.check_call(['tar', 'tzf', file_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                write_logs(f"Validation of zipped file was Successfull : {file_path}")
            except subprocess.CalledProcessError:
                write_logs(f"Validation of this zipped file Failed! : {file_path}")
                validation_results.append(False)
        elif file_path.endswith('.gz'):
            # Validate .gz file
            try:
                subprocess.check_call(['gunzip', '-t', file_path])
                write_logs(f"Validation of zipped file was Successfull! : {file_path}")
            except subprocess.CalledProcessError:
                write_logs(f"Validation of zipped file was Failed! : {file_path}")
                validation_results.append(False)
        else:
            # Invalid file type
            write_logs(f"Validation failed: Unknown filetype! : {file_path}")
            validation_results.append(False)
    return validation_results

def find_empty_files(path):
    empty_files=[]
    for root, dirs, files in os.walk(path):
        for filename in files:
            filepath = os.path.join(root, filename)
            if os.path.isfile(filepath):
                # Get the metadata of the file using os.stat()
                stat_info = os.stat(filepath)
                # Check if the file size is zero bytes
                if stat_info.st_size == 0:
                    empty_files.append(filepath)
    return empty_files

def reduce_cobg_dir(path):
    patern_to_delete=['.bg.fam','.bg.bed','bgs.fam','bgs.bed']
    files_to_delete=[]
    #patern_to_compress=['bgn.fam','bgn.bed','bgn.bim', '.bg.bim','bgs.bim']
    patern_to_compress=['.bg.bim','bgs.bim']
    files_to_compress=[]
    path = str(path)
    for d in glob.glob(f"{path}/*"):
        if (d[-7:]) in patern_to_delete:
            files_to_delete.append(d)
        if (d[-7:] in patern_to_compress):
            files_to_compress.append(d)
    if not files_to_compress:
        write_logs("Nothing to compress or delete!")
    else:
        compressed_paths=compress_files(files_to_compress)
        validation_results=validate_compress_files(compressed_paths)
        
        if all(validation_results):
            delete_files(files_to_delete)
            delete_files(files_to_compress)
            write_logs("FINISHED!!!")
        else:
            write_logs("FAILED!!!")

def reduce_tmp_report(path):
    path = str(path)
    patern_to_delete=['.fam','.bim','.bed'] 
    files_to_delete=[]
    files_to_compress=[path] 
    for d in glob.glob(f"{path}/*"):
        if (d[-4:]) in patern_to_delete:
            files_to_delete.append(d)

    delete_files(files_to_delete)
    compressed_paths=compress_files(files_to_compress)
    validation_results=validate_compress_files(compressed_paths)
    
    if all(validation_results):
        delete_files(files_to_compress)
        write_logs("FINISHED!!!")
    else:
        write_logs( "FAILED!!!")

def reduce_dasuqc1(path):
    path=[f"{path}/info"]
    compressed_paths=compress_files(path)
    validation_results=validate_compress_files(compressed_paths)
    if all(validataion_results):
        delelte_files(path)
        write_logs("FINISHED!!!")
    else:
        write_logs("FAILED!!!")

def remove_zero_files(path):
    empty_files=find_empty_files(path)
    nfiles=len(empty_files)
    if nfiles == 0:
        write_logs("EXITING: nothing to delete here")
    else:
        delete_files(empty_files)
        write_logs(f"Deleted {nfiles} file/s from this. FINISHED!!!")

def find_files_or_dirs(path, pattern):
    matching_paths = []
    for root, dirs, files in os.walk(path):
        for filename in files + dirs:
            if re.search(pattern, filename):
                matching_paths.append(os.path.join(root, filename))
    return matching_paths


if __name__ == '__main__':
    args=parser.parse_args()
    global global_path
    global_path=path=Path(args.dir)
    rest_path, last_dir=get_last_dir_and_rest(path)
    cwd = os.getcwd()
    try:
        if check_file_type(path)!=1:
            global_path=cwd
            write_logs("Script started!")
            write_logs("EXITING: This is NOT a directroy or this directroy doesn't EXIST!")
            exit()

        if check_file_type(path)==1:
            if args.allfunctions:
                write_logs("Script started!")
                cob_path=       find_files_or_dirs(path, "cobg_dir_genome_wide")
                tmp_path=       find_files_or_dirs(path, r"tmp_report_.*")
                errandout_path= find_files_or_dirs(path, "errandout")
                dasuqc1_path=   find_files_or_dirs(path, r"dasuqc1_.*")
                print(cob_path)
                print(tmp_path)
                print(errandout_path)
                print(dasuqc1_path)
                exit()
                ###Delete all errandout directories 
                try:
                    write_logs('_','_')
                    delete_files(errandout_path)
                except Exception as e:
                    write_logs(f"Error in deleting errandout directroy :{e}")
                ### dasuqc1
                try:
                    for p in dasuqc1_path:
                        write_logs('_','_')
                        reduce_dasuqc1(p)
                except Exception as e:
                    write_logs(f"ERROR in function reduce_dasuqc1 :{e}")
                ### tmp_report
                try:
                    for p in tmp_path:
                        write_logs('_','_')
                        reduce_tmp_report(p)
                except Exception as e:
                    write_logs(f"ERROR in function reduce_tmp_report :{e}")
                ### cobdg_
                try:
                    for p in cob_path:
                        write_logs('_','_')
                        reduce_cobg_dir(p)
                except Exception as e:
                    write_logs(f"ERROR in function reduce_cobg_dir :{e}")
                ###Delete a files with size 0 excludes symlinks
                try:
                    write_logs('_','_')
                    remove_zero_files(path)
                except Exception as e:
                    write_logs(f"ERROR in function remove_zero :{e}")
            
            elif args.delfiles:
                write_logs("Script started!")
                remove_zero_files(path)
            
            elif last_dir=="cobg_dir_genome_wide":
                write_logs("Script started!")
                reduce_cobg_dir(path)
            
            elif last_dir[0:11]=="tmp_report_":
                global_path=rest_path
                write_logs("Script started!")
                reduce_tmp_report(path)
            
            else:
                write_logs("Script started!")
                write_logs("EXITING: This is NOT a cobg_dir_genome_wide or tmp_report_ directroy!")
    
    except Exception as e:
        write_logs(f"ERROR:{e}")
