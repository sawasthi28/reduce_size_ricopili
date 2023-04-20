#!/usr/bin/env python
"""
This script will reduce the size of ricopili directories. Please note that it will DELETE some of the files from the directories. See this document for details: https://docs.google.com/document/d/1ixnzlOzb-x92PBam769nB5uMh3UAcaDDSoEZG36z6xc/edit#
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
from pathlib import Path

parser=argparse.ArgumentParser()
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--dir', default=None, type=str, help="full path of the directory work on \"tmp_report_*\" and \"cobg_dir_genome_wide\"", required=True)
parser.add_argument('--delfiles', action='store_true', help="required if you want to delete files with size 0")

class Logger(object):
    TODAY_YMD = '_'.join(datetime.datetime.today().strftime("%c").split())
    def __init__(self, fh):
        self.log_fh = open(fh, 'a')
    def log(self, line):
        self.log_fh.write(f"{self.TODAY_YMD}\t{line}\n")
    
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
                logger_object.log(f"{file_path}\tThis file was deleted!")
            elif file_type==1:
                shutil.rmtree(file_path)
                logger_object.log(f"{file_path}\tThis directroy was deleted!")
            elif file_type==0:
                logger_object.log(f"{file_path}\tThis is a symlink so NOT deleted!")
        except FileNotFoundError:
            logger_object.log(f"{file_path}\tThis file was not found!")

def compress_files(file_paths):
    compressed_paths=[]
    for file_path in file_paths:
        file_type=check_file_type(file_path)
        if file_type==1:
            with tarfile.open(f"{file_path}.tar.gz", "w:gz") as tar:
                tar.add(path, arcname=os.path.basename(file_path))
            compressed_paths.append(f"{file_path}.tar.gz")
            logger_object.log(f"{file_path}\tThis file was successfully compressed!")

        elif file_type==2:
            with open(file_path, "rb") as f_in:
                with gzip.open(f"{file_path}.gz", "wb") as f_out:
                    f_out.write(f_in.read())
            compressed_paths.append(f"{file_path}.gz")
            logger_object.log(f"{file_path}\tThis file was successfully compressed!")
        elif file_type==0:
            logger_object.log(f"{file_path}\tNOT compressed! This is a symlink!")
        else:
            logger_object.log(f"{file_path}\tNOT compressed! Uunknown filetype!")
    return compressed_paths

def validate_compress_files(compressed_paths):
    validation_results=[True]
    for file_path in compressed_paths:
        if file_path.endswith('.tar.gz'):
            # Validate .tar.gz file
            try:
                subprocess.check_call(['tar', 'tzf', file_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                logger_object.log(f"{file_path}\tValidation of zipped file was Successfull")
            except subprocess.CalledProcessError:
                logger_object.log(f"{file_path}\tValidation of this zipped file Failed!")
                validation_results.append(False)
        elif file_path.endswith('.gz'):
            # Validate .gz file
            try:
                subprocess.check_call(['gunzip', '-t', file_path])
                logger_object.log(f"{file_path}\tValidation of zipped file was Successfull!")
            except subprocess.CalledProcessError:
                logger_object.log(f"{file_path}\tValidation of zipped file was Failed!")
                validation_results.append(False)
        else:
            # Invalid file type
            logger_object.log(f"{file_path}\tValidation failed: Unknown filetype!")
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
    patern_to_compress=['bgn.fam','bgn.bed','bgn.bim', '.bg.bim','bgs.bim']
    files_to_compress=[]
    path = str(path)
    for d in glob.glob(f"{path}/*"):
        if (d[-7:]) in patern_to_delete:
            files_to_delete.append(d)
        if (d[-7:] in patern_to_compress):
            files_to_compress.append(d)
    if not files_to_compress:
        logger_object.log(f"{path}\tNothing to compress or delete!")
    else:
        compressed_paths=compress_files(files_to_compress)
        validation_results=validate_compress_files(compressed_paths)
        
        if all(validation_results):
            delete_files(files_to_delete)
            delete_files(files_to_compress)
            logger_object.log(f"{path}\tFINISHED!!!")
        else:
            logger_object.log(f"{path}\tFAILED!!!")

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
        logger_object.log(f"{path}\tFINISHED!!!")
    else:
        logger_object.log(f"\t{path}\tFAILED!!!")

if __name__ == '__main__':
    logger_object = Logger('reduce_size.v1.log')
    args=parser.parse_args()
    path=Path(args.dir)
    logger_object.log(f"{path}\tScript started!")
    rest_path, last_dir=get_last_dir_and_rest(path)
    try:
        if check_file_type(path)!=1:
            logger_object.log(f"{path}\tEXITING: This is NOT a directroy or this directroy doesn't EXIST!")
            exit()
        if last_dir=="cobg_dir_genome_wide":
            reduce_cobg_dir(path)
            if args.delfiles:
                empty_files=find_empty_files(path)
                delete_files(empty_files)
        elif last_dir[0:11]=="tmp_report_":
            reduce_tmp_report(path)
            if args.delfiles:
                empty_files=find_empty_files(path)
                delete_files(empty_files)
        elif args.delfiles:
            empty_files=find_empty_files(path)
            delete_files(empty_files)
        else:
            logger_object.log(f"{path}\tEXITING: Not a cobg_dir_genome_wide or tmp_report_ directroy!")
    except Exception as e:
        logger_object.log(f"{path}\tERROR:{e}")
