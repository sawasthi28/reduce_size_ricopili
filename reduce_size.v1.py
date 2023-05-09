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

def get_last_dir_and_rest(path):
    rest_path, last_dir = os.path.split(path)
    return rest_path, last_dir

def write_logs(line):
    logger_object = Logger(f'{global_path}/reduce_size.all.logs')
    logger_object.log(f"{line}")


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

def delete_files(files_to_delete, outlog):
    for file_path in files_to_delete:
        try:
            file_type=check_file_type(file_path)
            if file_type==2:
                os.remove(file_path)
                write_logs(f"Deleted_{outlog}\t{file_path}")
            elif file_type==1:
                shutil.rmtree(file_path)
                write_logs(f"Deleted_{outlog}\t{file_path}")
            elif file_type==0:
                write_logs(f"NotDeleted_SymLink_{outlog}\t{file_path}")
        except FileNotFoundError:
            write_logs(f"NotDeleted_NotFound_{outlog}\t{file_path}")

def compress_files(file_paths, outlog):
    compressed_paths=[]
    for file_path in file_paths:
        file_type=check_file_type(file_path)
        if file_type==1:
            with tarfile.open(f"{file_path}.tar.gz", "w:gz") as tar:
                tar.add(file_path, arcname=os.path.basename(file_path))
            compressed_paths.append(f"{file_path}.tar.gz")
            write_logs(f"Compressed_{outlog}\t{file_path}.tar.gz")

        elif file_type==2:
            subprocess.run(["gzip", "-f", "-c", file_path], stdout=open(f"{file_path}.gz", "wb"))
            compressed_paths.append(f"{file_path}.gz")
            write_logs(f"Compressed_{outlog}\t{file_path}.gz")
        elif file_type==0:
            write_logs(f"NotCompressed_SymLink_{outlog}\t{file_path}")
        else:
            write_logs(f"NotCompressed_NotFound_{outlog}\t{file_path}")
    return compressed_paths

def validate_compress_files(compressed_paths, outlog):
    validation_results=[True]
    for file_path in compressed_paths:
        if file_path.endswith('.tar.gz'):
            # Validate .tar.gz file
            try:
                subprocess.check_call(['tar', 'tzf', file_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                write_logs(f"Validated_{outlog}\t{file_path}")
            except subprocess.CalledProcessError:
                write_logs(f"ValidationFailed_{outlog}\t{file_path}")
                validation_results.append(False)
        elif file_path.endswith('.gz'):
            # Validate .gz file
            try:
                subprocess.check_call(['gunzip', '-t', file_path])
                write_logs(f"Validated_{outlog}\t{file_path}")
            except subprocess.CalledProcessError:
                write_logs(f"ValidationFailed_{outlog}\t{file_path}")
                validation_results.append(False)
        else:
            # Invalid file type
            write_logs(f"ValidationFailed_{outlog}\t{file_path}")
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

def compress_validate_delete(path, outlog):
    compressed_paths=compress_files(path, outlog)
    validation_results=validate_compress_files(compressed_paths, outlog)
    if all(validation_results):
        delete_files(path, outlog)
    else:
        write_logs(f"NotDeleting_{outlog}")

def reduce_cobg_dir(path, outlog):
    patern_to_delete=['.bg.fam','.bg.bed','bgs.fam','bgs.bed']
    files_to_delete=[]
    patern_to_compress=['.bg.bim','bgs.bim']
    files_to_compress=[]
    path = str(path)
    for d in glob.glob(f"{path}/*"):
        if (d[-7:]) in patern_to_delete:
            files_to_delete.append(d)
        if (d[-7:] in patern_to_compress):
            files_to_compress.append(d)

    if files_to_compress:
        compressed_paths=compress_files(files_to_compress, outlog)
        validation_results=validate_compress_files(compressed_paths, outlog)
        
        if all(validation_results):
            delete_files(files_to_delete, outlog)
            delete_files(files_to_compress, outlog)
        else:
            write_logs(f"NotDeleting_{outlog}")

def reduce_tmp_report(path, outlog):
    path = str(path)
    patern_to_delete=['.fam','.bim','.bed'] 
    files_to_delete=[]
    for d in glob.glob(f"{path}/*"):
        if (d[-4:]) in patern_to_delete:
            files_to_delete.append(d)

    delete_files(files_to_delete, outlog)
    compress_validate_delete([path], outlog)

def reduce_pcaer_sub(path, outlog):
    path =str(path)
    patern_to_delete=['.menv.mds.asso-nup.pdf.gz']
    files_to_delete=[]
    for d in glob.glob(f"{path}/*"):
        if (d[-25:]) in patern_to_delete:
            files_to_delete.append(d)
    
    delete_files(files_to_delete, outlog)
    compress_validate_delete([path], outlog)

def remove_zero_files(path, outlog):
    empty_files=find_empty_files(path)
    nfiles=len(empty_files)
    if nfiles > 0:
        delete_files(empty_files,outlog)

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
            write_logs("Not-A-Directory")
            exit()

        if check_file_type(path)==1:
            if args.allfunctions:
                cob_path=        find_files_or_dirs(path, "^cobg_dir_genome_wide$")
                tmp_path=        find_files_or_dirs(path, r"^tmp_report_.*\d$")
                errandout_path=  find_files_or_dirs(path, "^errandout$")
                dasuqc1_path=    find_files_or_dirs(path, r"^dasuqc1_.*hg19.ch.fl$")
                resdaner_path=   find_files_or_dirs(path, "^resdaner$")
                assoc_dos_path=  find_files_or_dirs(path, r"^dan_.*assoc.dosage.ngt.gz$")
                pca_qassoc_path= find_files_or_dirs(path, r".*.menv.assomds.*qassoc$")
                pca_assopdf_path=find_files_or_dirs(path, r".*.menv.mds.asso.pdf$")
                danscore_path   =find_files_or_dirs(path, r"^danscore_")
                report_path     =find_files_or_dirs(path, r"^report_")
                ###Delete all errandout directories 
                if len(errandout_path)>0:
                    try:
                        delete_files(errandout_path, "errandout")
                    except Exception as e:
                        write_logs(f"ERROR_errandout_{e}")

                ###Delete a files with size 0 excludes symlinks
                try:
                    remove_zero_files(path, "size-zero")
                except Exception as e:
                    write_logs(f"ERROR_size-zero_{e}")

                ### compress, validate and delete (CVD) dasuqc1_*/info directory 
                if len(dasuqc1_path)>0:
                    try:
                        dasuqc1_info_path = [f"{p}/info" for p in dasuqc1_path if check_file_type(p)==1]
                        compress_validate_delete(dasuqc1_info_path, "dasuqc1/info")
                    except Exception as e:
                        write_logs(f"ERROR_dasuqc1/info_{e}")
                
                ### delete files (*menv.mds.asso.pdf, *mds.asso-nup.pdf.gz and *menv.assomds*qassoc) from pcaer_*  and then CVD
                if len(pca_qassoc_path)>0:
                    try:
                        pcaer_sub_path = list(set([os.path.dirname(os.path.dirname(p)) for p in pca_qassoc_path]))
                        delete_files(pca_qassoc_path+pca_assopdf_path, "pcaer-sub")
                        for p in pcaer_sub_path:
                            reduce_pcaer_sub(p, "pcaer-sub")
                    except Exception as e:
                        write_logs(f"ERROR_pcaer-sub_{e}")

                ### CVD resdaner directory
                if len(resdaner_path)>0:
                    try:
                        compress_validate_delete(resdaner_path, "resdaner")
                    except Exception as e:
                        write_logs(f"ERROR_resdaner_{e}")

                ### deleting *assoc.dosage.ngt.gz files and CVD daner_subdirs   
                if len(assoc_dos_path)>0: 
                    try:
                        daner_paths = list(set([os.path.dirname(os.path.dirname(p)) for p in assoc_dos_path]))
                        delete_files(assoc_dos_path, "daner-sub")
                        compress_validate_delete(daner_paths, "daner-sub")
                    except Exception as e:
                        write_logs(f"ERROR_daner-sub_{e}")
                
                ### tmp_report
                if len(tmp_path)>0:
                    try:
                        for p in tmp_path:
                            reduce_tmp_report(p, "tmp-report")
                    except Exception as e:
                        write_logs(f"ERROR_tmp-report_{e}")
                
                ### cobg_dir
                if len(cob_path)>0:
                    try:
                        for p in cob_path:
                            reduce_cobg_dir(p,"cobg-dir")
                    except Exception as e:
                        write_logs(f"ERROR_cobg-dir_{e}")
                ### danscore 
                if len(danscore_path)>0:
                    try:
                        compress_validate_delete(danscore_path, "danscore")
                    except Exception as e:
                        write_logs(f"ERROR_danscore_{e}")
                ### repor_sub
                if len(report_path)>0:
                    try:
                        compress_validate_delete(report_path, "report-sub")
                    except Exception as e:
                        write_logs(f"ERROR_danscore_{e}")
            
            elif args.delfiles:
                remove_zero_files(path, 'size-zero')
            
            elif last_dir=="cobg_dir_genome_wide":
                reduce_cobg_dir(path, 'cobg-dir')
            
            elif last_dir[0:11]=="tmp_report_":
                global_path=rest_path
                reduce_tmp_report(path, 'tmp-report')
            
            else:
                write_logs("Not-A-Valid-Directory")
    
    except Exception as e:
        write_logs(f"ERROR_{e}")
