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
parser.add_argument('--allfunctions', action='store_true', help="required if you want to perform all the 3 tasks (find plus clean i.e. tmp_report*, cobg_dir* & delte files with size 0)")

class Logger(object):
    def __init__(self, fh):
        self.log_fh = open(fh, 'a')
    def log(self, line, summary=None):
        TODAY_YMD = '_'.join(datetime.datetime.today().strftime("%c").split())
        if summary:
            self.log_fh.write(f"{line}\n")
        else:
            self.log_fh.write(f"{TODAY_YMD}\t{line}\n")

def write_logs(line, summary=None):
    if summary:
        logger_object = Logger(f'{global_path}/reduce_size.summary.logs')
        logger_object.log(f"{line}","_")
    else:
        logger_object = Logger(f'{global_path}/reduce_size.all.logs')
        logger_object.log(f"{line}")

def count_files(path):
    files = os.listdir(path)
    return len(files)

def get_size(path):
    try:
        if os.path.isfile(path):
            return os.path.getsize(path)
        elif os.path.isdir(path):
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(file_path)
                    except FileNotFoundError:
                        pass
            return total_size
    except ValueError:
        write_logs(f"NotValid\t{path}")
        return 0

def find_files_or_dirs(path, pattern):
    matching_paths = []
    for root, dirs, files in os.walk(path):
        for filename in files + dirs:
            if re.search(pattern, filename):
                matching_paths.append(os.path.join(root, filename))
    return matching_paths

def compress_files(file_path, outlog):
    if os.path.islink(file_path):
        write_logs(f"NotCompressed_SymLink_{outlog}\t{file_path}")

    elif os.path.isdir(file_path):
        with tarfile.open(f"{file_path}.tar.gz", "w:gz") as tar:
            tar.add(file_path, arcname=os.path.basename(file_path))
        write_logs(f"Compressed_{outlog}\t{file_path}.tar.gz")
        return f"{file_path}.tar.gz"

    elif os.path.isfile(file_path):
        subprocess.run(["gzip", "-f", "-c", file_path], stdout=open(f"{file_path}.gz", "wb"))
        write_logs(f"Compressed_{outlog}\t{file_path}.gz")
        return f"{file_path}.gz"
    else:
        write_logs(f"NotCompressed_NotFound_{outlog}\t{file_path}")

def validate_compress_files(compressed_path, outlog):
    if compressed_path.endswith('.tar.gz'):
        # Validate .tar.gz file
        try:
            subprocess.check_call(['tar', 'tzf', compressed_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            write_logs(f"Validated_{outlog}\t{compressed_path}")
            return True
        except subprocess.CalledProcessError:
            write_logs(f"ValidationFailed_{outlog}\t{compressed_path}")
            return False
    
    elif compressed_path.endswith('.gz'):
         # Validate .gz file
        try:
            subprocess.check_call(['gunzip', '-t', compressed_path])
            write_logs(f"Validated_{outlog}\t{compressed_path}")
            return True
        except subprocess.CalledProcessError:
            write_logs(f"ValidationFailed_{outlog}\t{compressed_path}")
            return False
    
    else:
        # Invalid file type
        write_logs(f"ValidationFailed_{outlog}\t{compressed_path}")
        return False

def delete_files(file_to_delete, outlog):
    try:
        if os.path.islink(file_to_delete):
            write_logs(f"NotDeleted_SymLink_{outlog}\t{file_to_delete}")
        if os.path.isfile(file_to_delete):
            os.remove(file_to_delete)
            write_logs(f"Deleted_{outlog}\t{file_to_delete}")
        elif os.path.isdir(file_to_delete):
            shutil.rmtree(file_to_delete)
            write_logs(f"Deleted_{outlog}\t{file_to_delete}")
    except FileNotFoundError:
        write_logs(f"NotDeleted_NotFound_{outlog}\t{file_to_delete}")

def find_empty_files(path):
    empty_files=[]
    for root, dirs, files in os.walk(path):
        for filename in files:
            filepath = os.path.join(root, filename)
            if os.path.isfile(filepath):
                # Get the metadata of the file using os.stat()
                stat_info = os.stat(filepath)
                if stat_info.st_size == 0:
                    empty_files.append(filepath)
    return empty_files

def compress_validate_delete(path, outlog):
    compressed_path=compress_files(path, outlog)
    validation_result=validate_compress_files(compressed_path, outlog)
    if  validation_result:
        delete_files(path, outlog)
        return compressed_path
    else:
        write_logs(f"NotDeleting_{outlog}")

def reduce_errandout(path, outlog):
    for root, directories, files in os.walk(path):
        for filename in files:
            delete_files(os.path.join(root, filename), outlog)
    delete_files(path, outlog)

def reduce_cobg_dir(path, outlog):
    pattern_to_delete=['.bg.fam','.bg.bed','.bgs.fam','.bgs.bed']
    pattern_to_compress=['.bg.bim','.bgs.bim']
    ##
    for pattern in pattern_to_delete:
        matching_files=glob.glob(path + '/*' + pattern)
        for match_file in matching_files:
            delete_files(match_file, outlog)
    ##
    for pattern in pattern_to_compress:
        matching_files=glob.glob(path + '/*' + pattern)
        for match_file in matching_files:
            compress_path=compress_validate_delete(match_file, outlog)

def reduce_tmp_report(path, outlog):
    pattern_to_delete=['.fam','.bim','.bed'] 
    for pattern in pattern_to_delete:
        matching_files=glob.glob(path + '/*' + pattern)
        for match_file in matching_files:
            delete_files(match_file, outlog)
    compress_path=compress_validate_delete(path, outlog)
    return get_size(compress_path), 1

def reduce_pcaer_sub(path, outlog):
    files_to_delete= find_files_or_dirs(path, r".*.menv.assomds.*qassoc$")+\
            find_files_or_dirs(path, r".*.menv.mds.asso.pdf$")+ \
            find_files_or_dirs(path, r".*.menv.mds.asso-nup.pdf.gz$")

    for file in files_to_delete:
        delete_files(file, outlog)
    compress_path=compress_validate_delete(path, outlog)
    return get_size(compress_path), 1

def remove_zero_files(path, outlog):
    empty_files=find_empty_files(path)
    nfiles=len(empty_files)
    if nfiles > 0:
        for empty_file in empty_files:
            delete_files(empty_file, outlog)
    return nfiles

if __name__ == '__main__':
    args=parser.parse_args()
    global_path=path=Path(args.dir)
    
    try:
        if os.path.islink(path):
            print("This is a SymLink.")
            exit()

        elif os.path.isfile(path):
            print("This is a File.")
            exit()

        elif os.path.isdir(path):

            if args.allfunctions:
                ##removing all errandout directories
                try:
                    errandout_path=find_files_or_dirs(path, "^errandout$")
                    if len(errandout_path)>0:
                        for p in errandout_path:
                            size=get_size(p); count=count_files(p)
                            reduce_errandout(p, "errandout")
                            write_logs(f"Deleted_errandout\t{size}\t{count}\t0\t0\t{p}", "_")
                except Exception as e:
                    write_logs(f"ERROR_errandout_{e}")
                ##deleting all files with size 0
                try:
                    count=remove_zero_files(path, "size-zero")
                    if count>0:
                        write_logs(f"Deleted_size-0\t0\t{count}\t0\t0\tNA", "_")
                except Exception as e:
                    write_logs(f"ERROR_size-zero_{e}")
                ##reducing tmp_report directories
                try:
                    tmp_path=find_files_or_dirs(path, r"^tmp_report_.*\d$")
                    if len(tmp_path)>0:
                        for p in tmp_path:
                            size=get_size(p); count=count_files(p)
                            rsize, rcount=reduce_tmp_report(p, "tmp-report")
                            write_logs(f"Reduced-CVD_tmp-report\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")
                except Exception as e:
                    write_logs(f"ERROR_tmp-report_{e}")
               ##reducing cobg_dir  
                try:
                    cobg_path=find_files_or_dirs(path, "^cobg_dir_genome_wide$")
                    if len(cobg_path)>0:
                        for p in cobg_path:
                            size=get_size(p); count=count_files(p)
                            reduce_cobg_dir(p,"cobg-dir")
                            rsize=get_size(p); rcount=count_files(p)
                            if (size!=rsize) | (count!=rcount):
                                write_logs(f"Reduced_cobg-dir\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")
                except Exception as e:
                    write_logs(f"ERROR_cobg-dir_{e}")
                ## CVD dasuqc1_*/info
                try:
                    dasuqc1_path=find_files_or_dirs(path, r"^dasuqc1_.*hg19.ch.fl$")
                    dasuqc1_info_path = [f"{p}/info" for p in dasuqc1_path if os.path.isdir(p)]
                    if len(dasuqc1_info_path)>0:
                        for p in dasuqc1_info_path:
                            if os.path.isdir(p):
                                size=get_size(p); count=count_files(p)
                                compress_path=compress_validate_delete(p, "dasuqc1/info")
                                rsize=get_size(compress_path); rcount=1
                                write_logs(f"CVD_info\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")
                except Exception as e:
                    write_logs(f"ERROR_dasuqc1/info_{e}")
                ##
                try:
                    pca_qassoc_path= find_files_or_dirs(path, r".*.menv.assomds.*qassoc$")
                    pca_assopdf_path=find_files_or_dirs(path, r".*.menv.mds.asso.pdf$")
                    if (len(pca_qassoc_path)>0) | len(pca_assopdf_path)>0:
                        pcaer_sub_path = list(set([os.path.dirname(os.path.dirname(p)) for p in pca_qassoc_path]))
                        for p in pcaer_sub_path:
                            size=get_size(p); count=count_files(p)
                            rsize, rcount=reduce_pcaer_sub(p, "pcaer-sub")
                            write_logs(f"Reducedi-CVD_pacer-sub\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")
                except Exception as e:
                    write_logs(f"ERROR_pcaer-sub_{e}")
                ###
                try:
                    resdaner_path=find_files_or_dirs(path, "^resdaner$")
                    if len(resdaner_path)>0:
                        for p in resdaner_path:
                            size=get_size(p); count=count_files(p)
                            compress_path=compress_validate_delete(p, "resdaner")
                            rsize=get_size(compress_path); rcount=1
                            write_logs(f"CVD_resdaner\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")
                except Exception as e:
                     write_logs(f"ERROR_resdaner_{e}")
                ###
                try:
                    danscore_path=find_files_or_dirs(path, r"^danscore_")
                    if len(danscore_path)>0:
                        for p in danscore_path:
                            if os.path.isdir(p):
                                size=get_size(p); count=count_files(p)
                                compress_path=compress_validate_delete(p, "danscore")
                                rsize=get_size(compress_path); rcount=1
                                write_logs(f"CVD_danscore\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")
                except Exception as e:
                    write_logs(f"ERROR_danscore_{e}")
                ####
                try: 
                    report_path=find_files_or_dirs(path, r"^report_")
                    if len(report_path)>0:
                        for p in report_path:
                            if os.path.isdir(p):
                                size=get_size(p); count=count_files(p)
                                compress_path=compress_validate_delete(p, "report-sub")
                                rsize=get_size(compress_path); rcount=1
                                write_logs(f"CVD_report-sub\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")
                except Exception as e:
                    write_logs(f"ERROR_report-sub_{e}")
                
                ####
                try:
                    assoc_dos_path=find_files_or_dirs(path, r"^dan_.*assoc.dosage.ngt.gz$")
                    if len(assoc_dos_path)>0: 
                        daner_paths = list(set([os.path.dirname(os.path.dirname(p)) for p in assoc_dos_path]))
                        for p in daner_paths:
                            size=get_size(p); count=count_files(p)
                            assoc_paths=  find_files_or_dirs(p, r"^dan_.*assoc.dosage.ngt.gz$")
                            for assoc_path in assoc_paths:
                                delete_files(assoc_path, "daner-sub")
                            compress_path=compress_validate_delete(p, "daner-sub")
                            rsize=get_size(compress_path); rcount=1
                            write_logs(f"Reduced-CVD_daner-sub\t{size}\t{count}\t{rsize}\t{rcount}\t{p}", "_")

                except Exception as e:
                        write_logs(f"ERROR_daner-sub_{e}")
        else:
            print("UnKnown FileType.")
    
    except Exception as e:
        write_logs(f"ERROR_{e}")
