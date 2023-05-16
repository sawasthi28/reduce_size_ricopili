#!/usr/bin/env python
"""
This script will reduce (number of files and size of) the size of ricopili directories by deleting some files/directoires and compressing some files/directories. Please see this document for details: https://docs.google.com/document/d/1pAKygwzU2gJehx0jFdZ8TdZmHDp4ZvuANBsvD0oIqAw/edit#
"""
import argparse
import glob
import os
import subprocess
import datetime
import time
import gzip
import tarfile
import shutil
import contextlib
import re
import math
from pathlib import Path

parser=argparse.ArgumentParser()
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--dir', default=None, type=str, help="full path to the directory from where you want to start the script recursively", required=True)
parser.add_argument('--allfunctions', action='store_true', help="is required if you want to perform all the functions to reduce (number of files and size of) ricopili directories")
filestamp=datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

class Logger(object):
    def __init__(self, fh):
        self.log_fh = open(fh, 'a')
    def log(self, line, summary=None):
        TODAY_YMD = '_'.join(datetime.datetime.today().strftime("%c").split())
        if summary=="_":
            self.log_fh.write(f"{line}\n")
        elif summary=="0":
            self.log_fh.write(f"##{line}_{TODAY_YMD}\n")
        elif summary=="1":
            self.log_fh.write(f"##{line}\n")
        else:
            self.log_fh.write(f"{TODAY_YMD}\t{line}\n")

def write_logs(line, summary=None):
    if summary=="_":
        logger_object = Logger(f'{global_path}/reduce_size_{filestamp}.summary.logs')
        logger_object.log(f"{line}","_")
    elif summary=="0":
        logger_object = Logger(f'{global_path}/reduce_size_{filestamp}.all.logs')
        logger_object.log(f"{line}","0")
    elif summary=="1":
        logger_object = Logger(f'{global_path}/reduce_size_{filestamp}.all.logs')
        logger_object.log(f"{line}","1")
    else:
        logger_object = Logger(f'{global_path}/reduce_size_{filestamp}.all.logs')
        logger_object.log(f"{line}")


def convert_bytes(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s%s" % (s, size_name[i])

def count_files(path):
    if os.path.islink(path):
        return 0
    elif os.path.isdir(path):
        count = 0
        for root, dirs, files in os.walk(path):
            for file in files:
                file=os.path.join(root, file)
                if os.path.isfile(file):
                    count += 1
        return count
    elif os.path.isfile(path):
        return 1
    else:
        return 0

def get_size(path):
    try:
        if os.path.isfile(path):
            size=os.path.getsize(path)
            return size

        elif os.path.isdir(path):
            total_size = 0
            for root, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    file_path = os.path.join(root, filename)
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

def reduce_dameta(path, outlog):
    files_to_delete= find_files_or_dirs(path, r".*metadaner.gz$")
    for file in files_to_delete:
        delete_files(file, outlog)
    compress_path=compress_validate_delete(path, outlog)
    return get_size(compress_path), 1

def reduce_report_sub(path , outlog):
    files_to_delete= find_files_or_dirs(path, r"^daner.*meta.gz$")+\
            find_files_or_dirs(path, r"^daner.*het.gz$")
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

def reduce_dasuqc1(path, outlog):
    status=0
    if os.path.isdir(path):
        ##
        for sub_dir in ['bg', 'bgs', 'bgn']:
            sub_dir_path = os.path.join(path, sub_dir)
            if os.path.isdir(sub_dir_path):
                delete_files(sub_dir_path, f"{outlog}/{sub_dir}")
                status+=1
        ##
        sub_dir_qc1f=os.path.join(path, "qc1f")
        patterns=["*.ngt"] # not to delete 
        if os.path.isdir(sub_dir_qc1f):
            files_to_delete = [file for file in glob.glob(os.path.join(sub_dir_qc1f, '*')) if not any(pattern in file for pattern in patterns)]
            for file in files_to_delete:
                delete_files(file, f"{outlog}/qc1f")
                status+=1
            compress_validate_delete(sub_dir_qc1f, f"{outlog}/qc1f")
            status+=1
        ##
        sub_dir_info=os.path.join(path, "info")
        if os.path.isdir(sub_dir_info):
            compress_validate_delete(sub_dir_info, f"{outlog}/info")
            status+=1
    if status==0:
        return False
    else:
        return True 

if __name__ == '__main__':
    start_time = time.time()
    args=parser.parse_args()
    global_path=path=Path(args.dir)
    
    try:
        if os.path.islink(path):
            print("Exiting: This is a SymLink.")
            exit()

        elif os.path.isfile(path):
            print("Exiting: This is a File.")
            exit()

        elif os.path.isdir(path):
            write_logs("Script-started", '0')
            write_logs(f"#ACTIONs\tSIZE_before\tnFILES_before\tSIZE_after\tnFILES_after\tPATH", "_")
            total_size =convert_bytes(get_size(global_path)); total_files=count_files(global_path)
            single_size =0; single_count =0; rsingle_size=0; rsingle_count=0
            total_each=[]
            ##
            if args.allfunctions:
                ##deleting all files with size 0
                try:
                    count=remove_zero_files(path, "size-zero")
                    if count>0:
                        write_logs(f"Deleted_size-0\t0B\t{count}\t0B\t0\tNA", "_")
                    single_size +=0; single_count+=count
                    rsingle_size+=0; rsingle_count+=0
                except Exception as e:
                    write_logs(f"ERROR_size-zero_{e}")
                
                ##removing all errandout directories
                try:
                    errandout_path=find_files_or_dirs(path, "^errandout$")
                    ts=0; tc=0
                    if len(errandout_path)>0:
                        for p in errandout_path:
                            size=get_size(p); count=count_files(p)
                            ts+=size; tc+=count
                            reduce_errandout(p, "errandout")
                            write_logs(f"Deleted_errandout\t{convert_bytes(size)}\t{count}\t0B\t0\t{p}", "_")
                        single_size +=ts; single_count+=tc
                        rsingle_size+=0; rsingle_count+=0
                        ts=convert_bytes(ts)
                        total_each.append((f"Total_errandout\t{ts}\t{tc}\t0B\t0\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_errandout_{e}")

                ##reducing tmp_report directories
                try:
                    tmp_path=find_files_or_dirs(path, r"^tmp_report_.*\d$")
                    ts=0; tc=0; rts=0; rtc=0
                    if len(tmp_path)>0:
                        for p in tmp_path:
                            size=get_size(p); count=count_files(p)
                            ts+=size; tc+=count
                            rsize, rcount=reduce_tmp_report(p, "tmp-report")
                            rts+=rsize; rtc+=rcount
                            write_logs(f"dCVD_tmp-report\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        total_each.append((f"Total_tmp-report\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_tmp-report_{e}")

               ##reducing cobg_dir  
                try:
                    cobg_path=find_files_or_dirs(path, "^cobg_dir_genome_wide$")
                    ts=0; tc=0; rts=0; rtc=0
                    if len(cobg_path)>0:
                        for p in cobg_path:
                            size=get_size(p); count=count_files(p)
                            ts+=size; tc+=count
                            reduce_cobg_dir(p,"cobg-dir")
                            rsize=get_size(p); rcount=count_files(p)
                            rts+=rsize; rtc+=rcount
                            if (size!=rsize) | (count!=rcount):
                                write_logs(f"Reduced_cobg-dir\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        if (ts!=rts) | (tc!=rtc):
                            total_each.append((f"Total_cobg-dir\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_cobg-dir_{e}")

                ## CVD dasuqc1
                try:
                    dasuqc1_path=find_files_or_dirs(path, r"^dasuqc1_.*.ch.fl$")
                    ts=0; tc=0; rts=0; rtc=0;
                    if len(dasuqc1_path)>0:
                        for p in dasuqc1_path:
                            if os.path.isdir(p):
                                size=get_size(p); count=count_files(p)
                                ts+=size; tc+=count
                                status=reduce_dasuqc1(p, "dasuqc1")
                                rsize=get_size(p); rcount=count_files(p)
                                rts+=rsize; rtc+=rcount
                                if status:
                                    write_logs(f"Reduce_dasuqc1\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        if status:
                            total_each.append((f"Total_dasuqc1\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_dasuqc1_{e}")

                ## reduce pcaer_sub
                try:
                    pca_qassoc_path= find_files_or_dirs(path, r".*.menv.assomds.*qassoc$")
                    pca_assopdf_path=find_files_or_dirs(path, r".*.menv.mds.asso.pdf$")
                    ts=0; tc=0; rts=0; rtc=0
                    if (len(pca_qassoc_path)>0) | len(pca_assopdf_path)>0:
                        pcaer_sub_path = list(set([os.path.dirname(os.path.dirname(p)) for p in pca_qassoc_path]))
                        for p in pcaer_sub_path:
                            size=get_size(p); count=count_files(p)
                            ts+=size; tc+=count
                            rsize, rcount=reduce_pcaer_sub(p, "pcaer-sub")
                            rts+=rsize; rtc+=rcount
                            write_logs(f"dCVD_pacer-sub\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        total_each.append((f"Total_pcaer-sub\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_pcaer-sub_{e}")

                ###CVD resdaner
                try:
                    resdaner_path=find_files_or_dirs(path, "^resdaner$")
                    ts=0; tc=0; rts=0; rtc=0
                    if len(resdaner_path)>0:
                        for p in resdaner_path:
                            size=get_size(p); count=count_files(p)
                            ts+=size; tc+=count
                            compress_path=compress_validate_delete(p, "resdaner")
                            rsize=get_size(compress_path); rcount=1
                            rts+=rsize; rtc+=rcount
                            write_logs(f"CVD_resdaner\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        total_each.append((f"Total_resdaner\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                     write_logs(f"ERROR_resdaner_{e}")

                ### CVD danscore
                try:
                    danscore_path=find_files_or_dirs(path, r"^danscore_.*(?<!tar\.gz)$")
                    ts=0; tc=0; rts=0; rtc=0
                    if len(danscore_path)>0:
                        for p in danscore_path:
                            if os.path.isdir(p):
                                size=get_size(p); count=count_files(p)
                                ts+=size; tc+=count
                                compress_path=compress_validate_delete(p, "danscore")
                                rsize=get_size(compress_path); rcount=1
                                rts+=rsize; rtc+=rcount
                                write_logs(f"CVD_danscore\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        total_each.append((f"Total_danscore\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_danscore_{e}")

                #### CVD report 
                try: 
                    report_path=find_files_or_dirs(path, r"^report_.*(?<!tar\.gz)$")
                    ts=0; tc=0; rts=0; rtc=0
                    if len(report_path)>0:
                        for p in report_path:
                            if os.path.isdir(p):
                                size=get_size(p); count=count_files(p)
                                ts+=size; tc+=count
                                rsize, rcount=reduce_report_sub(p, "report-sub")
                                rts+=rsize; rtc+=rcount
                                write_logs(f"CVD_report-sub\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        total_each.append((f"Total_report-sub\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_report-sub_{e}")

                #### Reduce dameta 
                try:
                    dameta_path=find_files_or_dirs(path, r"^dameta_.*(?<!tar\.gz)$")
                    ts=0; tc=0; rts=0; rtc=0
                    if len(dameta_path)>0:
                        for p in dameta_path:
                            if os.path.isdir(p):
                                size=get_size(p); count=count_files(p)
                                ts+=size; tc+=count
                                rsize, rcount=reduce_dameta(p, "dameta")
                                rts+=rsize; rtc+=rcount
                                write_logs(f"dCVD_dameta\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        total_each.append((f"Total_dameta-sub\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                    write_logs(f"ERROR_dameta_{e}")

                #### reduce daner_sub
                try:
                    assoc_dos_path=find_files_or_dirs(path, r"^dan_.*assoc.dosage.ngt.gz$")
                    ts=0; tc=0; rts=0; rtc=0
                    if len(assoc_dos_path)>0: 
                        daner_paths = list(set([os.path.dirname(os.path.dirname(p)) for p in assoc_dos_path]))
                        for p in daner_paths:
                            size=get_size(p); count=count_files(p)
                            ts+=size; tc+=count
                            assoc_paths=  find_files_or_dirs(p, r"^dan_.*assoc.dosage.ngt.gz$")
                            for assoc_path in assoc_paths:
                                delete_files(assoc_path, "daner-sub")
                            compress_path=compress_validate_delete(p, "daner-sub")
                            rsize=get_size(compress_path); rcount=1
                            rts+=rsize; rtc+=rcount
                            write_logs(f"dCVD_daner-sub\t{convert_bytes(size)}\t{count}\t{convert_bytes(rsize)}\t{rcount}\t{p}", "_")
                        single_size +=ts;     single_count+=tc
                        rsingle_size+=rts;    rsingle_count+=rtc
                        ts=convert_bytes(ts); rts=convert_bytes(rts)
                        total_each.append((f"Total_daner-sub\t{ts}\t{tc}\t{rts}\t{rtc}\tNA"))
                except Exception as e:
                        write_logs(f"ERROR_daner-sub_{e}")
                
                for unit in total_each:
                    write_logs(unit, "_")
            write_logs(f"Total_active\t{convert_bytes(single_size)}\t{single_count}\t{convert_bytes(rsingle_size)}\t{rsingle_count}\t{path}", "_")
            rtotal_size =convert_bytes(get_size(global_path)); rtotal_files=count_files(global_path)
            write_logs(f"Total_directroy\t{total_size}\t{total_files}\t{rtotal_size}\t{rtotal_files}\t{path}", "_")
            end_time = time.time()
            elapsed_time = end_time - start_time
            hours = int(elapsed_time // 3600); minutes = int((elapsed_time % 3600) // 60); seconds = int(elapsed_time % 60)
            write_logs(f"Total-time-elapsed:\t{hours}:{minutes}:{seconds}",'1')

        else:
            print("Exiting: UnKnown FileType.")
    
    except Exception as e:
        write_logs(f"ERROR_{e}")
