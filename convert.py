import re
import os
import json

'''Simple rsync log parsing utility made during a regex class for practice.  The class version was on Windows using Robocopy logs, but I made mine using rsync.  Includes one successful job and one failed job, both output to a json flat file.'''

#Error codes from the man page
ERROR_CODES = {
    0: 'Success',
    1: 'Syntax or usage error',
    2: 'Protocol incompatibility',
    3: 'Errors selecting input/output files, dirs',
    4: 'Requested  action not supported',
    5: 'Error starting client-server protocol',
    6: 'Daemon unable to append to log-file',
    10: 'Error in socket I/O',
    11: 'Error in file I/O',
    12: 'Error in rsync protocol data stream',
    13: 'Errors with program diagnostics',
    14: 'Error in IPC code',
    20: 'Received SIGUSR1 or SIGINT',
    21: 'Some error returned by waitpid',
    22: 'Error allocating core memory buffers',
    23: 'Partial transfer due to error',
    24: 'Partial transfer due to vanished source files',
    25: 'The --max-delete limit stopped deletions',
    30: 'Timeout in data send/receive',
    35: 'Timeout waiting for daemon connection'
}


#source log
log_file_path = r"/home/useraddmario/python/re_practice/log_reports/logs/rsync.log"

#empty jobs container
jobs = []

#empty processed jobs container
processed = []


def parse_log(log):
    #open log and read entire file in
    with open(log_file_path, "r") as raw_file:
        file = raw_file.read()

    #match for individual jobs
    match = re.findall(r'(?m:Time\:)[\w\W]+?ending\.', file)

    #write individual jobs to list
    for matched in match:
        jobs.append(matched)


def parse_jobs(jobs):
    #match - machine=server1 user=mrodriguez path=~/
    DEST_DIR = r'(?:machine\W)(?P<server>[a-z]{6}\d)\s(?:user\W)(?P<user_dir>\w+?)\s(?:path\W)(?P<path>\W+)'

    #match - Time: Mon 08 Jun 2020 08:47:11 PM CST
    JOB_BEGIN = r'(?:Time\:\s)(?P<begin>[\w\W]+?(?:[A-Z]{2}\s[A-Z]{3}))(?:\.\sJob\sbegining\.)'
    JOB_END = r'(?:Time\:\s)(?P<end>.+?(?:[A-Z]{2}\s[A-Z]{3}))(?:\.\sJob\sending\.)'
    
    SOURCE = os.uname()[1]

    #2020/06/08 20:47:43 [36121] [sender] _exit_cleanup(code=11, file=io.c, line=-1642): entered
    EXIT_CODE = r'(?:code\=)(?P<code>\d{1,})'

    #match:
    #2020/06/08 20:47:16 [36113] Number of created files: 84 (reg: 55, dir: 29)
    #2020/06/08 20:47:16 [36113] Number of deleted files: 0
    #2020/06/08 20:47:16 [36113] sent 11,131,283 bytes  received 74,906 bytes  2,037,488.91 bytes/sec
    STATS_FILES = r'(?:created\sfiles\:\s)(?P<total>\d+?)\s\Wreg\:\s(?P<files>\d+?),\sdir\:\s(?P<dirs>\d+?)\W'
    STATS_DELETED = r'(?:deleted\sfiles\:\s)(?P<deleted>\d+?)'
    STATS_SENT = r'(?:\ssent\s)(?P<sent>\d+(,\d{3})*(\.\d{2})?)'
    STATS_XFER = r'(?P<speed>\d+(,\d{3})*(\.\d{2})?)\sbytes\/sec'

    for job in jobs:
        #running job format, will be output to json
        current_job = {
            'begin': '',
            'end': '',
            'source': '',
            'destination': '',
            'exit_code': 1,
            'exit_message': '',
            'total_files': 0,
            'files': 0,
            'directories': 0,
            'deleted': 0,
            'total_xfer': 0,
            'xfer_speed': 0
        }
        #
        #populate the current_job using line by line regex matching
        #
        for line in job.splitlines():
            current_job['source'] = SOURCE


            match = re.search(JOB_BEGIN, line)
            if match:
                current_job['begin'] = match.group('begin')


            match = re.search(JOB_END, line)
            if match:
                current_job['end'] = match.group('end')


            match = re.search(DEST_DIR, line)
            if match:
                current_job['destination'] = match.group('user_dir') + '@' + match.group('server') + ':' + match.group('path')


            match = re.search(STATS_FILES, line)
            if match:
                current_job['files'] = int(match.group('files'))
                current_job['directories'] = int(match.group('dirs'))
                current_job['total_files'] = int(match.group('total'))


            match = re.search(STATS_DELETED, line)
            if match:
                current_job['deleted'] = int(match.group('deleted'))


            match = re.search(STATS_SENT, line)
            if match:
                current_job['total_xfer'] = int(match.group('sent').replace(',', ''))


            match = re.search(STATS_XFER, line)
            if match:
                current_job['xfer_speed'] = float(match.group('speed').replace(',', ''))


            match = re.search(EXIT_CODE, line)
            if match:
                current_job['exit_code'] = int(match.group('code'))
                current_job['exit_message'] = ERROR_CODES[current_job['exit_code']]

        processed.append(current_job)


parse_log(log_file_path)

parse_jobs(jobs)

with open('processed.json', 'w') as export:
    json.dump(processed, export)

os.rename('processed.json', './converted/processed.json')
