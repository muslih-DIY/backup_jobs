from pathlib import Path
import os

CONFIG_DIR = os.path.join(Path(__file__).resolve().parent, 'config.ini')
# once backup failed , it should update the from table 
# if updating local table also failed, then data will be stored in csv at FAILED_BKP_DIR
FAILED_BKP_DIR = os.path.join(Path(__file__).resolve().parent, 'FailedJobs')
job_to_be_run:tuple = ('test',)
