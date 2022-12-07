from .dbbackups import BackupJobs
from .settings import job_to_be_run,debug
from .dbs import get_db

def execute(ClassOfJobs:list=None):
    if ClassOfJobs is None:
        ClassOfJobs = job_to_be_run
    for jobclass in ClassOfJobs:
        jobs = BackupJobs.get_acivte_jobs(jobclass)

    if not jobs:
        print("No bakup Jobs")
        return 1
    for name,job in jobs.items():
        print('class name: ',name)
        work=job()
        work(get_db,debug)
    return 1

def details(ClassOfJobs:list=None):
    if ClassOfJobs is None:
        ClassOfJobs = job_to_be_run

    for jobclass in ClassOfJobs:
        jobs = BackupJobs.get_acivte_jobs(jobclass)
        count = 1
        if not jobs:
            print("No bakup Jobs")
            return 1
        for name,work in jobs.items():
            job:BackupJobs=work()
            print(f'(count :{count})## job_name = {job.job_name} ##')
            print('='*60)
            print(f'fromdbname  = {job.fromdbname} ')
            print(f'todbname  = {job.todbname} ')
            print(f'FromTable  = {job.FromTable} ')
            print(f'ToTable  = {job.ToTable} ')
            print(f'FromColumn  = {job.FromColumn} ')
            print(f'ToColumn  = {job.ToColumn} ')
            print(f'unique_keys  = {job.unique_keys} ')
            print(f'pre_pull_querys  = {job.pre_pull_querys} ')
            print(f'pull_query  = {job.pull_query} ')
            print(f'error_update_query  = {job.error_update_query} ')
            print(f'post_push_error_handle_failed_querys  = {job.post_push_error_handle_failed_querys} ')
            print(f'post_push_error_handle_success_querys  = {job.post_push_error_handle_success_querys} ')
            print(f'final_post_push_querys  =  {job.final_post_push_querys} ')
            print('='*60)
            count+=1

        