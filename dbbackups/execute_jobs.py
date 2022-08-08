from ..dbbackups.BackupBase import BackupJobs

def main():
    jobs = BackupJobs.get_acivte_jobs()
    if not jobs:
        print("No bakup Jobs")
        return 1
    for name,job in BackupJobs.get_acivte_jobs().items():
        print('class name: ',name)
        x=job()
        x()
    return 1

if __name__=='__main__':
    main()