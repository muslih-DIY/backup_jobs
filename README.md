###### create following table in both sql database to get the status of backup
-------------
CREATE TABLE BACKUP_STATUS
(BACKUP_DAY timestamp(6) ,NAME VARCHAR(100) ,FAILURE_GRAVITY INT,REMARK VARCHAR(100));