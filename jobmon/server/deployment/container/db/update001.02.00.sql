use `docker`;


ALTER TABLE `job_instance`
ADD COLUMN `maxpss` varchar(50) DEFAULT NULL;

