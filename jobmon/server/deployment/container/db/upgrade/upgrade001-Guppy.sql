use `docker`;

ALTER TABLE `workflow_run`
ADD COLUMN `jobmon_version` VARCHAR(150) NOT NULL DEFAULT 'UNKNOWN';