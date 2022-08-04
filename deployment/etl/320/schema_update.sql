ALTER TABLE `docker`.`workflow_run`
ADD COLUMN `jobmon_server_version` VARCHAR(150) NULL DEFAULT NULL AFTER `jobmon_version`;