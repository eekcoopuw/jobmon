--
-- Table structure for table `cluster_type`
--
use `docker`;

DROP TABLE IF EXISTS `cluster_type`;

CREATE TABLE `cluster_type` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `package_location` VARCHAR(2500) NULL,
  `logfile_templates` VARCHAR(500) NULL,
  PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `cluster_type` ADD UNIQUE INDEX `ui_name` (`name`);

-- load defaults
LOCK TABLES `cluster_type` WRITE;
INSERT INTO `cluster_type`(`name`, `package_location`, `logfile_templates`)
VALUES
    ('dummy', 'jobmon.builtins.dummy', '{}'),
    ('sequential', 'jobmon.builtins.sequential', '{"job": {"stdout": "{root}/{name}.o{distributor_id}", "stderr": "{root}/{name}.e{distributor_id}"}}'),
    ('multiprocess', 'jobmon.builtins.multiprocess', '{"array": {"stdout": "{root}/{name}.o{distributor_id}", "stderr": "{root}/{name}.e{distributor_id}"}}');
UNLOCK TABLES;
