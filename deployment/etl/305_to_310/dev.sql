-- To be applied on dev 3.0.5 version to bring it up to 3.1, with schema elements for both 3.0.5 and 3.1
USE docker;

-- Imimate prod environment; only need it if starting testing from repo(instead of prod data)
-- INSERT INTO `cluster_type`(`name`, `package_location`)
--    VALUES
--        ('dummy', 'jobmon.cluster_type.dummy'),
--        ('sequential', 'jobmon.cluster_type.sequential'),
--        ('multiprocess', 'jobmon.cluster_type.multiprocess'),
--        ('UGE', 'jobmon_uge'),
--        ('slurm', 'jobmon_slurm');

-- COLLATE utf8mb4_bin to allow distinction between lowercase and uppercase
ALTER TABLE `arg` MODIFY COLUMN `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;

-- array
CREATE TABLE `array` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_template_version_id` INTEGER,
  `workflow_id` INTEGER,
  `max_concurrently_running` INTEGER,
  `created_date` datetime DEFAULT NULL,
   PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `array` ADD INDEX `ix_task_template_version_id` (`task_template_version_id`);
ALTER TABLE `array` ADD INDEX `ix_workflow_id` (`workflow_id`);

-- add constraint on task_template_version_id/workflow_id
ALTER TABLE `array` ADD CONSTRAINT `uc_task_template_version_id_workflow_id` UNIQUE (`task_template_version_id`, `workflow_id`);

-- add name colum; see GBDSCI-4184
ALTER TABLE `array` ADD COLUMN `name` varchar(255) NOT NULL;
ALTER TABLE `array` ADD INDEX `ix_name` (`name`);

-- task
ALTER TABLE `task`
  ADD COLUMN `array_id` INTEGER DEFAULT NULL AFTER `task_args_hash`;
ALTER TABLE `task` ADD INDEX `ix_array_id` (`array_id`);
ALTER TABLE `task` ADD INDEX `name` (`name`);

ALTER TABLE `task_instance`
  MODIFY COLUMN `distributor_id` varchar(20) DEFAULT NULL,
  ADD COLUMN `array_id` INTEGER DEFAULT NULL AFTER `cluster_type_id`,
  ADD COLUMN `cluster_id` INTEGER DEFAULT NULL AFTER `array_id`,
  ADD COLUMN `array_batch_num` INTEGER DEFAULT NULL AFTER `task_resources_id`,
  ADD COLUMN `array_step_id` INTEGER DEFAULT NULL AFTER `array_batch_num`,
  ADD COLUMN `stdout` varchar(150) DEFAULT NULL AFTER `io`,
  ADD COLUMN `stderr` varchar(150) DEFAULT NULL AFTER `stdout`,
  ADD KEY `ix_array_id` (`array_id`),
  ADD KEY `ix_array_batch_num` (`array_batch_num`),
  ADD KEY `ix_array_step_id` (`array_step_id`),
  ADD KEY `ix_cluster_id` (`cluster_id`),
  ADD KEY `ix_array_batch_index` (`array_id`,`array_batch_num`,`array_step_id`);

ALTER TABLE `task_resources`
  MODIFY COLUMN `task_id` INTEGER NULL;

ALTER TABLE `cluster_type`
  ADD COLUMN `package_location_306` VARCHAR(2500) DEFAULT NULL AFTER `package_location`;

UPDATE `cluster_type`
SET `package_location_306` =
    CASE
        WHEN `name` = 'dummy' THEN 'jobmon.cluster_type.dummy'
        WHEN `name` = 'sequential' THEN 'jobmon.cluster_type.sequential'
        WHEN `name` = 'multiprocess' THEN 'jobmon.cluster_type.multiprocess'
        WHEN `name` = 'UGE' THEN 'jobmon_uge'
        WHEN `name` = 'slurm' THEN 'jobmon_slurm'
    END,
    `package_location` =
    CASE
        WHEN `name` = 'dummy' THEN 'jobmon.builtins.dummy'
        WHEN `name` = 'sequential' THEN 'jobmon.builtins.sequential'
        WHEN `name` = 'multiprocess' THEN 'jobmon.builtins.multiprocess'
        WHEN `name` = 'UGE' THEN 'jobmon_uge'
        WHEN `name` = 'slurm' THEN 'jobmon_slurm'
    END;

