-- To be applied on dev 3.0.5 version to bring it up to 3.1, with schema elements for both 3.0.5 and 3.1
USE docker;

-- COLLATE utf8mb4_bin to allow distinction between lowercase and uppercase
ALTER TABLE `arg` MODIFY COLUMN `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;

CREATE TABLE `array` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_template_version_id` int(11) DEFAULT NULL,
  `workflow_id` int(11) DEFAULT NULL,
  `max_concurrently_running` int(11) DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uc_task_template_version_id_workflow_id` (`task_template_version_id`,`workflow_id`),
  KEY `ix_task_template_version_id` (`task_template_version_id`),
  KEY `ix_workflow_id` (`workflow_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE `task`
  ADD COLUMN `array_id` int(11) DEFAULT NULL AFTER `task_args_hash`,
  ADD KEY `ix_array_id` (`array_id`);
  ADD INDEX `name` (`name`);

ALTER TABLE `task_instance`
  MODIFY COLUMN `distributor_id` varchar(20) DEFAULT NULL,
  ADD COLUMN `array_id` int(11) DEFAULT NULL AFTER `cluster_type_id`,
  ADD COLUMN `cluster_id` int(11) DEFAULT NULL AFTER `array_id`,
  ADD COLUMN `array_batch_num` int(11) DEFAULT NULL AFTER `task_resources_id`,
  ADD COLUMN `array_step_id` int(11) DEFAULT NULL AFTER `array_batch_num`,
  ADD COLUMN `stdout` varchar(150) DEFAULT NULL AFTER `io`;
  ADD COLUMN `stderr` varchar(150) DEFAULT NULL AFTER `stdout`;
  ADD KEY `ix_array_id` (`array_id`),
  ADD KEY `ix_cluster_id` (`cluster_id`);

ALTER TABLE `task_resources`
  MODIFY COLUMN `task_id` int(11);

ALTER TABLE `array`
  ADD INDEX `name` (`name`);
