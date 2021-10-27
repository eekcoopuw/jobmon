--
-- Table structure for table `node_arg`
--

DROP TABLE IF EXISTS `node_arg`;

CREATE TABLE `node_arg` (
  `node_id` INTEGER NOT NULL,
  `arg_id` INTEGER NOT NULL,
  `hash` VARCHAR(150) NOT NULL,
  `val` text NOT NULL,
  PRIMARY KEY (`node_id`, `arg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE `node_arg` ADD INDEX `ix_node_id` (`node_id`);
ALTER TABLE `node_arg` ADD INDEX `ix_arg_id` (`arg_id`);
ALTER TABLE `node_arg` ADD INDEX `ix_hash` (`hash`);
