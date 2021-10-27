--
-- Table structure for table `node_instance_status`
--

DROP TABLE IF EXISTS `node_instance_status`;

CREATE TABLE `node_instance_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `node_instance_status`
VALUES
('G','REGISTERING'),
('Q','QUEUED'),
('A','ABORTED'),
('I','INSTANTIATING'),
('L','LAUNCHED'),
('R','RUNNING'),
('D','DONE'),
('T','TRIAGING'),
('F','ERROR_FATAL'),
('H','HALTED');
