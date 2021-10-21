--
-- Table structure for table `dag`
--
use `docker`;

DROP TABLE IF EXISTS `dag`;
CREATE TABLE `dag` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `hash` VARCHAR(150) NOT NULL,
  `created_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- add index on our fake foreign key
ALTER TABLE `dag` ADD CONSTRAINT `uc_hash` UNIQUE (`hash`);
