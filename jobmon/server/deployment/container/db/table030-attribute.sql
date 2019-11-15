--
-- Table structure for table `attribute`
--
use `docker`;

DROP TABLE IF EXISTS `attribute`;
CREATE TABLE `attribute` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;