--
-- Table structure for table `dag`
--
use `docker`;

DROP TABLE IF EXISTS `dag`;
CREATE TABLE `dag` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `hash` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
