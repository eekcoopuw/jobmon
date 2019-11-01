--
-- Table structure for table `dag`
--
use `docker`;

DROP TABLE IF EXISTS `dag`;
CREATE TABLE `dag` (
  `id` INTEGER NOT NULL,
  `hash` VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;