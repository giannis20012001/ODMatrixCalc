CREATE TABLE `testDb`.`input` (
  `objId` BIGINT NULL,
  `trajId` BIGINT NULL,
  `t` BIGINT NULL,
  `lon` DOUBLE NULL,
  `lat` DOUBLE NULL,
  `x` DOUBLE NULL,
  `y` DOUBLE NULL);

CREATE TABLE testDb.results (
  jobCode VARCHAR(20),
  cellIdO VARCHAR(20),
  cellIdD VARCHAR(20),
  trajIds LONGTEXT,
  count LONG
);
##########################################
#######Unused sql table creators##########
##########################################
CREATE TABLE `testDb`.`fmroutput` (
  `objId`         BIGINT NOT NULL,
  `trajId`        BIGINT NOT NULL,
  `startCellId`   INT    NOT NULL,
  `endCellId`     INT    NOT NULL,
  `crossingPairs` TEXT   NOT NULL);

CREATE TABLE `testDb`.`jobsResult` (
  `fromCell` INT      NOT NULL,
  `toCell`   INT      NOT NULL,
  `jobID`    INT      NOT NULL,
  `result`   LONGTEXT NOT NULL
);

CREATE TABLE `testDb`.`minmax` (
  `minX` DOUBLE NULL,
  `minY` DOUBLE NULL,
  `minT` DOUBLE NULL,
  `maxX` DOUBLE NULL,
  `maxY` DOUBLE NULL,
  `maxT` DOUBLE NULL);

CREATE TABLE `testDb`.`cells` (
  `cellId` INT NULL,
  `bottomLeftX` DOUBLE NULL,
  `bottomLeftY` DOUBLE NULL,
  `topRightX` DOUBLE NULL,
  `topRightY` DOUBLE NULL);

CREATE TABLE `testDb`.`stage1tuples` (
  `cellId` INT NULL,
  `objId` BIGINT NULL,
  `trajId` BIGINT NULL,
  `minT` DATETIME NULL,
  `maxT` DATETIME NULL);