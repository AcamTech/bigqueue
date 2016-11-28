DROP DATABASE IF EXISTS bigqueue;
CREATE DATABASE bigqueue;

use bigqueue;
-- Tables

DROP TABLE IF EXISTS `clusters`;
CREATE TABLE `clusters` (
  `name` varchar(50) NOT NULL,
  `default` char(1) DEFAULT 'N',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `consumers`;
CREATE TABLE `consumers` (
  `consumer_id` varchar(255) NOT NULL,
  `tenant_id` varchar(255) DEFAULT NULL,
  `tenant_name` varchar(255) DEFAULT NULL,
  `consumer_name` varchar(255) DEFAULT NULL,
  `cluster` varchar(50) NOT NULL,
  `topic_id` varchar(255) NOT NULL,
  `create_time` datetime,
  PRIMARY KEY (`consumer_id`, `cluster`, `topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `consumers_history`;
CREATE TABLE `consumers_history` (
  `consumer_id` varchar(255) NOT NULL,
  `tenant_id` varchar(255) DEFAULT NULL,
  `tenant_name` varchar(255) DEFAULT NULL,
  `consumer_name` varchar(255) DEFAULT NULL,
  `cluster` varchar(50) DEFAULT NULL,
  `topic_id` varchar(255) DEFAULT NULL,
  `create_time` datetime,
  `modify_date` datetime,
  `modify_action` varchar(45)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



DROP TABLE IF EXISTS `data_nodes`;
CREATE TABLE `data_nodes` (
  `id` varchar(255) NOT NULL,
  `host` varchar(255) DEFAULT NULL,
  `port` int(11) DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `type` varchar(45) DEFAULT NULL,
  `options` varchar(512) DEFAULT NULL,
  `cluster` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `data_nodes_history`;
CREATE TABLE `data_nodes_history` (
  `id` varchar(255) NOT NULL,
  `host` varchar(255) DEFAULT NULL,
  `port` int(11) DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `type` varchar(45) DEFAULT NULL,
  `options` varchar(512) DEFAULT NULL,
  `cluster` varchar(50) DEFAULT NULL,
  `modify_date` datetime DEFAULT NULL,
  `modify_action` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `endpoints`;
CREATE TABLE `endpoints` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `host` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `port` int(11) NOT NULL,
  `cluster` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `node_journals`;
CREATE TABLE `node_journals` (
  `node_id` varchar(255) NOT NULL,
  `journal_id` varchar(255) NOT NULL,
  PRIMARY KEY (`node_id`,`journal_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `stats`;
CREATE TABLE `stats` (
  `cluster` varchar(255) NOT NULL,
  `node` varchar(255) NOT NULL,
  `topic` varchar(255) NOT NULL,
  `consumer` varchar(255) NOT NULL,
  `lag` int(11) NOT NULL,
  `fails` int(11) NOT NULL,
  `processing` int(11) NOT NULL,
  `last_update` datetime NOT NULL,
  PRIMARY KEY (`cluster`,`node`,`topic`,`consumer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
ALTER TABLE `bigqueue`.`stats` 
ADD INDEX `IDX_STATS_BY_CONSUMER` (`consumer` ASC) ;

DROP TABLE IF EXISTS `tasks`;
CREATE TABLE `tasks` (
  `task_id` int(11) NOT NULL AUTO_INCREMENT,
  `data_node_id` varchar(255) DEFAULT NULL,
  `task_group` varchar(255) DEFAULT NULL,
  `task_type` varchar(45) DEFAULT NULL,
  `task_data` varchar(1024) DEFAULT NULL,
  `task_status` varchar(45) DEFAULT 'PENDING',
  `create_time` datetime,
  `last_update_time` datetime,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `bigqueue`.`tasks` 
ADD INDEX `tasks_host_status_idx` (`data_node_id` ASC, `task_status` ASC) ;


DROP TABLE IF EXISTS `topics`;
CREATE TABLE `topics` (
  `topic_id` varchar(255) NOT NULL,
  `tenant_id` varchar(255) DEFAULT NULL,
  `tenant_name` varchar(255) DEFAULT NULL,
  `topic_name` varchar(255) DEFAULT NULL,
  `cluster` varchar(50) DEFAULT NULL,
  `ttl` int(11) DEFAULT NULL,
  `create_time` datetime,
  PRIMARY KEY (`topic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `topics_history`;
CREATE TABLE `topics_history` (
  `topic_id` varchar(255) NOT NULL,
  `tenant_id` varchar(255) DEFAULT NULL,
  `tenant_name` varchar(255) DEFAULT NULL,
  `topic_name` varchar(255) DEFAULT NULL,
  `cluster` varchar(50) DEFAULT NULL,
  `ttl` int(11) DEFAULT NULL,
  `create_time` datetime,
  `modify_date` datetime,
  `modify_action` varchar(45)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- Triggers

DROP TRIGGER IF EXISTS data_nodes_delete;
CREATE TRIGGER data_nodes_delete AFTER DELETE ON data_nodes 
FOR EACH ROW INSERT INTO data_nodes_history (id, host, port, status, type, options, cluster, modify_date, modify_action) 
VALUES(OLD.id, OLD.host, OLD.port, OLD.status, OLD.type, OLD.options,  OLD.cluster, now(), "DELETE");

DROP TRIGGER IF EXISTS data_nodes_update;
CREATE TRIGGER data_nodes_udate AFTER UPDATE ON data_nodes
FOR EACH ROW INSERT INTO data_nodes_history (id, host, port, status, type, options, cluster, modify_date, modify_action) 
VALUES(OLD.id, OLD.host, OLD.port, OLD.status, OLD.type, OLD.options, OLD.cluster, now(), "UPDATE");

DROP TRIGGER IF EXISTS topics_delete;
CREATE TRIGGER topics_delete AFTER DELETE ON topics 
FOR EACH ROW INSERT INTO topics_history (topic_id, tenant_id, tenant_name, topic_name, cluster, ttl, create_time, modify_date, modify_action)
VALUES(OLD.topic_id, OLD.tenant_id, OLD.tenant_name, OLD.topic_name, OLD.cluster, OLD.ttl, OLD.create_time, now(), "DELETE");

DROP TRIGGER IF EXISTS topics_update;
CREATE TRIGGER topics_update AFTER UPDATE ON topics 
FOR EACH ROW INSERT INTO topics_history (topic_id, tenant_id, tenant_name, topic_name, cluster, ttl, create_time, modify_date, modify_action)
VALUES(OLD.topic_id, OLD.tenant_id, OLD.tenant_name, OLD.topic_name, OLD.cluster, OLD.ttl, OLD.create_time, now(), "UPDATE");

DROP TRIGGER IF EXISTS consumers_delete;
CREATE TRIGGER consumers_delete AFTER DELETE ON consumers
FOR EACH ROW INSERT INTO consumers_history (consumer_id, tenant_id, tenant_name, consumer_name, cluster, topic_id, create_time, modify_date, modify_action)
VALUES (OLD.consumer_id, OLD.tenant_id, OLD.tenant_name, OLD.consumer_name, OLD.cluster, OLD.topic_id, OLD.create_time, now(), "DELETE");

DROP TRIGGER IF EXISTS consumers_update;
CREATE TRIGGER consumers_update AFTER UPDATE ON consumers
FOR EACH ROW INSERT INTO consumers_history (consumer_id, tenant_id, tenant_name, consumer_name, cluster, topic_id, create_time, modify_date, modify_action)
VALUES (OLD.consumer_id, OLD.tenant_id, OLD.tenant_name, OLD.consumer_name, OLD.cluster, OLD.topic_id, OLD.create_time, now(), "UPDATE");
