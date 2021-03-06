<?php
/**
 * Lithium: the most rad php framework
 *
 * @copyright     Copyright 2010, Union of RAD (http://union-of-rad.org)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_sqlsrv\extensions\adapter\data\source\database\sql_srv;

class Result extends \lithium\data\source\database\Result {

	protected function _next() {
		return sqlsrv_fetch_array($this->_resource, SQLSRV_FETCH_ASSOC);
	}

	protected function _close() {
		if (is_resource($this->_resource)) {
			sqlsrv_free_stmt($this->_resource);
		}
	}
}

?>