<?php
/**
 * Lithium: the most rad php framework
 *
 * @copyright     Copyright 2010, Union of RAD (http://union-of-rad.org)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_sqlsrv\extensions\adapter\data\source\database;

use lithium\data\model\QueryException;

/**
 * Extends the `Database` class to implement the necessary SQL-formatting and resultset-fetching
 * features for working with SQL Server.
 *
 * For more information on configuring the database connection, see the `__construct()` method.
 *
 * @see li3_sqlsrv\data\source\SqlSrv::__construct()
 */
class SqlSrv extends \lithium\data\source\Database {

	protected $_classes = array(
		'entity' => 'lithium\data\entity\Record',
		'set' => 'lithium\data\collection\RecordSet',
		'relationship' => 'lithium\data\model\Relationship',
	);

	/**
	 * SqlSrv column type definitions.
	 *
	 * @var array
	 */
	protected $_columns = array(
		'primary_key' => array('name' => 'IDENTITY (1, 1) NOT NULL'),
		'string' => array('name' => 'varchar', 'length' => '255'),
		'text' => array('name' => 'varchar', 'length' => 'max'),
		'integer' => array('name' => 'integer', 'length' => 11, 'formatter' => 'intval'),
		'float' => array('name' => 'float', 'formatter' => 'floatval'),
		'datetime' => array('name' => 'datetime', 'format' => 'Y-m-d H:i:s.u', 'formatter' => 'date'),
		'timestamp' => array('name' => 'timestamp', 'format' => 'Y-m-d H:i:s', 'formatter' => 'date'),
		'time' => array('name' => 'datetime', 'format' => 'H:i:s', 'formatter' => 'date'),
		'date'  => array('name' => 'datetime', 'format' => 'Y-m-d', 'formatter' => 'date'),
		'binary' => array('name' => 'varbinary', 'length' => 'max'),
		'boolean' => array('name' => 'bit')
	);

	/**
	 * Strings used to render the given statement
	 *
	 * @see lithium\data\source\Database::renderCommand()
	 * @var array
	 */
	protected $_strings = array(
		'read'   => "SELECT {:limit} {:fields} From {:source}
				{:joins} {:conditions} {:group} {:order} {:comment}",
		'paged'  => "
			SELECT * From (
				SELECT {:fields}, ROW_NUMBER() OVER({:order}) AS [__LI3_ROW_NUMBER__]
				From {:source} {:joins} {:conditions} {:group}
			) a {:limit} {:comment}",
		'create' => "INSERT INTO {:source} ({:fields}) VALUES ({:values}) {:comment}",
		'update' => "UPDATE {:source} SET {:data} {:conditions} {:comment}",
		'delete' => "DELETE {:flags} From {:source} {:aliases} {:conditions} {:comment}",
		'schema' => "CREATE TABLE {:source} (\n{:columns}\n) {:indexes} {:comment}",
		'join'   => "{:type} JOIN {:source} {:constraint}"
	);


	/**
	 * Pair of opening and closing quote characters used for quoting identifiers in queries.
	 *
	 * @var array
	 */
	protected $_quotes = array('[', ']');

	/**
	 * SQL Server-specific value denoting whether or not table aliases should be used in DELETE and
	 * UPDATE queries.
	 *
	 * @var boolean
	 */
	protected $_useAlias = true;

    /**
	 * The available SQL Server driver to use
	 *
	 * @var string
	 */

    protected $driver = 'sqlsrv';

	/**
	 * Constructs the SQL Server adapter and sets the default port to 1433.
	 *
	 * @see lithium\data\source\Database::__construct()
	 * @see lithium\data\Source::__construct()
	 * @see lithium\data\Connections::add()
	 * @param array $config Configuration options for this class. For additional configuration,
	 *        see `lithium\data\source\Database` and `lithium\data\Source`. Available options
	 *        defined by this class:
	 *        - `'database'`: The name of the database to connect to. Defaults to 'lithium'.
	 *        - `'host'`: The IP or machine name where SQL Server is running, followed by a colon,
	 *          followed by a port number or socket. Defaults to `(local), 1433`.
	 *        - `'persistent'`: If a persistent connection (connection pooling) should be made.
	 *          Defaults to true.
	 *        - `encoding`: One of `SQLSRV_ENC_CHAR`, `SQLSRV_ENC_BINARY` or `UTF-8`. Defaults
	 *          to `SQLSRV_ENC_CHAR` (the SQL Server default).
	 *        - `encrypted`: Specifies whether the communication with SQL Server is encrypted.
	 *        - `replica`: Specifies the server and instance of the database's mirror
	 *          (if enabled and configured) to use when the primary server is unavailable.
	 *        - `password`: Specifies the password associated with the User ID to be used when
	 *          connecting with SQL Server Authentication.
	 *        - `UID`: Specifies the User ID to be used when connecting with
	 *          SQL Server Authentication.
	 *        - `APP`: Specifies the application name used in tracing.
	 *        - `timeout`: Specifies the number of seconds to wait before failing the connection attempt.
	 *
	 * Typically, these parameters are set in `Connections::add()`, when adding the adapter to the
	 * list of active connections.
	 * @return The adapter instance.
	 */
	public function __construct(array $config = array()) {
		$defaults = array(
			'APP' => 'app',
			'host' => '(local), 1433',
			'UID' => null,
			'password' => null,
			'encoding' => SQLSRV_ENC_CHAR,
			'persistent' => true,
			'Encrypted' => false,
			'replica' => null,
			'timeout' => null,
			'driver' => null
		);

		$config = $config + $defaults;
		$this->_setDriver($config['driver']);

		parent::__construct($config);
	}

	protected function _setDriver($driver = null) {
	    if(!empty($driver)) {
	        $this->driver = $driver;
	    }
	    else {
	        if(function_exists('sqlsrv_connect')) {
			    $this->driver = 'sqlsrv';
			    $this->_classes['result'] = 'li3_sqlsrv\extensions\adapter\data\source\database\sql_srv\Result';
		    } elseif(function_exists('mssql_connect')) {
			    $this->driver = 'mssql';
			    $this->_classes['result'] = 'li3_sqlsrv\extensions\adapter\data\source\database\mssql\Result';
		    }
		    else {
		        // @todo Add support for ODBTP		        
		    }
		}
	}

	/**
	 * Check for required PHP extension, or supported database feature.
	 *
	 * @param string $feature Test for support for a specific feature, i.e. `"transactions"` or
	 *               `"arrays"`.
	 * @return boolean Returns `true` if the particular feature (or if SQL Server) support is enabled,
	 *         otherwise `false`.
	 */
	public static function enabled($feature = null) {
		if (!$feature) {
			return extension_loaded($this->driver);
		}
		$features = array(
			'arrays'        => false,
			'transactions'  => false,
			'booleans'      => true,
			'relationships' => true,
		);
		return isset($features[$feature]) ? $features[$feature] : null;
	}

	/**
	 * Connects to the database using the options provided to the class constructor.
	 *
	 * @return boolean Returns `true` if a database connection could be established, otherwise
	 *         `false`.
	 */
	public function connect() {
		$options = array();
		$config = $this->_config;
		$this->_isConnected = false;

		if (!$config['database']) {
			return false;
		}

		switch($this->driver) {
	        case 'mssql': $this->mssql_connect($config); break;
	        case 'sqlsrv': $this->sqlsrv_connect($config); break;
	    }

		if(!$this->connection) return false;

		return $this->_isConnected = true;
	}

	protected function mssql_connect($config) {

	    $pc = strpos(PHP_OS,'Win') !== false ? ',' : ':';

	    if(empty($config['persistent'])) { //Not persistent
	        $this->connection = mssql_connect($config['host'] . $pc . $config['port'], $config['login'], $config['password'], true);
	    }
	    else { //Persistent
	        $this->connection = mssql_pconnect($config['host'] . $pc . $config['port'], $config['login'], $config['password']);
	    }

	    if($this->connection) {
	        mssql_select_db($config['database'], $this->connection);
        }
	}

	protected function sqlsrv_connect($config) {
	    $mapping = array(
			'database'   => 'Database',
			'replica'    => 'Failover_partner',
			'password'   => 'PWD',
			'login'      => 'UID',
			'persistent' => 'ConnectionPooling',
			'encoding'   => 'CharacterSet',
			'timeout'    => 'LoginTimeout'
		);
		foreach ($mapping as $from => $to) {
			if (isset($config[$from])) {
				$options[$to] = $config[$from];
			}
		}

	    $this->connection = sqlsrv_connect($config['host'], $options);
	}

	/**
	 * Disconnects the adapter from the database.
	 *
	 * @return boolean True on success, else false.
	 */
	public function disconnect() {
		if ($this->_isConnected) {
		    switch($this->driver) {
		        case 'sqlsrv': $this->_isConnected = !sqlsrv_close($this->connection); break;
		        case 'mssql': $this->_isConnected = !mssql_close($this->connection); break;
		    }

			return !$this->_isConnected;
		}
		return true;
	}

	/**
	 * Returns the list of tables in the currently-connected database.
	 *
	 * @param string $model The fully-name-spaced class name of the model object making the request.
	 * @return array Returns an array of objects to which models can connect.
	 * @filter This method can be filtered.
	 */
	public function sources($model = null) {
		return $this->_filter(__METHOD__, compact('model'), function($self, $params) {
			$query = "SELECT TABLE_NAME FROM [INFORMATION_SCHEMA].[TABLES]";

			if (!$result = $self->invokeMethod('_execute', array($query))) {
				return null;
			}
			$entities = array();

			while ($data = $result->next()) {
				$entities[] = $data['TABLE_NAME'];
			}
			return $entities;
		});
	}

	/**
	 * Gets the column schema for a given SQL Server database table.
	 *
	 * @param mixed $entity Specifies the table name for which the schema should be returned, or
	 *        the class name of the model object requesting the schema, in which case the model
	 *        class will be queried for the correct table name.
	 * @param array $meta
	 * @return array Returns an associative array describing the given table's schema, where the
	 *         array keys are the available fields, and the values are arrays describing each
	 *         field, containing the following keys:
	 *         - `'type'`: The field type name
	 * @filter This method can be filtered.
	 */
	public function describe($entity, array $meta = array()) {
		$params = compact('entity', 'meta');
		return $this->_filter(__METHOD__, $params, function($self, $params, $chain) {
			extract($params);

			$name = $self->invokeMethod('_entityName', array($entity));
			$sql = "SELECT COLUMN_NAME as Field, DATA_TYPE as Type, "
				. "COL_LENGTH('{$name}', COLUMN_NAME) as Length, IS_NULLABLE As [Null], "
				. "COLUMN_DEFAULT as [Default], "
				. "COLUMNPROPERTY(OBJECT_ID('{$name}'), COLUMN_NAME, 'IsIdentity') as [Key], "
				. "NUMERIC_SCALE as Size FROM INFORMATION_SCHEMA.COLUMNS "
				. "WHERE TABLE_NAME = '{$name}'";

			if (!$columns = $self->invokeMethod('_execute', array($sql))) {
				return null;
			}
			$fields = array();

			while ($column = $columns->next()) {
				$fields[$column['Field']] = array(
					'type'     => $column['Type'],
					'length'   => $column['Length'],
					'null'     => ($column['Null'] == 'YES' ? true : false),
					'default'  => $column['Default'],
					'key'      => ($column['Key'] == 1 ? 'primary' : null)
				);
			}
			return $fields;
		});
	}

	public function create($query, array $options = array()) {
		if (is_object($query)) {
			$table = $query->source();
			$this->_execute("Set IDENTITY_INSERT [dbo].[{$table}] On");
		}
		return parent::create($query, $options);
	}

	/**
	 * Converts a given value into the proper type based on a given schema definition.
	 *
	 * @see lithium\data\source\Database::schema()
	 * @param mixed $value The value to be converted. Arrays will be recursively converted.
	 * @param array $schema Formatted array from `lithium\data\source\Database::schema()`
	 * @return mixed Value with converted type.
	 */
	public function value($value, array $schema = array()) {
		if (($result = parent::value($value, $schema)) !== null) {
			return $result;
		}
		return "'" . $value . "'";
	}

	/**
	 * In cases where the query is a raw string (as opposed to a `Query` object), to database must
	 * determine the correct column names from the result resource.
	 *
	 * @param mixed $query
	 * @param resource $resource
	 * @param object $context
	 * @return array
	 */
	public function schema($query, $result = null, $context = null) {
		if (is_object($query)) {
			return parent::schema($query, $result, $context);
		}
		$fields = array();
		$count = 0;

		if($this->driver == 'sqlsrv') {
		    $count = sqlsrv_num_fields($result->resource());

		    foreach (sqlsrv_field_metadata($result->resource()) as $name => $value) {
		        // TODO: Ensure this works correctly
    			if ($name === 'Name') {
    				$fields[] = $value;
    			}
    		}
		}
		elseif($this->driver == 'mssql') {
		    $count = mssql_num_fields($result->resource());

		    for($i == 0; $i < $count; ++$i) {
		        $field = mssql_fetch_field($result->resource(),$i);

		        $fields[] = $field->name;
		    }

		}

		return $fields;
	}

	public function data($data, $context) {
		if ($context->type() != "update" || !($entity =& $context->entity())) {
			return $data;
		}

		$data = array();

		foreach ($entity->export($this) as $name => $value) {
			$name = $this->name($name);
			$value = $this->value($value);
			$data[] = "{$name} = {$value}";
		}
		return join(", ", $data);
	}

	public function encoding($encoding = null) {
		return true;
	}

	/**
	 * Retrieves database error message and error code.
	 *
	 * @return array
	 */
	public function error() {
	    if($this->driver == 'sqlsrv') {
		    if ($error = sqlsrv_errors(SQLSRV_ERR_ALL)) {
			    return array($error[0]['code'], $error[0]['message']);
		    }
	    }
	    elseif($this->driver == 'mssql') {
	        $error = mssql_get_last_message();
	        if(!empty($error)) {
	            return array(001,$error); //Driver does not provide error codes
	        }
	    }

		return null;
	}

	/**
	 * Returns a TOP clause (usually) from the given limit and the offset of the context object.
	 *
	 * @param integer $limit A number of records to which the query is limited to returning.
	 * @param object $context The `lithium\data\model\Query` object
	 * @return string
	 */
	public function limit($limit, $context) {
		if (!$limit) {
			return;
		}
		if ($offset = $context->offset() ?: '') {
			// @todo HERE BE DRAGONS
			$offset .= ', ';
		}
		return "TOP {$limit}";
	}

	public function alias($alias, $context) {
		if ($context->type() == 'update' || $context->type() == 'delete') {
			return;
		}
		return parent::alias($alias, $context);
	}

    public function driver() {
        return $this->driver;
    }

	/**
	 * @todo Eventually, this will need to rewrite aliases for DELETE and UPDATE queries, same with
	 *       order().
	 * @param string $conditions
	 * @param string $context
	 * @param array $options
	 * @return void
	 */
	public function conditions($conditions, $context, array $options = array()) {
		return parent::conditions($conditions, $context, $options);
	}

	/**
	 * Execute a given query.
 	 *
 	 * @see lithium\data\source\Database::renderCommand()
	 * @param string $sql The sql string to execute
	 * @param array $options
	 * @return resource Returns the result resource handle if the query is successful.
	 */
	protected function _execute($sql, array $options = array()) {
		return $this->_filter(__METHOD__, compact('sql', 'options'), function($self, $params) {
			$sql = $params['sql'];
			$options = $params['options'];
			$resource = null;

			switch($self->driver()) {
			    case 'sqlsrv': $resource = sqlsrv_query($self->connection, $sql); break;
			    case 'mssql': $resource = mssql_query($sql,$self->connection); break;
		    }

			if ($resource === true) {
				return true;
			}
			if (is_resource($resource)) {
				return $self->invokeMethod('_instance', array('result', compact('resource')));
			}
			list($code, $error) = $self->error();
			throw new QueryException("{$sql}: {$error}", $code);
		});
	}

	/**
	 * Gets the last auto-generated ID from the query that inserted a new record.
	 *
	 * @param object $query The `Query` object associated with the query which generated
	 * @return mixed Returns the last inserted ID key for an auto-increment column or a column
	 *         bound to a sequence.
	 */
	protected function _insertId($query) {
		$resource = $this->_execute('SELECT @@identity as insertId');
		$id = $this->result('next', $resource, null);
		$id = $id['insertId'];
		$this->result('close', $resource, null);

		if (!empty($id) && $id !== '0') {
			return $id;
		}
	}

	/**
	 * Converts database-layer column types to basic types.
	 *
	 * @param string $real Real database-layer column type (i.e. "varchar(255)")
	 * @return string Abstract column type (i.e. "string")
	 */
	protected function _column($real) {

		if (is_array($real)) {
			$length = '';
			if (isset($real['length'])) {
				$length = $real['length'];
				if ($length === -1) {
					$length = 'max';
				}
				$length = '(' . $length . ')';
			}
			return $real['type'] . $length;
		}

		if (!preg_match('/(?P<type>[^(]+)(?:\((?P<length>[^)]+)\))?/', $real, $column)) {
			return $real;
		}
		$column = array_intersect_key($column, array('type' => null, 'length' => null));

		switch (true) {
			case $column['type'] === 'datetime':
			break;
			case ($column['type'] == 'tinyint' && $column['length'] == '1'):
			case ($column['type'] == 'bit'):
				$column = array('type' => 'boolean');
			break;
			case (strpos($column['type'], 'int') !== false):
				$column['type'] = 'integer';
			break;
			case (strpos($column['type'], 'text') !== false):
				$column['type'] = 'text';
			break;
			case strpos($column['type'], 'char') !== false:
				if (isset($column['length']) && $column['length'] === 'max') {
					$column['type'] = 'text';
					unset($column['length']);
				} else {
					$column['type'] = 'string';
				}
			break;
			case (strpos($column['type'], 'binary') !== false || $column['type'] == 'image'):
				$column['type'] = 'binary';
			break;
			case preg_match('/float|double|decimal/', $column['type']):
				$column['type'] = 'float';
			break;
			default:
				$column['type'] = 'text';
			break;
		}
		return $column;
	}

	/**
	 * Helper method that retrieves an entity's name via its metadata.
	 *
	 * @param string $entity Entity name.
	 * @return string Name.
	 */
	protected function _entityName($entity) {
		if (class_exists($entity, false) && method_exists($entity, 'meta')) {
			$entity = $entity::meta('name');
		}
		return $entity;
	}
}

?>