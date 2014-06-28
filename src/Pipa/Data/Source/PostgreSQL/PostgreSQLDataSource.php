<?php

namespace Pipa\Data\Source\PostgreSQL;
use DateTime;
use Pipa\Data\Aggregate;
use Pipa\Data\Collection;
use Pipa\Data\Criteria;
use Pipa\Data\DataSource;
use Pipa\Data\Exception\AuthException;
use Pipa\Data\Exception\ConnectionException;
use Pipa\Data\Exception\ConstraintException;
use Pipa\Data\Exception\DataException;
use Pipa\Data\Exception\DuplicateEntryException;
use Pipa\Data\Exception\InvalidHostException;
use Pipa\Data\Exception\QueryException;
use Pipa\Data\Exception\QuerySyntaxException;
use Pipa\Data\Exception\UnknownCollectionException;
use Pipa\Data\Exception\UnknownFieldException;
use Pipa\Data\Exception\UnknownHostException;
use Pipa\Data\Exception\UnknownSchemaException;
use Pipa\Data\JoinableCollection;
use Pipa\Data\MultipleInsertionSupport;
use Pipa\Data\RelationalCriteria;
use Pipa\Data\SQLDataSource;
use Pipa\Data\TransactionalDataSource;
use Pipa\Data\Util\AbstractConvenientSQLDataSource;
use Psr\Log\LoggerInterface;

class PostgreSQLDataSource extends AbstractConvenientSQLDataSource implements DataSource, TransactionalDataSource, MultipleInsertionSupport {

	const TYPE_INT2 = "int2";
	const TYPE_INT4 = "int4";
	const TYPE_INT8 = "int8";
	const TYPE_BPCHAR = "bpchar";
	const TYPE_VARCHAR = "varchar";
	const TYPE_TEXT = "text";
	const TYPE_FLOAT4 = "float4";
	const TYPE_FLOAT8 = "float8";
	const TYPE_NUMERIC = "numeric";
	const TYPE_TIMESTAMP_TZ = "timestamptz";
	const TYPE_TIMESTAMP = "timestamp";
	const TYPE_BOOL = "bool";
	const TYPE_BYTE_ARRAY = "bytea";

	protected $connection;
	protected $generator;
	protected $logger;

	function __construct($db, $host, $user, $password) {
		@list($host, $port) = explode(":", $host);
		if (!$port) $port = 5432;

		if ($this->connection = pg_pconnect("dbname=$db host=$host port=$port user=$user password=$password")) {
			$this->generator = new PostgreSQLGenerator($this);
			pg_set_client_encoding($this->connection, "UTF-8");
		} else {
			throw new ConnectionException("Could not connect to database");
		}
	}

	function setLogger(LoggerInterface $logger) {
		$this->logger = $logger;
	}

	function aggregate(Aggregate $aggregate, Criteria $criteria) {
		$result = $this->query($this->generator->generateAggregate($aggregate, $criteria));
		return current(current($result));
	}

	function beginTransaction() {
		$this->execute("START TRANSACTION");
	}

	function commit() {
		$this->execute("COMMIT");
	}

	function count(Criteria $criteria) {
		$result = $this->query($this->generator->generateCount($criteria));
		return current(current($result));
	}

	function delete(Criteria $criteria) {
		return $this->execute($this->generator->generateDelete($criteria));
	}

	function execute($sql, array $parameters = null) {
		if ($parameters) $sql = $this->generator->interpolateParameters($sql, $parameters);

		if ($this->logger) {
			$this->logger->debug("$sql");
			$start = microtime(true);
		}

		pg_send_query($this->connection, $sql);

		while(pg_connection_busy($this->connection)) usleep(10 * 1000);

		$result = pg_get_result($this->connection);
		$sqlState = pg_result_error_field($result, PGSQL_DIAG_SQLSTATE);

		if (!$sqlState) {
			$rows = pg_affected_rows($result);

			if ($this->logger) {
				$elapsed = microtime(true) - $start;
				$this->logger->debug("$rows affected row(s), took {$elapsed}s");
			}

			return $rows;
		} else {
			throw $this->translateException($sqlState, pg_errormessage($this->connection));
		}
	}

	function find(Criteria $criteria) {
		return $this->query($this->generator->generateSelect($criteria));
	}

	function getCollection($name) {
		return new JoinableCollection($name);
	}

	function getConnection() {
		return $this->connection;
	}

	function getCriteria() {
		return new RelationalCriteria($this);
	}

	function query($sql, array $parameters = null) {
		if ($parameters) $sql = $this->generator->interpolateParameters($sql, $parameters);

		if ($this->logger) {
			$this->logger->debug("$sql");
			$start = microtime(true);
		}

		pg_send_query($this->connection, $sql);

        while(pg_connection_busy($this->connection)) usleep(10 * 1000);

		$result = pg_get_result($this->connection);
		$sqlState = pg_result_error_field($result, PGSQL_DIAG_SQLSTATE);

		if (!$sqlState) {
			$types = $this->getResultTypes($result);
			$items = array();
			while ($item = pg_fetch_assoc($result)) {
				$this->processItem($item, $types);
				$items[] = $item;
			}

			if ($this->logger) {
				$elapsed = microtime(true) - $start;
				$count = count($items);
				$this->logger->debug("Query returned $count item(s), took {$elapsed}s");
			}

			return $items;
		} else {
			throw $this->translateException($sqlState, pg_errormessage($this->connection));
		}
	}

	function rollback() {
		$this->execute("ROLLBACK");
	}

	function save(array $values, Collection $collection, $sequence = null) {
		$this->execute($this->generator->generateInsert($values, $collection));
		if ($sequence) {
			$id = $this->query($this->generator->generateSequenceSelect($sequence));
			$id = current(current($id));
			return $id;
		}
	}

	function saveMultiple(array $values, Collection $collection) {
		$this->execute($this->generator->generateMultipleInsert($values, $collection));
	}

	function update(array $values, Criteria $criteria) {
		return $this->execute($this->generator->generateUpdate($values, $criteria));
	}

	protected function processItem(array &$items, array $types) {
		foreach($items as $field=>&$value)
			if (!is_null($value)) {
				switch($types[$field]) {
					case self::TYPE_INT2:
					case self::TYPE_INT4:
					case self::TYPE_INT8:
						$value = (int) $value;
						continue;
					case self::TYPE_FLOAT4:
					case self::TYPE_FLOAT8:
					case self::TYPE_NUMERIC:
						$value = (double) $value;
						continue;
					case self::TYPE_TIMESTAMP:
					case self::TYPE_TIMESTAMP_TZ:
						$value = new DateTime($value);
						continue;
					case self::TYPE_BOOL:
						$value = $value == 't';
						continue;
				}
			}
	}

	protected function getResultTypes($result) {
		$types = array();
		for($i = 0, $n = pg_num_fields($result); $i < $n; $i++)
			$types[pg_field_name($result, $i)] = pg_field_type($result, $i);
		return $types;
	}

	protected function translateException($sqlState, $error) {
		if ($this->logger)
			$this->logger->error($error);

		switch($sqlState) {
			case "42P01":
				return new UnknownCollectionException($error);
			case "42703":
				return new UnknownFieldException($error);
			case "23505":
				return new DuplicateEntryException($error);
			default:
				return new QueryException($error);
		}
	}
}
