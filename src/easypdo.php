<?php
  /**
   * *** BEGIN LICENSE BLOCK *****
   *
   * This file is part of EasyPDO (http://easypdo.robpoyntz.com/).
   *
   * Software License Agreement (New BSD License)
   *
   * Copyright (c) 2010, Robert Poyntz / Digital Finery Pty Ltd
   * All rights reserved.
   *
   * Redistribution and use in source and binary forms, with or without modification,
   * are permitted provided that the following conditions are met:
   *
   *     * Redistributions of source code must retain the above copyright notice,
   *       this list of conditions and the following disclaimer.
   *
   *     * Redistributions in binary form must reproduce the above copyright notice,
   *       this list of conditions and the following disclaimer in the documentation
   *       and/or other materials provided with the distribution.
   *
   *     * Neither the name of Robert Poyntz, Digital Finery nor the names of its
   *       contributors may be used to endorse or promote products derived from this
   *       software without specific prior written permission.
   *
   * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
   * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
   * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
   * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
   * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
   * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
   *
   * ***** END LICENSE BLOCK *****
   *
   * @copyright   Copyright (C) 2010 Robert Poyntz
   * @author      Robert Poyntz <rob@digitalfinery.com.au>
   * @license     http://www.opensource.org/licenses/bsd-license.php
   * @package     EasyPDO
   * @version     0.1.6
   */
  class EasyPDOException extends \Exception
  {
  }

  ;
  class EDatabaseException extends EasyPDOException
  {
  }

  ;
  class ENoDatabaseConnection extends EDatabaseException
  {
  }

  ;
  class EDuplicateKey extends EDatabaseException
  {
  }

  ;

  // ##########################################

  interface EasyPDOInterface
  {
    function GetConnectionObject();

    function Close();

    function Reset();

    function StartTransaction();

    function RollbackTransaction();

    function CommitTransaction();

    function Fetch($sql);

    function FetchObject($sql);

    function FetchArray($sql);

    function FetchValue($sql);

    function ExecuteSQL($sql);

    function GetLastSQL();

    static function SetFetchMode($mode);
  }

  // ##########################################

  class QueryResultIterator implements \Iterator
  {
    protected $Query;
    protected $CurrentObj;
    protected $Idx;
    protected $FetchMode;

    public function __construct($query, $fetchMode)
    {
      $this->Query = $query;
      $this->Idx = 0;
      $this->FetchMode = $fetchMode;
    }

    public function current()
    {
      return $this->CurrentObj;
    }

    public function next()
    {
      $this->CurrentObj = $this->Query->fetch($this->FetchMode);
      $this->Idx ++;
    }

    public function key()
    {
      return $this->Idx;
    }

    public function valid()
    {
      return ($this->CurrentObj !== FALSE);
    }

    public function rewind()
    {
      $this->Idx = 0;
      $this->CurrentObj = $this->Query->fetch($this->FetchMode);
    }
  }

  // ##########################################

  class QueryResultIteratorClass extends QueryResultIterator
  {
    private $FetchClass;

    private function SetFetchClass($className)
    {
      if(! isset($className) || ($className == ''))
      {
        throw new EasyPDOException('Invalid class specified for FETCH_MODE_CLASS');
      }
      else if(! class_exists($className))
      {
        throw new EasyPDOException('Specified class does not exist for FETCH_MODE_CLASS');
      }

      $this->FetchClass = $className;
    }

    public function __construct($query, $fetchMode, $fetchClass)
    {
      parent::__construct($query, \PDO::FETCH_CLASS);
      $this->SetFetchClass($fetchClass);
    }

    public function rewind()
    {
      $this->Idx = 0;
      $this->CurrentObj = $this->Query->fetchObject($this->FetchClass);
    }

    public function next()
    {
      $this->CurrentObj = $this->Query->fetchObject($this->FetchClass);
      $this->Idx ++;
    }
  }

  // ##########################################

  abstract class EasyPDO implements EasyPDOInterface
  {
    const FETCH_MODE_NUMERIC_ARRAY = 1;
    const FETCH_MODE_ASSOCIATIVE_ARRAY = 2;
    const FETCH_MODE_OBJECT = 3;
    const FETCH_MODE_CLASS = 4;

    private static $FetchModes = array(
      EasyPDO::FETCH_MODE_NUMERIC_ARRAY     => \PDO::FETCH_NUM,
      EasyPDO::FETCH_MODE_ASSOCIATIVE_ARRAY => \PDO::FETCH_ASSOC,
      EasyPDO::FETCH_MODE_OBJECT            => \PDO::FETCH_OBJ,
      EasyPDO::FETCH_MODE_CLASS             => \PDO::FETCH_CLASS
    );

    protected static $FetchMode = EasyPDO::FETCH_MODE_OBJECT;
    protected static $FetchClass = NULL;
    protected static $Instance = NULL;

    private $LastSQL = '';

    /**
     * @var \PDO
     */
    protected $PDO;
    protected $ParamTypes = array(
      'i' => \PDO::PARAM_INT,
      'd' => \PDO::PARAM_STR,
      's' => \PDO::PARAM_STR,
      'n' => \PDO::PARAM_STR,
      'b' => \PDO::PARAM_LOB
    );

    /**
     * @var \PDOStatement
     */
    protected $Query = NULL;

    protected $QueryLog = array();

    private function UpdateQueryLog($sql)
    {
      $this->QueryLog[] = $sql;
    }

    /**
     * Sets the fetch mode for EasyPDO
     *
     * @param integer $mode expects EasyPDO::FETCH_MODE_NUMERIC_ARRAY, EasyPDO::FETCH_MODE_ASSOCIATIVE_ARRAY or EasyPDO::FETCH_MODE_OBJECT
     *
     * @return void
     */
    public static function SetFetchMode($mode, $className = NULL)
    {
      if($mode === EasyPDO::FETCH_MODE_CLASS)
      {
        if(! isset($className) || ($className == ''))
        {
          throw new EasyPDOException('Invalid class specified for FETCH_MODE_CLASS');
        }
        else if(! class_exists($className))
        {
          throw new EasyPDOException('Specified class does not exist for FETCH_MODE_CLASS');
        }
        else
        {
          EasyPDO::$FetchClass = $className;
        }
      }
      else
      {
        EasyPDO::$FetchClass = NULL;
      }

      EasyPDO::$FetchMode = $mode;
    }

    /**
     * Returns the \PDO::FETCH_XXX constant associated with the current fetch mode
     * @return integer
     */
    private static function GetFetchMode()
    {
      return EasyPDO::$FetchModes[EasyPDO::$FetchMode];
    }

    /*
    * Attempts to connect to the database using the specified connection string and credentials
    * @param string $connectionString
    * @param string $username
    * @param string $password
    * @throws ENoDatabaseConnection
    */
    protected function __construct($connectionString, $username = NULL, $password = NULL)
    {
      try
      {
        $this->PDO = new \PDO($connectionString, $username, $password);
        $this->PDO->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
      }
      catch(\Exception $e)
      {
        throw new ENoDatabaseConnection();
      }
    }

    /*
    * Binds values to the parameters in a PDOStatement object.
    * $args is an assoc array which holds the keys/values for named parameters.
    * @param array $args
    */
    protected function BindParams($args)
    {
      if(isset($args[0]) && count($args[0]) > 0)
      {
        /**
         * bindParam: only pass value as reference:
         * "...support the invocation of stored procedures that return data as output parameters,
         * and some also as input/output parameters that both send in data and are updated to receive it."
         */
        foreach($args[0] as $key => &$value)
        {
          $valueType = $this->getValueType($value);
          $paramType = strtolower(substr($valueType['type'], 0, 1));

          // default param type would be s = string
          if(! array_key_exists($paramType, $this->ParamTypes))
          {
            echo 'PARAM TYPE DOES NOT EXIST: ' . $key . '=>' . $value . ' (' . $valueType['type'] . ')';
            $paramType = 's';
          }

          $this->Query->bindParam(':' . $key, $value, $this->ParamTypes[$paramType]);
        }
      }
    }

    /*
    * Returns the last SQL executed (or attempted to be executed)
    */
    public function GetLastSQL()
    {
      return $this->LastSQL;
    }

    /*
    * Returns the \PDO connection instance for this object
    * @return \PDO
    */
    public function GetConnectionObject()
    {
      return $this->PDO;
    }

    /*
    * Deletes the current PDOStatement, if any.
    */
    public function Close()
    {
      if($this->Query)
      {
        unset($this->Query);
      }
    }

    /*
    * Alias for EasyPDO::Close()
    */
    public function Reset()
    {
      $this->Close();
    }

    /*
    * Starts a transaction (for database engines that support this feature)
    */
    public function StartTransaction()
    {
      $this->Query = NULL;
      $this->PDO->beginTransaction();
    }

    /*
    * Commits a transaction (for database engines that support this feature)
    */
    public function CommitTransaction()
    {
      $this->Query = NULL;
      $this->PDO->commit();
    }

    /*
    * Rolls back a transaction (for database engines that support this feature)
    */
    public function RollbackTransaction()
    {
      $this->Query = NULL;
      $this->PDO->rollBack();
    }

    /*
    * Destructor
    * Closes the current PDOStatement object (if any), and deletes the reference
    * to current EasyPDO singleton instance.
    */
    public function __destruct()
    {
      $this->Query = NULL;
      EasyPDO::$Instance = NULL;
    }

    private function PrepareSQL($sql)
    {
      if($sql != $this->LastSQL)
      {
        $this->UpdateQueryLog($sql);
        $this->Query = NULL;
        $this->LastSQL = $sql;
        $this->Query = $this->PDO->prepare($sql);
      }
    }

    /*
    * Executes an SQL 'SELECT' statement and returns an Iterator interface allowing access to the result set.
    * @param string $sql
    * @param string $types optional parameter type definition
    * @param mixed $value,... optional parameter value
    * @return QueryResultIterator
    */
    public function Fetch($sql)
    {
      $this->PrepareSQL($sql);
      $args = func_get_args();
      array_shift($args);
      $this->BindParams($args);
      $this->Query->execute();

      if(EasyPDO::$FetchMode == EasyPDO::FETCH_MODE_CLASS)
      {
        return new QueryResultIteratorClass($this->Query, EasyPDO::GetFetchMode(), EasyPDO::$FetchClass);
      }
      else
      {
        return new QueryResultIterator($this->Query, EasyPDO::GetFetchMode());
      }
    }

    /*
    * Returns the first row of an SQL SELECT statement as an array.
    * Array indexing is determined by the current FetchMode and can be either numerical or associative
    * @param string $sql
    * @param string $types optional parameter type definition
    * @param mixed $value,... optional parameter value
    * @return array
    */
    public function FetchArray($sql)
    {
      $this->PrepareSQL($sql);
      $args = func_get_args();
      array_shift($args);
      $this->BindParams($args);
      $this->Query->execute();
      if(EasyPDO::$FetchMode === EasyPDO::FETCH_MODE_NUMERIC_ARRAY)
      {
        return $this->Query->fetch(\PDO::FETCH_NUM);
      }
      else
      {
        return $this->Query->fetch(\PDO::FETCH_ASSOC);
      }
    }

    /*
    * Returns a single value from an SQL SELECT statement
    * @param string $sql
    * @param string $types optional parameter type definition
    * @param mixed $value,... optional parameter value
    * @return mixed
    */
    public function FetchValue($sql)
    {
      $this->PrepareSQL($sql);
      $args = func_get_args();
      array_shift($args);
      $this->BindParams($args);
      $this->Query->execute();
      $result = $this->Query->fetch(\PDO::FETCH_NUM);
      if($result && (count($result) > 0))
      {
        return $result[0];
      }
      else
      {
        return NULL;
      }
    }

    /*
    * Returns the first row of an SQL SELECT statement as an object.
    * @param string $sql
    * @param string $types optional parameter type definition
    * @param mixed $value,... optional parameter value
    * @return StdClass
    */
    public function FetchObject($sql)
    {
      $this->PrepareSQL($sql);
      $args = func_get_args();
      array_shift($args);
      $this->BindParams($args);
      $this->Query->execute();
      if(EasyPDO::$FetchMode == EasyPDO::FETCH_MODE_CLASS)
      {
        return $this->Query->fetch(\PDO::FETCH_CLASS, EasyPDO::$FetchClass);
      }
      else
      {
        return $this->Query->fetch(\PDO::FETCH_OBJ);
      }
    }

    /*
    * Returns an entire result set as either an array of objects or an
    * array of arrays, depending on the current FetchMode
    * @param string $sql
    * @param string $types optional parameter type definition
    * @param mixed $value,... optional parameter value
    * @return array
    */
    public function FetchAll($sql)
    {
      $this->PrepareSQL($sql);
      $args = func_get_args();
      array_shift($args);
      $this->BindParams($args);
      $this->Query->execute();
      if(EasyPDO::$FetchMode == EasyPDO::FETCH_MODE_CLASS)
      {
        return $this->Query->fetchAll(EasyPDO::GetFetchMode(), EasyPDO::$FetchClass);
      }
      else
      {
        return $this->Query->fetchAll(EasyPDO::GetFetchMode());
      }
    }

    /*
    * Returns the identity of the last inserted row, if any
    * Note this feature is not supported by all database engines
    * @return integer|null
    */
    protected function GetLastInsertID()
    {
      // Use the generic \PDO "lastInsertId" method.
      // Not all database engines support this feature. Those that do often have differing implementations.
      // This method may be overriden as required
      return ($this->PDO->lastInsertId() > 0) ? $this->PDO->lastInsertId() : NULL;
    }

    /*
    * Executes an SQL statement against the database
    * @param string $sql
    * @param string $types optional parameter type definition
    * @param mixed $value,... optional parameter value
    * @return integer|null returns the last inserted identity for INSERT statements, or null for other SQL statements
    */
    public function ExecuteSQL($sql)
    {
      $this->UpdateQueryLog($sql);
      if(! isset($this->Query) || ($this->LastSQL != $sql))
      {
        $this->Query = NULL;
        $this->LastSQL = $sql;
        $this->Query = $this->PDO->prepare($sql);
      }

      $args = func_get_args();
      array_shift($args);
      $this->BindParams($args);
      try
      {
        $this->Query->execute();
      }
      catch(\PDOException $e)
      {
        if($e->getCode() == ERROR_DUPLICATE_KEY)
        {
          throw new EDuplicateKey($e->getMessage());
        }
        else
        {
          throw $e;
        }
      }

      return $this->GetLastInsertID();
    }

    public function GetQueryLog()
    {
      return $this->QueryLog;
    }

    protected function getValueType($value, $max_length = 50)
    {
      $type = gettype($value);

      if($type == 'NULL' || $type == 'boolean' || $type == 'integer' || $type == 'double' || $type == 'object' || $type == 'resource' || $type == 'array')
      {
        return array(
          'type'  => $type,
          'value' => $value
        );
      }

      if($type == 'string' && empty($value))
      {
        return array(
          'type'  => 'NULL',
          'value' => $value
        );
      }

      if($type == 'string' && strlen($value) > $max_length)
      {
        return array(
          'type'  => 'blob',
          'value' => $value
        );
      }

      if($type == 'string' && substr($value, 0, 1) === '0')
      {
        return array(
          'type'  => 'string',
          'value' => $value
        );
      }

      if($type == 'string' && is_numeric($value))
      {
        $int = (int)$value;
        $float = (float)$value;

        if($int == $value)
        {
          $value = $int;
          $type = 'integer';
        }
        elseif($float == $value)
        {
          $value = $float;
          $type = 'double';
        }
      }
      elseif($type == 'string')
      {
        $type = 'string';
      }
      else
      {
        $type = 'blob';
      }

      return array(
        'type'  => $type,
        'value' => $value
      );
    }
  }
