package com.datastax.spark.connector.types

/**
  * An object representing a column which will be skipped on insert.
  */
protected[connector] object Unset extends Serializable

/**
  * An option with an extra bit of information to let us know whether a value should be treated
  * as a delete or as an unsetIfNone value. Reading a table using this as an column type will
  * cause all empty values to not be treated as deletes on write.
  */
case class CassandraOption[A](option: Option[A], unsetIfNone: Boolean = true) {
  def UnsetIfNone = copy(option, true)
  def DeleteIfNone = copy(option, false)
}

object CassandraOption {
  def Unset[A]: CassandraOption[A] = CassandraOption[A](None, true)
  def Delete[A]: CassandraOption[A] = CassandraOption[A](None, false)
}
