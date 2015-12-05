package com.datastax.spark.connector.writer

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}

import com.datastax.driver.core.ProtocolVersion
import com.datastax.spark.connector.types.{CassandraOption, Unset}

class BoundStatementBuilderSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session, ks)
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".tab (id INT PRIMARY KEY, value TEXT)""")
  }
  val cluster = conn.withClusterDo(c => c)

  val schema = Schema.fromCassandra(conn, Some(ks), Some("tab"))
  val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, CassandraOption[String])]
    .rowWriter(schema.tables.head, IndexedSeq("id", "value"))
  val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "value"))
  val ps = conn.withSessionDo( session =>
    session.prepare(s"""INSERT INTO "$ks".tab (id, value) VALUES (?, ?) """))

  val protocolVersion = conn.withClusterDo(cluster => cluster.getConfiguration.getProtocolOptions.getProtocolVersion)

  "BoundStatementBuilder" should "ignore Unset values if ProtocolVersion > 3" in {
    val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = ProtocolVersion.V4)
    val x = bsb.bind((1, CassandraOption.Unset))
    x.isSet("value") should be (false)
  }

  it should "set Unset values to null if ProtocolVersion <= 3" in {
    val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = ProtocolVersion.V3)
    val x = bsb.bind((1, CassandraOption.Unset))
    x.isNull("value") should be (true)
  }
}
