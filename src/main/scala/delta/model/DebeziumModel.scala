package delta.model

case class Field(`type`: String, optional: Boolean, field: String)

case class FieldsOuter(`type`: String, fields: Field, optional: Boolean, field: String)

case class Schema(`type`: String, fields: Field, optional: Boolean, name: String)

case class Source(version: String, connector: String, name: String, ts_ms: Long, snapshot: String, db: String, table: String, server_id: Int, gtid: String, file: String, pos: Long, row: Int, thread: Int, query: String)

case class Payload(before: Map[String, String], after: Map[String, String], source: Source, op: String, ts_ms: Long, transaction: String)

case class DebeziumModel(schema: Schema, payload: Payload)
