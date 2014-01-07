// The datastore package provides a framework for storing data in Cassandra. An application using
// this package defines a Model consisting of Tables, where each Table corresponds to a column
// family in Cassandra. This package then takes care of creating the column families in a given
// keyspace, keeping the schema in sync with the given Model definition, as well as loading and
// storing records in Tables.
package datastore
