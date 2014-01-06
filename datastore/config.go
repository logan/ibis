package datastore

import "strings"

import "tux21b.org/v1/gocql"

type CassandraConfig struct {
	Consistency string
	Keyspace    string
	Node        []string
}

type CassandraConn struct {
	Config        CassandraConfig
	Session       *gocql.Session
	Model         *Schema
	SchemaUpdates *SchemaDiff
}

func DialCassandra(config CassandraConfig) (*CassandraConn, error) {
	var session *gocql.Session
	var err error
	if session, err = makeCluster(config).CreateSession(); err != nil {
		return nil, err
	}
	return &CassandraConn{Config: config, Session: session}, nil
}

func makeCluster(config CassandraConfig) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(config.Node...)
	cluster.Keyspace = config.Keyspace
	cluster.Consistency = parseConsistency(config.Consistency)
	return cluster
}

func parseConsistency(value string) (consistency gocql.Consistency) {
	switch strings.ToLower(value) {
	default:
	case "quorum":
		consistency = gocql.Quorum
	case "any":
		consistency = gocql.Any
	case "one":
		consistency = gocql.One
	case "two":
		consistency = gocql.Two
	case "three":
		consistency = gocql.Three
	case "all":
		consistency = gocql.All
	case "localquorum":
		consistency = gocql.LocalQuorum
	case "eachquorum":
		consistency = gocql.EachQuorum
	case "serial":
		consistency = gocql.Serial
	case "localserial":
		consistency = gocql.LocalSerial
	}
	return
}

func (c *CassandraConn) SetModel(model *Schema) error {
	var err error
	c.Model = model
	c.SchemaUpdates, err = c.DiffLiveSchema()
	return err
}
