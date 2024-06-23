package app

import "github.com/core-go/log/zap"

type Config struct {
	Cql Cassandra  `mapstructure:"cassandra"`
	Log log.Config `mapstructure:"log"`
}

type Cassandra struct {
	PublicIp string `mapstructure:"public_ip"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Keyspace string `mapstructure:"keyspace"`
}
