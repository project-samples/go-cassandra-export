package app

import (
	"context"
	"github.com/core-go/cassandra/export"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	"path/filepath"
	"reflect"
	"time"
)

const (
	Keyspace = `masterdata`

	CreateKeyspace = `create keyspace if not exists masterdata with replication = {'class':'SimpleStrategy', 'replication_factor':1}`

	CreateTable = `
					create table if not exists users (
					id varchar,
					username varchar,
					email varchar,
					phone varchar,
					date_of_birth date,
					primary key (id)
	)`
)

type ApplicationContext struct {
	Export func(ctx context.Context) error
}

func NewApp(ctx context.Context, config Config) (*ApplicationContext, error) {
	cluster := gocql.NewCluster(config.Cql.PublicIp)
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Timeout = time.Second * 3000
	cluster.ConnectTimeout = time.Second * 3000
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: config.Cql.UserName, Password: config.Cql.Password}
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	err = session.Query(CreateKeyspace).Exec()
	if err != nil {
		return nil, err
	}

	session.Close()
	cluster.Keyspace = Keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	err = session.Query(CreateTable).Exec()
	if err != nil {
		return nil, err
	}

	userType := reflect.TypeOf(User{})
	formatWriter, err := export.NewFixedLengthFormatter(userType)
	if err != nil {
		return nil, err
	}
	writer, err := export.NewFileWriter(GenerateFileName)
	if err != nil {
		return nil, err
	}
	exportService, err := export.NewExporter(cluster, userType, BuildQuery, formatWriter.Format, writer.Write, writer.Close)
	if err != nil {
		return nil, err
	}
	return &ApplicationContext{
		Export: exportService.Export,
	}, nil
}

type User struct {
	Id          string     `json:"id" gorm:"column:id;primary_key" bson:"_id" format:"%011s" length:"11" dynamodbav:"id" firestore:"id" validate:"required,max=40"`
	Username    string     `json:"username" gorm:"column:username" bson:"username" length:"10" dynamodbav:"username" firestore:"username" validate:"required,username,max=100"`
	Email       *string    `json:"email" gorm:"column:email" bson:"email" dynamodbav:"email" firestore:"email" length:"31" validate:"email,max=100"`
	Phone       string     `json:"phone" gorm:"column:phone" bson:"phone" dynamodbav:"phone" firestore:"phone" length:"20" validate:"required,phone,max=18"`
	DateOfBirth *time.Time `json:"dateOfBirth" gorm:"column:date_of_birth" bson:"dateOfBirth" length:"10" format:"dateFormat:2006-01-02" dynamodbav:"dateOfBirth" firestore:"dateOfBirth" avro:"dateOfBirth"`
}

func BuildQuery(ctx context.Context) (string, []interface{}) {
	query := "select id, username, email, phone, date_of_birth from users"
	params := make([]interface{}, 0)
	return query, params
}
func GenerateFileName() string {
	fileName := time.Now().Format("20060102150405") + ".csv"
	fullPath := filepath.Join("export", fileName)
	export.DeleteFile(fullPath)
	return fullPath
}
