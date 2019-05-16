package cfg_util

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/globalsign/mgo"
	"github.com/gomodule/redigo/redis"
	config "github.com/i11cn/go_config"
	"github.com/lib/pq"
)

type (
	DBConfig interface {
		GetMongoSession() (*mgo.Session, error)
		GetPostgreSQL() (*sql.DB, error)
		GetMysql() (*sql.DB, error)
		GetRedis() (redis.Conn, error)
	}

	db_config struct {
		mongo *mgo.Session
		pg    *sql.DB
		mysql *sql.DB
		redis *redis.Pool
	}
)

func (db *db_config) GetMongoSession() (*mgo.Session, error) {
	if db.mongo == nil {
		return nil, fmt.Errorf("MongoDB未初始化")
	}
	return db.mongo.Clone(), nil
}

func (db *db_config) GetPostgreSQL() (*sql.DB, error) {
	if db.pg == nil {
		return nil, fmt.Errorf("PostgreSQL未初始化")
	}
	return db.pg, nil
}

func (db *db_config) GetMysql() (*sql.DB, error) {
	if db.mysql == nil {
		return nil, fmt.Errorf("MySQL未初始化")
	}
	return db.mysql, nil
}

func (db *db_config) GetRedis() (redis.Conn, error) {
	if db.redis == nil {
		return nil, fmt.Errorf("Redis未初始化")
	}
	return db.redis.Get(), nil
}

func create_mongodb(cfg config.Config) (*mgo.Session, error) {
	val := ""
	if err := cfg.GetAs(&val, "url"); err == nil {
		if di, err := mgo.ParseURL(val); err != nil {
			return nil, err
		} else {
			return mgo.DialWithInfo(di)
		}
	}
	di, _ := mgo.ParseURL("localhost")
	di.Addrs = di.Addrs[0:0]
	hosts := cfg.SubArray("hosts")
	if hosts == nil || len(hosts) == 0 {
		return nil, fmt.Errorf("MongoDB配置中必须设置MongoDB的服务器地址")
	}
	for _, h := range hosts {
		addr := ""
		port := 0
		if err := h.GetAs(&addr, "addr"); err != nil {
			return nil, err
		}
		if err := h.Get(&port, "port"); err != nil {
			port = 0
		}
		if port != 0 && port != 27017 {
			addr = fmt.Sprintf("%s:%d", addr, port)
		}
		di.Addrs = append(di.Addrs, addr)
	}
	cfg.GetAs(&di.Database, "db")
	cfg.GetAs(&di.Username, "username")
	cfg.GetAs(&di.Password, "password")
	cfg.GetAs(&di.Mechanism, "protocol")
	if pool := cfg.SubConfig("pool"); pool != nil {
		pool.Get(&di.PoolLimit, "limit")
		pool.Get(&di.MinPoolSize, "min")
		pool.Get(&di.MaxIdleTimeMS, "max_idle")
	}
	if timeout := cfg.SubConfig("timeout"); timeout != nil {
		if err := timeout.Get(&val, "pool"); err == nil {
			if di.PoolTimeout, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
		if err := timeout.Get(&val, "read"); err == nil {
			if di.ReadTimeout, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
		if err := timeout.Get(&val, "write"); err == nil {
			if di.WriteTimeout, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
	}
	return mgo.DialWithInfo(di)
}

func escape_pg_str(str string) string {
	if strings.Index(str, "\\") != -1 {
		str = strings.ReplaceAll(str, "\\", "\\\\")
	}
	if strings.Index(str, "'") != -1 {
		str = strings.ReplaceAll(str, "'", "\\'")
	}
	if strings.Index(str, " ") != -1 {
		str = fmt.Sprintf("'%s'", str)
	}
	return str
}

func create_pgsql(cfg config.Config) (*sql.DB, error) {
	val := ""
	if cfg.GetAs(&val, "url") == nil {
		if _, err := pq.ParseURL(val); err != nil {
			return nil, err
		}
		return sql.Open("postgres", val)
	}
	args := make([]string, 0, 10)
	add_arg := func(key, value string) {
		args = append(args, fmt.Sprintf("%s=%s", key, escape_pg_str(value)))
	}
	if host := cfg.SubConfig("host"); host == nil {
		return nil, fmt.Errorf("Postgresql配置信息中必须设置服务器信息")
	} else {
		if err := host.GetAs(&val, "addr"); err != nil {
			return nil, fmt.Errorf("PostgreSQL配置中必须设置服务器地址")
		}
		add_arg("host", val)
		port := 0
		if host.Get(&port, "port") == nil && port != 0 && port != 5432 {
			args = append(args, fmt.Sprintf("port=%d", port))
		}
	}
	if cfg.GetAs(&val, "db") == nil {
		add_arg("dbname", val)
	}
	if cfg.GetAs(&val, "username") == nil {
		add_arg("user", val)
	}
	if cfg.GetAs(&val, "password") == nil {
		add_arg("password", val)
	}
	if ssl := cfg.SubConfig("ssl"); ssl != nil {
		if ssl.GetAs(&val, "mode") == nil {
			add_arg("sslmode", val)
		}
		if ssl.GetAs(&val, "cert") == nil {
			add_arg("sslcert", val)
		}
		if ssl.GetAs(&val, "key") == nil {
			add_arg("sslkey", val)
		}
		if ssl.GetAs(&val, "root") == nil {
			add_arg("sslrootcert", val)
		}
	}
	return sql.Open("postgres", strings.Join(args, " "))
}

func create_mysql(cfg config.Config) (*sql.DB, error) {
	val := ""
	if err := cfg.GetAs(&val, "url"); err == nil {
		if _, err := mysql.ParseDSN(val); err != nil {
			return nil, err
		}
		return sql.Open("mysql", val)
	}
	mc, _ := mysql.ParseDSN("")
	if host := cfg.SubConfig("host"); host == nil {
		return nil, fmt.Errorf("MySQL配置中必须设置服务器的地址信息")
	} else {
		if err := host.GetAs(&val, "addr"); err != nil {
			return nil, fmt.Errorf("MySQL配置中必须设置服务器的地址")
		}
		port := 0
		if err := host.Get(&port, "port"); err == nil {
			val = fmt.Sprintf("%s:%d", val, port)
		}
		mc.Addr = val
		mc.Net = "tcp"
		host.GetAs(&mc.Net, "net")
	}
	if err := cfg.GetAs(&val, "charset"); err != nil {
		mc.Params["Charset"] = val
	}
	cfg.GetAs(&mc.User, "username")
	cfg.GetAs(&mc.Passwd, "password")
	cfg.GetAs(&mc.DBName, "db")
	cfg.GetAs(&mc.Collation, "collation")
	cfg.Get(&mc.ClientFoundRows, "match_rows")
	if err := cfg.GetAs(&val, "location"); err == nil {
		if strings.ToUpper(val) == "LOCAL" {
			val = "Local"
		}
		if loc, err := time.LoadLocation(val); err != nil {
			return nil, err
		} else {
			mc.Loc = loc
		}
	}
	cfg.Get(&mc.ParseTime, "parse_time")
	if timeout := cfg.SubConfig("timeout"); timeout != nil {
		if err := timeout.GetAs(&val, "conn"); err == nil {
			if mc.Timeout, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
		if err := timeout.GetAs(&val, "read"); err == nil {
			if mc.ReadTimeout, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
		if err := timeout.GetAs(&val, "write"); err == nil {
			if mc.WriteTimeout, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
	}
	return sql.Open("mysql", mc.FormatDSN())
}

func create_redis(cfg config.Config) (*redis.Pool, error) {
	val := ""
	ival := 0
	ret := &redis.Pool{}
	if pool := cfg.SubConfig("pool"); pool != nil {
		if err := pool.GetAs(&val, "lifetime"); err == nil {
			if ret.MaxConnLifetime, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
		if err := pool.GetAs(&val, "idletime"); err == nil {
			if ret.IdleTimeout, err = time.ParseDuration(val); err != nil {
				return nil, err
			}
		}
		pool.Get(&ret.MaxIdle, "maxidle")
		pool.Get(&ret.MaxActive, "maxactive")
	}
	if err := cfg.GetAs(&val, "url"); err == nil {
		ret.Dial = func() (redis.Conn, error) {
			return redis.DialURL(val)
		}
		return ret, nil
	}
	net := "tcp"
	addr := ""
	if host := cfg.SubConfig("host"); host == nil {
		return nil, fmt.Errorf("Redis配置中必须设置服务器的地址信息")
	} else {
		if err := host.GetAs(&val, "addr"); err != nil {
			return nil, fmt.Errorf("Redis配置中必须设置服务器的地址信息")
		}
		host.GetAs(&net, "net")
		port := 6379
		if err := host.Get(&port, "port"); err != nil {
			port = 6379
		}
		if port == 0 {
			port = 6379
		}
		addr = fmt.Sprintf("%s:%d", val, port)
	}
	opts := make([]redis.DialOption, 0, 5)
	if err := cfg.GetAs(&val, "client"); err == nil {
		// opts = append(opts, redis.DialClientName(val))
	}
	if err := cfg.Get(&ival, "database"); err == nil {
		opts = append(opts, redis.DialDatabase(ival))
	}
	if err := cfg.GetAs(&val, "password"); err == nil {
		opts = append(opts, redis.DialPassword(val))
	}
	if timeout := cfg.SubConfig("timeout"); timeout != nil {
		if err := timeout.GetAs(&val, "keepalive"); err != nil {
			if dur, err := time.ParseDuration(val); err != nil {
				return nil, err
			} else {
				opts = append(opts, redis.DialKeepAlive(dur))
			}
		}
		if err := timeout.GetAs(&val, "conn"); err != nil {
			if dur, err := time.ParseDuration(val); err != nil {
				return nil, err
			} else {
				opts = append(opts, redis.DialConnectTimeout(dur))
			}
		}
		if err := timeout.GetAs(&val, "read"); err != nil {
			if dur, err := time.ParseDuration(val); err != nil {
				return nil, err
			} else {
				opts = append(opts, redis.DialReadTimeout(dur))
			}
		}
		if err := timeout.GetAs(&val, "write"); err != nil {
			if dur, err := time.ParseDuration(val); err != nil {
				return nil, err
			} else {
				opts = append(opts, redis.DialWriteTimeout(dur))
			}
		}
	}
	ret.Dial = func() (redis.Conn, error) {
		return redis.Dial(net, addr, opts...)
	}
	return ret, nil
}

func DBFromConfig(cfg config.Config) (DBConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("配置文件无效: nil")
	}
	subs := cfg.SubArray("")
	ret := &db_config{}
	for _, sub := range subs {
		typ := ""
		if err := sub.Get(&typ, "type"); err != nil {
			return nil, err
		}
		var err error
		switch strings.ToUpper(typ) {
		case "MONGODB":
			if ret.mongo, err = create_mongodb(sub); err != nil {
				return nil, err
			}
		case "POSTGRESQL":
			if db, err := create_pgsql(sub); err != nil {
				return nil, err
			} else if err = db.Ping(); err != nil {
				return nil, err
			} else {
				ret.pg = db
			}
		case "MYSQL":
			if db, err := create_mysql(sub); err != nil {
				return nil, err
			} else if err = db.Ping(); err != nil {
				return nil, err
			} else {
				ret.mysql = db
			}
		case "REDIS":
			if ret.redis, err = create_redis(sub); err != nil {
				return nil, err
			}
		}
	}
	return ret, nil
}

func DBFromYaml(in []byte) (DBConfig, error) {
	if cfg, err := config.NewConfig().LoadYaml(in); err != nil {
		return nil, err
	} else {
		return DBFromConfig(cfg)
	}
}

func DBFromYamlFile(file string) (DBConfig, error) {
	if cfg, err := config.NewConfig().LoadYamlFile(file); err != nil {
		return nil, err
	} else {
		return DBFromConfig(cfg)
	}
}

func DBFromJson(in []byte) (DBConfig, error) {
	if cfg, err := config.NewConfig().LoadJson(in); err != nil {
		return nil, err
	} else {
		return DBFromConfig(cfg)
	}
}

func DBFromJsonFile(file string) (DBConfig, error) {
	if cfg, err := config.NewConfig().LoadJsonFile(file); err != nil {
		return nil, err
	} else {
		return DBFromConfig(cfg)
	}
}

func mongo_config_stub() string {
	return `type: mongodb  # 必须设置，设置配置的数据库类型，MongoDB的类型填写为mongodb，不区分大小写
url: mongodb://user:pass@localhost:10000,192.168.100.100/db  # 以url形式的连接字符串，可选，如果设置了url，则忽略其他所有设置项
db: database  # 设置默认的数据库名，如果在Session.DB()方法不带参数时，会使用该数据库，可选参数
hosts:  # 必选项，设置服务器的地址信息，数组格式，可以包含多个，即使是localhost，也必须在这里明确设置，不得省略
  - addr: localhost  # 必选项，服务器的地址
    port: 27017  # 服务器的端口，可选项，0和默认都代表27017
username: user  # 可选
password: pass  # 可选
protocol: protocol  # 定义协商凭证的协议，可选项，默认为 MONGODB-CR
pool:  # 连接池的相关设置，可选
  limit: 0  # 定义针对每个服务器，连接池所持有socket的限制
  min: 0  # 定义连接池中最小连接数，可选，默认为0
  max_idle: 0  # 连接池中连接的最大空闲毫秒数，可选，默认为0
timeout:  # 定义超时相关参数
  pool: 0s  # 从连接池中获取连接的超时时间，可选，默认为0s，具体格式可以参考time.Duration
  read: 0s  # 读取数据的超时时间，可选，默认为0s，具体格式可以参考time.Duration
  write: 0s  # 写入数据的超时时间，可选，默认为0s，具体格式可以参考time.Duration`
}

func pgsql_config_stub() string {
	return `type: postgresql  # 必须设置，设置配置的数据库类型，PostgreSQL的类型填写为postgresql，不区分大小写
url: postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full  # 以url形式的连接字符串，可选，如果设置了url，则忽略其他所有设置项
host:  # 必选项，设置服务器的地址信息，只能配置单个服务器，即使是localhost，也必须在这里明确设置，不得省略
  addr: localhost  # 必选，服务器的地址
  port: 5432  # 服务器的端口，可选，0或默认代表5432
db: database  # 数据库的库名，可选
username: user  # 可选
password: pass  # 可选
ssl:  # 服务器连接的ssl设置，可选
  mode: 连接的ssl模式，可选，默认为 require
  # mode的取值说明：disable(无SSL)、require(始终SSL，跳过证书验证)、verify-ca、verify-full
  cert: /ssl/cert.pem  # Cert文件的路径
  key: /ssl/key.pem  # Key文件的路径
  root: /ssl/root.pem  # RootCert文件的路径`
}

func mysql_config_stub() string {
	return `type: mysql  # 必须设置，设置配置的数据库类型，MySQL的类型填写为mysql，不区分大小写
url: user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname?charset=utf-8&timeout=90s&collation=utf8mb4_unicode_ci  # 以url形式的连接字符串，可选，如果设置了url，则忽略其他所有设置项
host:  # 必选项，设置服务器的地址信息，只能配置单个服务器，即使是localhost，也必须在这里明确设置，不得省略
  addr: localhost  # 必选，服务器的地址
  port: 3306  # 服务器的端口，可选，0或默认代表3306
  net: tcp  # 可选，网络连接类型，可以设置为tcp、unix等值,默认为tcp
username: user  # 自己看着办吧
password: pass  # 自己看着办吧
db: database  # 可选，连接数据库的库名
charset: utf8  # 可选，连接使用的字符集
collation: utf8mb4_unicode_ci  # 可选，校对字符集")
match_rows: false  # 可选，返回匹配行数，而不是改变行数，默认为 false
location: local  # 可选，设置默认时区，默认为 UTC，设置为local代表本地时区，不区分大小写
parse_time: false  # 可选，设置是否将数据库中的time、timestamp等类型解析为time.Time，默认为false
timeout:  # 可选，超时设置
  conn: 0s  # 可选，连接超时，默认为0，具体格式可以参考time.Duration
  read: 0s  # 可选，读取数据超时，默认为0，具体格式可以参考time.Duration
  write: 0s  # 可选，写入数据超时，默认为0，具体格式可以参考time.Duration`
}

func redis_config_stub() string {
	return `type: redis  # 必须设置，设置配置的数据库类型，Redis的类型填写为redis，不区分大小写
url: ...  # 以url形式的连接字符串，可选，如果设置了url，则忽略其他所有设置项，url的格式可以参考 https://www.iana.org/assignments/uri-schemes/prov/redis
host:  # 必选项，设置服务器的地址信息，只能配置单个服务器，即使是localhost，也必须在这里明确设置，不得省略
  addr: localhost  # 必选，服务器的地址
  port: 6379  # 服务器的端口，可选，0或默认代表6379
  net: tcp  # 可选，网络连接类型，可以设置为tcp、unix等值,默认为tcp
db: database  # 可选，连接数据库的库名
client: sample  # 可选，客户端的名称
password: pass  # 可选
tls:  # 可选，连接的tls设置，由于目前还没有实现完整，不建议使用
  mode: disable  # 可选，设置是否需要tls，默认为disable，require表示需要，但是不校验证书，verify表示需要且校验证书
pool:  # 可选，连接池的相关设置
  lifetime: 0s  # 可选，设置单个连接的最大存活时长，默认0s，表示不计时长，具体格式可以参考time.Duration
  idletime: 0s  # 可选，设置单个连接的最大空闲时长，超时后关闭连接，默认0s，表示不计空闲时长，具体格式可以参考time.Duration
  maxidle: 0  # 可选，设置最大空闲连接数，默认为0，不限制连接数
  maxactive: 0  # 可选，设置最大活动连接数，默认为0，不限制连接数
timeout:  # 可选，超时设置
  keepalive: 5m  # 可选，连接保活的超时时间，默认为5分钟，具体格式可以参考time.Duration
  conn: 0s  # 可选，连接超时，默认为0，具体格式可以参考time.Duration
  read: 0s  # 可选，读取数据超时，默认为0，具体格式可以参考time.Duration
  write: 0s  # 可选，写入数据超时，默认为0，具体格式可以参考time.Duration`
}

func GenConfigStub(db ...string) string {
	if len(db) == 0 {
		return ""
	}
	buf := &bytes.Buffer{}
	buf.WriteString("database:\n")
	for _, t := range db {
		var stub string
		switch strings.ToUpper(t) {
		case "MONGODB":
			stub = mongo_config_stub()
		case "POSTGRESQL":
			stub = pgsql_config_stub()
		case "MYSQL":
			stub = mysql_config_stub()
		case "REDIS":
			stub = redis_config_stub()
		}
		if len(stub) > 0 {
			lines := strings.Split(stub, "\n")
			for index, line := range lines {
				if index == 0 {
					buf.WriteString("  - ")
				} else {
					buf.WriteString("    ")
				}
				buf.WriteString(line)
				buf.WriteString("\n")
			}
		}
	}
	return buf.String()
}
