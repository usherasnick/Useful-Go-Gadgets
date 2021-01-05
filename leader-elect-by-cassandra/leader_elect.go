package leaderelect

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog/log"
)

const (
	__StatusAcquiring int32 = 0
	__StatusMaster    int32 = 1
	__StatusResigned  int32 = 2

	__DefaultTimeout = 2 * time.Second
	__DefaultTTL     = 10
)

// Role 节点角色
type Role int

const (
	// Follower 从节点
	Follower Role = iota
	// Leader 主节点
	Leader
)

// Status 节点状态
type Status struct {
	LeaderID      string
	LeaderAddress string
	Role          Role
}

// Elector 用于选主
type Elector struct {
	cfg *ElectorCfg

	session *gocql.Session
	status  int32
	ev      chan Status
	dead    chan struct{}
}

// ElectorCfg 选主配置
type ElectorCfg struct {
	CassandraEndpoints []string `json:"cassandra_endpoints"`
	NodeID             string   `json:"node_id"`
	AdvertiseAddress   string   `json:"advertise_address"`
	Heartbeat          int      `json:"heartbeat"`
	Keyspace           string   `json:"keyspace"`
	TableName          string   `json:"table_name"`
	ResourceName       string   `json:"resource_name"`
}

// NewConfig 生成默认配置.
// CREATE KEYSPACE IF NOT EXISTS leader_elect_by_cassandra WITH replication = {'class':'SimpleStrategy', 'replication_factor': 3};
func NewConfig(node string, resource string) *ElectorCfg {
	return &ElectorCfg{
		NodeID:       node,
		Heartbeat:    10,
		Keyspace:     "leader_elect_by_cassandra",
		TableName:    "leader_elect",
		ResourceName: resource,
	}
}

// NewElector 返回Elector实例.
func NewElector(cfg *ElectorCfg) *Elector {
	if cfg.Keyspace == "" {
		panic("no keyspace")
	}

	clusterCfg := gocql.NewCluster(cfg.CassandraEndpoints...)
	clusterCfg.ConnectTimeout = __DefaultTimeout
	clusterCfg.Timeout = __DefaultTimeout
	clusterCfg.Keyspace = cfg.Keyspace

	session, err := clusterCfg.CreateSession()
	if err != nil {
		log.Fatal().Err(err)
	}

	e := Elector{
		cfg:     cfg,
		session: session,
		ev:      make(chan Status, 1000),
		dead:    make(chan struct{}),
	}
	err = e.createTable(__DefaultTTL)
	if err != nil {
		log.Fatal().Err(err)
	}

	return &e
}

// NewElectorWithSession 返回Elector实例.
func NewElectorWithSession(cfg *ElectorCfg, session *gocql.Session) *Elector {
	if cfg.Keyspace == "" {
		panic("no keyspace")
	}
	return &Elector{
		cfg:     cfg,
		session: session,
		ev:      make(chan Status, 1000),
		dead:    make(chan struct{}),
	}
}

func (e *Elector) createTable(ttl int) error {
	cql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		resource_name text PRIMARY KEY,
		leader_id text,
		value text
	) with default_time_to_live = %d`, e.cfg.Keyspace, e.cfg.TableName, ttl)
	return e.session.Query(cql).Exec()
}

// Start 开始选举.
func (e *Elector) Start() {
	go e.elect()
}

func (e *Elector) elect() {
ELECT_LOOP:
	for {
		s := atomic.LoadInt32(&e.status)
		switch s {
		case __StatusAcquiring:
			{
				status, err := e.createLease()
				if err != nil {
					log.Warn().Err(err).Msg("failed to acquire lease")
					e.sleep(false)
					continue
				}
				e.ev <- status
				if status.Role == Leader {
					if !atomic.CompareAndSwapInt32(&e.status, s, __StatusMaster) {
						continue
					}
				}
				e.sleep(status.Role == Leader)
			}
		case __StatusMaster:
			{
				status, err := e.updateLease()
				if err != nil {
					log.Warn().Err(err).Msg("failed to acquire lease for master, downgrade to follower")
					status = Status{Role: Follower}
				}
				e.ev <- status
				if status.Role != Leader {
					if !atomic.CompareAndSwapInt32(&e.status, s, __StatusAcquiring) {
						continue
					}
				}
				e.sleep(status.Role == Leader)
			}
		case __StatusResigned:
			{
				if err := e.removeLease(); err != nil {
					log.Warn().Err(err).Msg("failed to remove lease")
				}
				break ELECT_LOOP
			}
		default:
			panic("unknown node status")
		}
	}
	log.Info().Msgf("election resigned: %s", e.cfg.NodeID)
	close(e.ev)
	close(e.dead)
}

func (e *Elector) sleep(leader bool) {
	sec := e.cfg.Heartbeat
	if leader {
		sec = (e.cfg.Heartbeat - 1) / 2
	}

	deadline := time.Now().Add(time.Duration(sec) * time.Second)
SLEEP_LOOP:
	for {
		s := atomic.LoadInt32(&e.status)
		if s == __StatusResigned {
			break SLEEP_LOOP
		}
		now := time.Now()
		if deadline.Before(now) {
			break SLEEP_LOOP
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *Elector) createLease() (Status, error) {
	cql := fmt.Sprintf(`INSERT INTO %s.%s (resource_name, leader_id, value) VALUES (?,?,?) IF NOT EXISTS`,
		e.cfg.Keyspace, e.cfg.TableName)
	q := e.session.Query(cql, e.cfg.ResourceName, e.cfg.NodeID, e.cfg.AdvertiseAddress)
	defer q.Release()

	var rn, id, val string
	applied, err := q.ScanCAS(&rn, &id, &val)
	if err != nil {
		return Status{}, err
	}
	return e.statusFromDB(applied, id, val), nil
}

func (e *Elector) updateLease() (Status, error) {
	cql := fmt.Sprintf(`UPDATE %s.%s SET leader_id = ?, value = ? WHERE resource_name = ? IF leader_id = ?`,
		e.cfg.Keyspace, e.cfg.TableName)
	q := e.session.Query(cql, e.cfg.NodeID, e.cfg.AdvertiseAddress, e.cfg.ResourceName, e.cfg.NodeID)
	defer q.Release()

	var id, val string
	applied, err := q.ScanCAS(&id, &val)
	if err != nil {
		return Status{}, err
	}
	return e.statusFromDB(applied, id, val), nil
}

func (e *Elector) removeLease() error {
	cql := fmt.Sprintf(`DELETE FROM %s.%s WHERE resource_name = ? IF leader_id = ?`,
		e.cfg.Keyspace, e.cfg.TableName)
	q := e.session.Query(cql, e.cfg.ResourceName, e.cfg.NodeID)
	defer q.Release()

	var id string
	_, err := q.ScanCAS(&id)
	return err
}

func (e *Elector) statusFromDB(applied bool, id string, val string) Status {
	if applied || id == e.NodeID() {
		return Status{
			LeaderID:      e.cfg.NodeID,
			LeaderAddress: e.cfg.AdvertiseAddress,
			Role:          Leader,
		}
	}
	return Status{
		LeaderID:      id,
		LeaderAddress: val,
		Role:          Follower,
	}
}

// NodeID 显示当前选举节点的ID.
func (e *Elector) NodeID() string {
	return e.cfg.NodeID
}

// Resign 退出选举.
func (e *Elector) Resign() {
	atomic.StoreInt32(&e.status, __StatusResigned)
	<-e.dead
}

// Status 用于显示当前选举节点的状态.
func (e *Elector) Status() <-chan Status {
	return e.ev
}
