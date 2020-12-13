package leaderelect

import (
	"time"

	leaderelection "github.com/Comcast/go-leaderelection"
	"github.com/rs/zerolog/log"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	__ElectionPath      = "/election"
	__LeaderPath        = "/leader"
	__ElectionHeartbeat = 10 * time.Second
)

// Elector 用于选主
type Elector struct {
	cfg          *ElectorCfg
	conn         *zk.Conn
	participator string
	close        chan struct{}
	stop         chan struct{}
}

// ElectorCfg 选主配置
type ElectorCfg struct {
	ZkEndpoints []string `json:"zk_endpoints"`
	Heartbeat   int      `json:"heartbeat"`
}

// NewElector 返回Elector实例.
func NewElector(cfg *ElectorCfg) *Elector {
	return &Elector{
		cfg:   cfg,
		close: make(chan struct{}),
		stop:  make(chan struct{}),
	}
}

// ElectLeader 让participator参与选主.
// TODO: 主节点挂掉后, 从节点
func (e *Elector) ElectLeader(participator string) {
	var heartbeat time.Duration
	if e.cfg.Heartbeat > 0 {
		heartbeat = time.Duration(e.cfg.Heartbeat) * time.Second
	} else {
		heartbeat = __ElectionHeartbeat
	}

	conn, sessionEv, err := zk.Connect(e.cfg.ZkEndpoints, heartbeat)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", participator).Msgf("failed to connect to zookeeper cluster (%v)", e.cfg.ZkEndpoints)
	}
	e.conn = conn
	e.participator = participator

	e.prepare()

	candidate, err := leaderelection.NewElection(e.conn, __ElectionPath, e.participator)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msg("failed to start election for candidate")
	}
	go candidate.ElectLeader()

	fromStop := false
ELECT_LOOP:
	for {
		select {
		case ev, ok := <-sessionEv:
			{
				if !ok {
					log.Warn().Str("[participator]", e.participator).Msg("election channel has been closed, election will terminate")
					break ELECT_LOOP
				} else if ev.State == zk.StateExpired {
					log.Error().Err(zk.ErrSessionExpired).Str("[participator]", e.participator)
					break ELECT_LOOP
				} else if ev.State == zk.StateAuthFailed {
					log.Error().Err(zk.ErrAuthFailed).Str("[participator]", e.participator)
					break ELECT_LOOP
				} else if ev.State == zk.StateUnknown {
					continue
				} else if ev.State.String() == "unknown state" {
					continue
				}
				if ev.State == zk.StateDisconnected {
					log.Warn().Str("[participator]", e.participator).Msgf("zookeeper server (%s) state turns into %s", ev.Server, ev.State.String())
				} else {
					log.Info().Str("[participator]", e.participator).Msgf("zookeeper server (%s) state turns into %s", ev.Server, ev.State.String())
				}
			}
		case status, ok := <-candidate.Status():
			{
				if !ok {
					log.Warn().Str("[participator]", e.participator).Msg("election channel has been closed, election will terminate")
					break ELECT_LOOP
				} else if status.Err != nil {
					log.Error().Err(status.Err).Str("[participator]", e.participator).Msg("candidate received election status error")
					break ELECT_LOOP
				} else {
					log.Info().Str("[participator]", e.participator).Msgf("candidate received status message\n\n%v", status.String())
					if status.Role == leaderelection.Leader {
						e.setLeader(e.participator)
						log.Info().Str("[participator]", e.participator).Msg("candidate has been promoted to leader")
						// TODO: do your stuff
					} else if status.Role == leaderelection.Follower {
						e.getLeader()
						// TODO: do your stuff
					}
				}
			}
		case _, ok := <-e.stop:
			{
				if !ok {
					fromStop = true
					break ELECT_LOOP
				}
			}
		}
	}
	candidate.Resign()
	e.conn.Close()
	if fromStop {
		e.close <- struct{}{}
	}
}

func (e *Elector) prepare() {
	_, err := e.conn.Create(__ElectionPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to create znode (%s)", __ElectionPath)
	}

	_, err = e.conn.Create(__LeaderPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to create znode (%s)", __LeaderPath)
	}
}

func (e *Elector) setLeader(leader string) {
	_, err := e.conn.Set(__LeaderPath, []byte(leader), -1)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to set value for znode (%s)", __LeaderPath)
	}
}

func (e *Elector) getLeader() string {
	v, _, err := e.conn.Get(__LeaderPath)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to get value from znode (%s)", __LeaderPath)
	}

	return string(v)
}

// Close participator主动退出选主.
func (e *Elector) Close() {
	close(e.stop)
	<-e.close
	log.Warn().Str("[participator]", e.participator).Msg("leave")
}
