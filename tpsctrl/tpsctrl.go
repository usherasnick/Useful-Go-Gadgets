package tpsctrl

import (
	"time"

	"github.com/juju/ratelimit"
	"github.com/rs/zerolog/log"
)

// TPSController 用于调控TPS
type TPSController struct {
	quota  int
	bucket *ratelimit.Bucket
}

// NewTPSController 返回TPSController实例.
// Max(TPS) == quota
func NewTPSController(quota int) *TPSController {
	ctrl := TPSController{}
	ctrl.quota = quota

	interval := time.Second / time.Duration(ctrl.quota)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ctrl.bucket = ratelimit.NewBucket(interval, int64(ctrl.quota))

	return &ctrl
}

// Take 从事务桶中取1个令牌, 如果当前无可用令牌, 等待y秒时间, 直到出现可用令牌.
func (ctrl *TPSController) Take() {
	if ctrl.bucket == nil {
		return
	}
	waitUntilAvailable := ctrl.bucket.Take(1)
	if waitUntilAvailable != 0 {
		log.Warn().Msgf("tps quota limit exceeds, wait %s secs until resource turns to be available", waitUntilAvailable.String())
		time.Sleep(waitUntilAvailable)
	}
}

// TakeX 从事务桶中取x个令牌, 如果当前无可用令牌, 等待y秒时间, 直到出现可用令牌.
func (ctrl *TPSController) TakeX(x int64) {
	if ctrl.bucket == nil {
		return
	}
	waitUntilAvailable := ctrl.bucket.Take(x)
	if waitUntilAvailable != 0 {
		log.Warn().Msgf("tps quota limit exceeds, wait %s secs until resource turns to be available", waitUntilAvailable.String())
		time.Sleep(waitUntilAvailable)
	}
}
