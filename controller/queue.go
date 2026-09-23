package controller

import (
	"sort"
	"sync"
	"time"
)

// EventQueue 是按对象名去重的内存工作队列（K8s controller 的
// workqueue 风格），并对同一对象提供严格单飞（single-flight）保证：
//
//   - pending：等待处理，每个名字至多一条（重复 Add 折叠，保留代数更新者）；
//   - processing：正在对账的名字集合。名字在 processing 中时绝不会再次
//     从 Drain 弹出，因此任一时刻同一对象至多一个对账；
//   - deferred：processing 期间到达（Add 或重试到期）的事件在此折叠，
//     Done 后原子转入 pending，更新不丢失、不并发；
//   - timers：延迟重试最小堆，由唯一 loop goroutine 等到堆顶到期。
//
// 唤醒模型（不丢信号）：等待只在 cond.Wait（释放锁）中发生；任何状态
// 变化（Add/RetryAfter/Done/堆顶到期）都在持锁时递增 wakeSeq 并
// Broadcast。消费方比较 wakeSeq，因此不会漏掉“开始等待之前”的通知。
//
// 关闭语义（stop 关闭后）：未到期重试立即到期，存量 pending/deferred/
// timers 事件全部消化完、且没有在飞对账时，Drain 返回 ok=false；
// 随后 loop 才退出。即“已接受事件不丢失，处理完才结束”。
type EventQueue struct {
	mu         sync.Mutex
	cond       *sync.Cond
	wakeSeq    uint64
	pending    map[string]Event
	deferred   map[string]Event
	processing map[string]struct{}
	timers     []scheduledEvent
	stopping   bool
	drained    bool
	started    bool
}

type scheduledEvent struct {
	e  Event
	at time.Time
}

// NewEventQueue 创建空队列。
func NewEventQueue() *EventQueue {
	q := &EventQueue{
		pending:    make(map[string]Event),
		deferred:   make(map[string]Event),
		processing: make(map[string]struct{}),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// signalLocked 调用方持锁：递增序号并广播。
func (q *EventQueue) signalLocked() {
	q.wakeSeq++
	q.cond.Broadcast()
}

// Add 加入事件（去重折叠）。关闭后拒绝新事件（已接受事件不受影响）。
func (q *EventQueue) Add(e Event) {
	q.mu.Lock()
	if q.stopping {
		q.mu.Unlock()
		return
	}
	q.addLocked(e)
	q.signalLocked()
	q.mu.Unlock()
}

// addLocked 调用方持锁。
func (q *EventQueue) addLocked(e Event) {
	if _, busy := q.processing[e.Name]; busy {
		q.deferred[e.Name] = newerEvent(q.deferred[e.Name], e)
		return
	}
	if cur, queued := q.pending[e.Name]; queued {
		q.pending[e.Name] = newerEvent(cur, e)
		return
	}
	q.pending[e.Name] = e
}

// RetryAfter 安排 delay 后重新投递事件（关闭后仍接受：属于已接受事件的重试）。
func (q *EventQueue) RetryAfter(e Event, delay time.Duration) {
	q.mu.Lock()
	q.timers = append(q.timers, scheduledEvent{e: e, at: time.Now().Add(delay)})
	sort.Slice(q.timers, func(i, j int) bool {
		return q.timers[i].at.Before(q.timers[j].at)
	})
	q.signalLocked()
	q.mu.Unlock()
}

// loop 是唯一的调度 goroutine：把堆顶到期事件提升为就绪并唤醒 Drain。
// 它只在 drained（收尾彻底完成）后退出，确保关闭时滞留的重试仍被处理。
func (q *EventQueue) loop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	seen := q.wakeSeq
	for {
		if q.drained {
			return
		}
		q.promoteDueLocked(time.Now())

		wait := time.Hour
		if len(q.timers) > 0 {
			if d := time.Until(q.timers[0].at); d > 0 {
				wait = d
			} else {
				wait = 0
			}
		}

		// 定时器在持锁下创建，其回调必须拿同一把锁才能 signal；
		// loop 随后在 Wait 中释放锁，因此到期信号不可能先于等待丢失。
		timer := time.NewTimer(wait)
		go func() {
			<-timer.C
			q.mu.Lock()
			q.signalLocked()
			q.mu.Unlock()
		}()

		for q.wakeSeq == seen && !q.drained {
			q.cond.Wait()
		}
		seen = q.wakeSeq
		timer.Stop()
	}
}

// promoteDueLocked 把到期事件转入 pending/deferred。调用方持锁。
func (q *EventQueue) promoteDueLocked(now time.Time) {
	ready := 0
	for ready < len(q.timers) && !q.timers[ready].at.After(now) {
		se := q.timers[ready]
		ready++
		q.addLocked(se.e)
	}
	q.timers = q.timers[ready:]
}

func (q *EventQueue) popLocked() Event {
	var name string
	var e Event
	for name, e = range q.pending {
		break
	}
	delete(q.pending, name)
	q.processing[name] = struct{}{}
	return e
}

// workRemainingLocked 判断是否还有未完成的已接受工作。
func (q *EventQueue) workRemainingLocked() bool {
	return len(q.pending) > 0 || len(q.deferred) > 0 || len(q.timers) > 0 || len(q.processing) > 0
}

// Drain 阻塞等待并返回一个可处理事件（同时记入 processing）。
func (q *EventQueue) Drain(stop <-chan struct{}) (Event, bool) {
	q.mu.Lock()
	if !q.started {
		q.started = true
		// 唯一的 stop watcher：唤醒阻塞在 cond.Wait 上的消费者与 loop。
		go func() {
			<-stop
			q.mu.Lock()
			q.stopping = true
			q.signalLocked()
			q.mu.Unlock()
		}()
		go q.loop()
	}
	for {
		q.promoteDueLocked(time.Now())
		if len(q.pending) > 0 {
			e := q.popLocked()
			q.mu.Unlock()
			return e, true
		}
		if isClosed(stop) {
			q.stopping = true
			// 收尾：所有未到期重试立即到期。
			for _, se := range q.timers {
				q.addLocked(se.e)
			}
			q.timers = nil
			if len(q.pending) > 0 {
				e := q.popLocked()
				q.signalLocked()
				q.mu.Unlock()
				return e, true
			}
			if !q.workRemainingLocked() {
				q.drained = true
				q.signalLocked() // 同时唤醒其他 worker 与 loop
				q.mu.Unlock()
				return Event{}, false
			}
		}
		q.cond.Wait()
	}
}

// Done 标记一次对账结束：若期间折叠了新事件，此刻才转入 pending。
func (q *EventQueue) Done(e Event) {
	q.mu.Lock()
	delete(q.processing, e.Name)
	if ne, ok := q.deferred[e.Name]; ok {
		delete(q.deferred, e.Name)
		q.pending[e.Name] = ne
	}
	q.signalLocked()
	q.mu.Unlock()
}

// HasInFlight 报告对象是否正在对账（测试观测用）。
func (q *EventQueue) HasInFlight(name string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.processing[name]
	return ok
}

// Stats 返回待处理与在飞计数（测试/日志观测用）。
func (q *EventQueue) Stats() (pending, inFlight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending), len(q.processing)
}

func newerEvent(a, b Event) Event {
	if b.Generation >= a.Generation {
		return b
	}
	return a
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
