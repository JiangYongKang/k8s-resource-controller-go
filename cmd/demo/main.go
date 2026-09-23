// Command demo 在进程内演示本地 K8s 风格控制器：
// 创建（带前两次对账失败）-> 观察指数退避重试 -> Ready -> 删除（终结器）。
// 运行期间收到 SIGINT/SIGTERM 会触发优雅退出：控制器把已接受事件全部
// 处理完后进程才结束（事件不丢失）。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/highcumontoa/k8s-resource-controller-go/controller"
)

func main() {
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)

	cfg := controller.DefaultConfig()
	cfg.BaseBackoff = 20 * time.Millisecond
	cfg.MaxBackoff = 320 * time.Millisecond

	// 统计副作用实际执行次数，验证去重。
	hits := map[string]int{}
	controller.SetSideEffectHook(func(phase string, obj *controller.Object) {
		key := phase + ":" + obj.Name
		hits[key]++
		logger.Printf(">>> side effect executed phase=%s name=%s times=%d", phase, obj.Name, hits[key])
	})

	c := controller.New(controller.NewStore(), nil, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runDone := make(chan struct{})
	go func() {
		c.Run(ctx, 2)
		close(runDone)
	}()

	obj, rv, err := c.CreateWithName("demo", controller.ObjectSpec{Message: "hello", FailTimes: 2})
	if err != nil {
		logger.Fatalf("create: %v", err)
	}
	logger.Printf("created name=%s rv=%d (first two reconciles will fail with exponential backoff)", obj.Name, rv)

	if _, ok := c.WaitForPhase("demo", controller.PhaseReady, 3*time.Second); !ok {
		logger.Fatal("object never became Ready")
	}
	summary, _ := c.Summary("demo")
	logger.Printf("summary: phase=%s ready=%v sideEffectDone=%v attempts=%d",
		summary.Phase, summary.Ready, summary.SideEffectDone, summary.Attempts)

	// 发起删除：先进入 Terminating，终结完成后彻底移除。
	_, rv, _ = c.Get("demo")
	if _, _, err := c.Delete("demo", rv); err != nil {
		logger.Fatalf("delete: %v", err)
	}
	logger.Printf("delete requested rv=%d -> Terminating (still readable until finalizers clear)", rv)

	if !c.WaitForRemoved("demo", 2*time.Second) {
		logger.Fatal("object not removed after finalize")
	}
	logger.Printf("object removed; side-effect counts=%v", fmt.Sprint(hits))

	// 演示完成：通知控制器优雅退出并等待其处理完已接受事件。
	logger.Print("demo workflow finished; shutting down controller gracefully")
	stop()
	select {
	case <-runDone:
		logger.Print("controller drained accepted events and stopped; bye")
	case <-time.After(3 * time.Second):
		logger.Fatal("controller did not stop in time")
	}
}
