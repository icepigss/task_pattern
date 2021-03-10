package pattern1

import (
	"fmt"
	"time"

	"testing"
)

func TestFlow(t *testing.T) {
	f := NewFlow(10)
	f.SetWorkerFn(func(payload interface{}) interface{} {
		r := payload.(int)

		fmt.Printf("worker runing, payload: %d.\n", r)
		time.Sleep(time.Second)

		return r + 1
	})
	f.SetConsumerFn(func(payload interface{}) {
		r := payload.(int)

		fmt.Printf("consumer runing, payload: %d.\n", r)
		time.Sleep(time.Second)
	})
	f.Start()
	defer f.Close()

	for i := 1; i < 10; i++ {
		f.Row(i)
	}
}
