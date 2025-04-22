package lmstfy

import (
	"context"
	"fmt"
	"github.com/hxx258456/kratos-transport-lmstfy/broker/lmstfy"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	api "github.com/tx7do/kratos-transport/testing/api/manual"

	"github.com/tx7do/kratos-transport/broker"
)

const (
	localBroker = "192.168.3.104:7777"
	testTopic   = "test"
)

func handleHygrothermograph(_ context.Context, topic string, headers broker.Headers, msg *api.Hygrothermograph) error {
	log.Infof("Topic %s, Headers: %+v, Payload: %+v\n", topic, headers, msg)
	return nil
}

func TestServer(t *testing.T) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ctx := context.Background()

	srv := NewServer(
		WithAddress(localBroker),
		WithCodec("json"),
		WithToken("01JR8FDAGHHSR3T3Q5741ZKCCT"),
		WithNamespace("saas"),
	)

	err := RegisterSubscriber(srv,
		testTopic,
		handleHygrothermograph,
		lmstfy.WithTTR(3),
		lmstfy.WithTimeout(10),
	)
	assert.Nil(t, err)

	//if err := srv.Connect(); err != nil {
	//	panic(err)
	//}

	if err := srv.Start(ctx); err != nil {
		panic(err)
	}

	defer func() {
		if err := srv.Stop(ctx); err != nil {
			t.Errorf("expected nil got %v", err)
		}
	}()

	<-interrupt
}

func TestClient(t *testing.T) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ctx := context.Background()

	b := lmstfy.NewBroker(
		broker.WithAddress(localBroker),
		broker.WithCodec("json"),
		lmstfy.WithToken("01JR8FDAGHHSR3T3Q5741ZKCCT"),
		lmstfy.WithNamespace("saas"),
	)

	_ = b.Init()

	if err := b.Connect(); err != nil {
		t.Logf("cant connect to broker, skip: %v", err)
		t.Skip()
	}
	defer b.Disconnect()

	var msg api.Hygrothermograph
	const count = 10
	for i := 0; i < count; i++ {
		startTime := time.Now()
		msg.Humidity = float64(rand.Intn(100))
		msg.Temperature = float64(rand.Intn(100))
		err := b.Publish(ctx, testTopic, msg, lmstfy.WithDelay(5), lmstfy.WithTries(3))
		assert.Nil(t, err)
		elapsedTime := time.Since(startTime) / time.Millisecond
		fmt.Printf("Publish %d, elapsed time: %dms, Humidity: %.2f Temperature: %.2f\n",
			i, elapsedTime, msg.Humidity, msg.Temperature)
	}

	fmt.Printf("total send %d messages\n", count)

	<-interrupt
}
