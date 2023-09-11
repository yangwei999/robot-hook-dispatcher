package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	kafka "github.com/opensourceways/kafka-lib/agent"
	"github.com/opensourceways/server-common-lib/utils"
	"github.com/sirupsen/logrus"
)

const (
	headerUserAgent = "User-Agent"
)

type dispatcher struct {
	hc             utils.HttpClient
	topic          string
	endpoint       string
	userAgent      string
	concurrentSize func() (int, error)

	startTime time.Time
	sentNum   int
}

type message struct {
	Body   []byte
	Header map[string]string
}

func newDispatcher(
	cfg *configuration,
	concurrentSize func() (int, error),
) (*dispatcher, error) {
	return &dispatcher{
		hc:             utils.NewHttpClient(3),
		topic:          cfg.Topic,
		endpoint:       cfg.AccessEndpoint,
		userAgent:      cfg.UserAgent,
		concurrentSize: concurrentSize,
	}, nil
}

func (d *dispatcher) run(ctx context.Context) error {
	if err := kafka.Subscribe(component, d.handle, []string{d.topic}); err != nil {
		return err
	}

	<-ctx.Done()

	return nil
}

func (d *dispatcher) handle(payload []byte, header map[string]string) error {
	msg := &message{
		Body:   payload,
		Header: header,
	}

	if err := d.validateMessage(msg); err != nil {
		return err
	}

	d.dispatch(msg)

	d.speedControl()

	return nil
}

func (d *dispatcher) speedControl() {
	if d.sentNum == 1 {
		d.startTime = time.Now()

		return
	}

	size, err := d.concurrentSize()
	if err != nil {
		logrus.Errorf("get concurrent size, err:%s", err.Error())

		return
	}

	if size > 0 && d.sentNum >= size {
		now := time.Now()

		if v := d.startTime.Add(time.Second); v.After(now) {
			du := v.Sub(now)
			time.Sleep(du)

			logrus.Debugf(
				"will sleep %s after sending %d events",
				du.String(), d.sentNum,
			)
		} else {
			logrus.Debugf(
				"It took %s to send %d events",
				now.Sub(d.startTime).String(), d.sentNum,
			)
		}

		d.sentNum = 0
	}
}

func (d *dispatcher) validateMessage(msg *message) error {
	if len(msg.Header) == 0 || msg.Header[headerUserAgent] != d.userAgent {
		return errors.New("unexpect message: invalid header")
	}

	if len(msg.Body) == 0 {
		return errors.New("unexpect message: The payload is empty")
	}

	return nil
}

func (d *dispatcher) dispatch(msg *message) {
	if err := d.send(msg); err != nil {
		logrus.Errorf("send message, err:%s", err.Error())
	} else {
		d.sentNum++
	}
}

func (d *dispatcher) send(msg *message) error {
	req, err := http.NewRequest(
		http.MethodPost, d.endpoint, bytes.NewBuffer(msg.Body),
	)
	if err != nil {
		return err
	}

	for k, v := range msg.Header {
		req.Header.Add(k, v)
	}

	_, err = d.hc.ForwardTo(req, nil)

	return err
}
