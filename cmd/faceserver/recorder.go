package main

import (
	"strings"

	"github.com/fagongzi/log"
	"github.com/pkg/errors"
	nsq "github.com/youzan/go-nsq"
	"golang.org/x/net/context"
)

type Recorder struct {
	nsqlookupdURLs string
	topic          string
	visitCh        <-chan *Visit
	tpm            *nsq.TopicProducerMgr
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewRecorder(nsqlookupdURLs string, topic string, visitCh <-chan *Visit) (rcd *Recorder, err error) {
	rcd = &Recorder{
		nsqlookupdURLs: nsqlookupdURLs,
		topic:          topic,
		visitCh:        visitCh,
	}
	var tpm *nsq.TopicProducerMgr
	nc := nsq.NewConfig()
	tpm, err = nsq.NewTopicProducerMgr([]string{topic}, nc)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	tpm.AddLookupdNodes(strings.Split(nsqlookupdURLs, ","))
	return
}

func (this *Recorder) Start() {
	if this.ctx != nil {
		return
	}
	this.ctx, this.cancel = context.WithCancel(context.Background())
	go func() {
		var pr *PredResp
		var err error
		for {
			select {
			case <-this.ctx.Done():
				return
			case visit := <-this.visitCh:
				if err = this.do(visit); err != nil {
					log.Errorf("%+v", err)
					continue
				}
				log.Debug(pr)
			}
		}

	}()
}

func (this *Recorder) Stop() {
	if this.ctx == nil {
		return
	}
	this.cancel()
	this.tpm.Stop()

	this.ctx = nil
	this.cancel = nil
}

func (this *Recorder) do(v *Visit) (err error) {
	var data []byte
	if data, err = v.Marshal(); err != nil {
		err = errors.Wrapf(err, "v: %+v", v)
		return
	}
	if err = this.tpm.Publish(this.topic, data); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}
