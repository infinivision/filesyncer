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
}

func NewRecorder(nsqlookupdURLs string, topic string, visitCh <-chan *Visit) (rcd *Recorder, err error) {
	var tpm *nsq.TopicProducerMgr
	nc := nsq.NewConfig()
	tpm, err = nsq.NewTopicProducerMgr([]string{topic}, nc)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	tpm.AddLookupdNodes(strings.Split(nsqlookupdURLs, ","))
	rcd = &Recorder{
		nsqlookupdURLs: nsqlookupdURLs,
		topic:          topic,
		visitCh:        visitCh,
		tpm:            tpm,
	}
	return
}

func (this *Recorder) Serve(ctx context.Context) {
	go func() {
		var pr *PredResp
		var err error
		for {
			select {
			case <-ctx.Done():
				this.tpm.Stop()
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
