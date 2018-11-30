package main

import (
	"strings"

	"github.com/pkg/errors"
	nsq "github.com/youzan/go-nsq"
)

type Recorder struct {
	nsqlookupdURLs string
	topic          string
	tpm            *nsq.TopicProducerMgr
}

func NewRecorder(nsqlookupdURLs string, topic string) (rcd *Recorder, err error) {
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
		tpm:            tpm,
	}
	return
}

func (this *Recorder) Recode(v *Visit) (err error) {
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
