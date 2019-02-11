package main

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type Recorder struct {
	mqAddrs  []string
	topic    string
	producer sarama.SyncProducer
}

func NewRecorder(mqAddrs []string, topic string) (rcd *Recorder, err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(mqAddrs, config)
	if err != nil {
		err = errors.Wrap(err, "")
		return nil, err
	}

	rcd = &Recorder{
		mqAddrs:  mqAddrs,
		topic:    topic,
		producer: producer,
	}
	return
}

func (this *Recorder) Record(visits []*Visit) (err error) {
	var data []byte
	for _, v := range visits {
		if data, err = v.Marshal(); err != nil {
			err = errors.Wrapf(err, "v: %+v", v)
			return
		}
		_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{
			Topic: this.topic,
			Value: sarama.ByteEncoder(data),
		})
		if err != nil {
			err = errors.Wrap(err, "")
			return err
		}
	}
	return
}
