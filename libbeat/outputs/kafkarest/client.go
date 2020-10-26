// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package kafkarest

import (
	"context"
	"encoding/json"
	"errors"
	//"fmt"
	"strings"
	//"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"

	"github.com/snappyflow/beats/v7/libbeat/common"

	"github.com/snappyflow/beats/v7/libbeat/common/fmtstr"
	//"github.com/snappyflow/beats/v7/libbeat/common/transport"
	"github.com/snappyflow/beats/v7/libbeat/logp"
	"github.com/snappyflow/beats/v7/libbeat/outputs"
	"github.com/snappyflow/beats/v7/libbeat/outputs/codec"
	//"github.com/snappyflow/beats/v7/libbeat/outputs/outil"
	"github.com/snappyflow/beats/v7/libbeat/publisher"
	"github.com/snappyflow/beats/v7/libbeat/testing"
)

type client struct {
	log      *logp.Logger
	observer outputs.Observer
	hosts    []string
	//topic    outil.Selector
	key      *fmtstr.EventFormatString
	index    string
	codec    codec.Codec
	//config   sarama.Config
	config   common.Config
	//mux      sync.Mutex

	//producer sarama.AsyncProducer

	//wg sync.WaitGroup

	token	 string
}

type msgRef struct {
	client *client
	count  int32
	total  int
	failed []publisher.Event
	batch  publisher.Batch

	err error
}

var (
	errNoTopicsSelected = errors.New("no topic could be selected")
)

func newKafkaRestClient(
	observer outputs.Observer,
	hosts []string,
	index string,
	key *fmtstr.EventFormatString,
	writer codec.Codec,
	//cfg *sarama.Config,
	cfg *common.Config,
	token string,
) (*client, error) {
	c := &client{
		log:      logp.NewLogger(logSelector),
		observer: observer,
		hosts:    hosts,
		key:      key,
		index:    strings.ToLower(index),
		codec:    writer,
		config:   *cfg,
		token:	  token,
	}
	return c, nil
}


func (c *client) Connect() error {
/*
	c.mux.Lock()
	defer c.mux.Unlock()

	c.log.Debugf("connect: %v", c.hosts)

	// try to connect
	producer, err := sarama.NewAsyncProducer(c.hosts, &c.config)
	if err != nil {
		c.log.Errorf("Kafka connect fails with: %+v", err)
		return err
	}

	c.producer = producer

	c.wg.Add(2)
	go c.successWorker(producer.Successes())
	go c.errorWorker(producer.Errors())
*/
	return nil
}


func (c *client) Close() error {
/*
	c.mux.Lock()
	defer c.mux.Unlock()
	c.log.Debug("closed kafka client")

	// producer was not created before the close() was called.
	if c.producer == nil {
		return nil
	}

	c.producer.AsyncClose()
	c.wg.Wait()
	c.producer = nil
*/
	return nil
}

func (c *client) Publish(_ context.Context, batch publisher.Batch) error {
	events := batch.Events()
	c.observer.NewBatch(len(events))
	fmt.Println("====================================")
	fmt.Println(events)
	fmt.Println("=================================")
	data := make(map[string][]map[string]interface{})
	eventsRecord := make(map[string][]publisher.Event)
	failedEvents := events[:0]
	fmt.Println("-------------------------")
	fmt.Println(failedEvents)
	var sendErr error
	url := c.hosts[0]

	ref := &msgRef{
		client: c,
		count:  int32(len(events)),
		total:  len(events),
		failed: nil,
		batch:  batch,
	}

	dropped := 0

	for i := range events {
		var valueData map[string]interface{}
		d := &events[i]
		msg, err := c.getEventMessage(d)
		if err != nil {
			c.log.Errorf("Dropping event: %+v", err)
			ref.done()
			c.observer.Dropped(1)
			continue
		}

		json.Unmarshal(msg.value, &valueData)
		if processor,  ok := valueData["processor"]; ok {
            eventType := processor.(map[string]interface{})["event"].(string)
            c.log.Infof("eventType: %v", eventType)
            if(eventType == "metric"){
				dropped += 1
                continue;
            }
        }

		if labels, ok := valueData["labels"]; ok {
			profileId := labels.(map[string]interface{})["_tag_profileId"].(string)
			topic := "trace-" + profileId 
			record := map[string]interface{}{"key": msg.key, "value": valueData}
			if rec, exist := data[topic]; exist {
				rec = append(rec, record)
				data[topic] = rec
				if evnts, exst := eventsRecord[topic]; exst {
					evnts = append(evnts, events[i])
					eventsRecord[topic] = evnts
				}
			} else {
				rec := []map[string]interface{}{}
				rec = append(rec, record)
				data[topic] = rec
				evnts := []publisher.Event{}
				evnts = append(evnts, events[i])
				eventsRecord[topic] = evnts
			}
			 
		}
	}
	c.observer.Dropped(dropped)


	if len(data) > 0 {
		for topic, records := range data {
			sendErr = c.sendToDest(url, topic,  records)
			if sendErr != nil {
				if evnts, ok:= eventsRecord[topic]; ok {
					for _, event := range evnts {
						failedEvents = append(failedEvents, event) 		
					}
				}
			}
		}
	}

	if len(failedEvents) == 0{
		batch.ACK()
	} else {
		batch.RetryEvents(failedEvents)
	}

	return sendErr
}

func (c *client) String() string {
	return "kafka(" + strings.Join(c.hosts, ",") + ")"
}

func (c *client) getEventMessage(data *publisher.Event) (*message, error) {
	event := &data.Content
	msg := &message{partition: -1, data: *data}

	value, err := data.Cache.GetValue("partition")
	if err == nil {
		if c.log.IsDebug() {
			c.log.Debugf("got event.Meta[\"partition\"] = %v", value)
		}
		if partition, ok := value.(int32); ok {
			msg.partition = partition
		}
	}

	value, err = data.Cache.GetValue("topic")
	if err == nil {
		if c.log.IsDebug() {
			c.log.Debugf("got event.Meta[\"topic\"] = %v", value)
		}
		if topic, ok := value.(string); ok {
			msg.topic = topic
		}
	}
/*
	if msg.topic == "" {
		topic, err := c.topic.Select(event)
		if err != nil {
			return nil, fmt.Errorf("setting kafka topic failed with %v", err)
		}
		if topic == "" {
			return nil, errNoTopicsSelected
		}
		msg.topic = topic
		if _, err := data.Cache.Put("topic", topic); err != nil {
			return nil, fmt.Errorf("setting kafka topic in publisher event failed: %v", err)
		}
	}
*/
	serializedEvent, err := c.codec.Encode(c.index, event)
	if err != nil {
		if c.log.IsDebug() {
			c.log.Debugf("failed event: %v", event)
		}
		return nil, err
	}

	buf := make([]byte, len(serializedEvent))
	copy(buf, serializedEvent)
	msg.value = buf
/*
	// message timestamps have been added to kafka with version 0.10.0.0
	if c.config.Version.IsAtLeast(sarama.V0_10_0_0) {
		msg.ts = event.Timestamp
	}
*/
	if c.key != nil {
		if key, err := c.key.RunBytes(event); err == nil {
			msg.key = key
		}
	}

	return msg, nil
}

/*
func (c *client) successWorker(ch <-chan *sarama.ProducerMessage) {
	defer c.wg.Done()
	defer c.log.Debug("Stop kafka ack worker")

	for libMsg := range ch {
		msg := libMsg.Metadata.(*message)
		msg.ref.done()
	}
}

func (c *client) errorWorker(ch <-chan *sarama.ProducerError) {
	defer c.wg.Done()
	defer c.log.Debug("Stop kafka error handler")

	for errMsg := range ch {
		msg := errMsg.Msg.Metadata.(*message)
		msg.ref.fail(msg, errMsg.Err)
	}
}
*/

func (r *msgRef) done() {
	r.dec()
}

func (r *msgRef) fail(msg *message, err error) {
	switch err {
	case sarama.ErrInvalidMessage:
		r.client.log.Errorf("Kafka (topic=%v): dropping invalid message", msg.topic)
		r.client.observer.Dropped(1)

	case sarama.ErrMessageSizeTooLarge, sarama.ErrInvalidMessageSize:
		r.client.log.Errorf("Kafka (topic=%v): dropping too large message of size %v.",
			msg.topic,
			len(msg.key)+len(msg.value))
		r.client.observer.Dropped(1)

	default:
		r.failed = append(r.failed, msg.data)
		r.err = err
	}
	r.dec()
}

func (r *msgRef) dec() {
	i := atomic.AddInt32(&r.count, -1)
	if i > 0 {
		return
	}

	r.client.log.Info("finished kafka batch")
	stats := r.client.observer

	err := r.err
	if err != nil {
		failed := len(r.failed)
		success := r.total - failed
		r.batch.RetryEvents(r.failed)

		stats.Failed(failed)
		if success > 0 {
			stats.Acked(success)
		}

		r.client.log.Infof("Kafka publish failed with: %+v", err)
	} else {
		r.batch.ACK()
		stats.Acked(r.total)
	}
}

func (c *client) Test(d testing.Driver) {
/*
	if c.config.Net.TLS.Enable == true {
		d.Warn("TLS", "Kafka output doesn't support TLS testing")
	}
	for _, host := range c.hosts {
		d.Run("Kafka: "+host, func(d testing.Driver) {
			netDialer := transport.TestNetDialer(d, c.config.Net.DialTimeout)
			_, err := netDialer.Dial("tcp", host)
			d.Error("dial up", err)
		})
	}

*/
}

