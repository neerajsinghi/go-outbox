// Package outbox implements the transactional outbox pattern on top of
// database/sql.
//
// A service that must both change its database and emit an event cannot do
// the two atomically: commit then publish loses the event if the process
// crashes in between, publish then commit emits an event for a change that
// may roll back. The outbox fixes this by writing the event into an outbox
// table in the same transaction as the business change, and letting a
// separate [Relay] publish committed rows to the broker afterwards.
//
//	ob, _ := outbox.New(dialect.Postgres)
//
//	tx, _ := db.BeginTx(ctx, nil)
//	_, _ = tx.ExecContext(ctx, `INSERT INTO orders ...`)
//	_, _ = ob.Enqueue(ctx, tx, outbox.Message{Topic: "orders.created", Key: orderID, Payload: body})
//	_ = tx.Commit()
//
//	relay := ob.NewRelay(db, publisher)
//	go relay.Run(ctx)
//
// # Delivery guarantees
//
// Delivery is at-least-once. A message can be published more than once, for
// example when the broker accepted it but the relay crashed before marking
// it sent. Consumers must be idempotent; [Message.ID] is stable across
// retries and is the natural deduplication key.
//
// # Writing a Publisher
//
// Adapters are deliberately not part of this module so that it pulls in no
// broker SDKs. A Kafka adapter using github.com/segmentio/kafka-go is a few
// lines:
//
//	type KafkaPublisher struct{ W *kafka.Writer } // RequiredAcks: kafka.RequireAll
//
//	func (p KafkaPublisher) Publish(ctx context.Context, m outbox.Message) error {
//		hdrs := []kafka.Header{{Key: "outbox-id", Value: []byte(m.ID)}}
//		for k, v := range m.Headers {
//			hdrs = append(hdrs, kafka.Header{Key: k, Value: []byte(v)})
//		}
//		return p.W.WriteMessages(ctx, kafka.Message{
//			Topic: m.Topic, Key: []byte(m.Key), Value: m.Payload, Headers: hdrs,
//		})
//	}
//
// and an Amazon SQS adapter using aws-sdk-go-v2:
//
//	type SQSPublisher struct {
//		Client   *sqs.Client
//		QueueURL func(topic string) string
//	}
//
//	func (p SQSPublisher) Publish(ctx context.Context, m outbox.Message) error {
//		in := &sqs.SendMessageInput{
//			QueueUrl:    aws.String(p.QueueURL(m.Topic)),
//			MessageBody: aws.String(string(m.Payload)),
//		}
//		if m.Key != "" { // FIFO queues
//			in.MessageGroupId = aws.String(m.Key)
//			in.MessageDeduplicationId = aws.String(m.ID)
//		}
//		_, err := p.Client.SendMessage(ctx, in)
//		return err
//	}
//
// The publisher/memory package provides in-process publishers for tests.
package outbox
