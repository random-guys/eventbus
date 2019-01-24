import amqp, { ConsumeMessage } from 'amqplib';
import Base, { BaseContract } from './base';

type RabbitMQCallback = (msg: amqp.ConsumeMessage) => any;

/**
 * Event Bus Subscriber.
 * Enables you to subscribe to events and pick up messages from
 * queues.
 */
class Subscriber extends Base implements BaseContract {
  /**
   * Creates a new Subscriber instance
   */
  create() {
    return new Subscriber();
  }

  /**
   * Listens for an `event` on an `exchange` and invokes the provided `callback` when
   * the event gets emitted.
   * Works as a pub-sub subscriber.
   * @param exchange Exchange to listen for events on
   * @param event Event to be consumed (routing key)
   * @param callback Callback to be invoked when event gets emitted
   * @returns Promise<amqp.Replies.Consume> AMQP reply
   */
  async on(
    exchange: string,
    event: string,
    callback: RabbitMQCallback,
  ): Promise<amqp.Replies.Consume> {
    this.isEventBusInitialized();

    await this.channel.assertExchange(exchange, 'topic');

    // create temporary queue that gets deleted when connection closes
    const { queue } = await this.channel.assertQueue('', {
      exclusive: true,
    });

    await this.channel.bindQueue(queue, exchange, event);

    return this.channel.consume(queue, callback, {
      noAck: true,
    });
  }

  /**
   * Consumes tasks/messages from a queue `queueName` and invokes the provided callback
   * @param queueName Queue to consume tasks from
   * @param callback Callback to be invoked for each message that gets sent to the queue
   * @param limit The number of concurrent jobs the consumer can handle. Defaults to 3
   * @param options Optional options. If the noAck option is set to true or not specified,
   * you are expected to call channel.ack(message) at the end of the supplied
   * callback inorder to notify the queue that the message has been acknowledged.
   */
  async consume(
    queueName: string,
    callback: RabbitMQCallback,
    limit: number = 3,
    options?: amqp.Options.Consume,
  ): Promise<amqp.Replies.Consume> {
    this.isEventBusInitialized();

    // limit number of concurrent jobs
    this.channel.prefetch(limit);
    await this.channel.assertQueue(queueName, { durable: true });
    return this.channel.consume(queueName, callback, options);
  }

  /**
   * Acknowledges a message.
   * @param message The message to be acknowledged
   */
  acknowledgeMessage(message: ConsumeMessage) {
    this.isEventBusInitialized();
    this.channel.ack(message);
  }

  /**
   * Rejects a message and requeues it by default.
   * @param message The message to be reject
   * @param requeue Boolean flag on if the message should be requeued. Defaults to true
   */
  rejectMessage(message: ConsumeMessage, requeue: boolean = true) {
    this.isEventBusInitialized();
    this.channel.nack(message, false, requeue);
  }
}

/**
 * The only export is an instance of the Subscriber
 * It essentially acts a singleton, reusing the same connection
 * across our app.
 */
export const subscriber = new Subscriber();
