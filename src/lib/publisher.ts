import amqp, { ConsumeMessage, connect } from 'amqplib';
import Base, { BaseContract } from './base';

/**
 * Event Bus Publisher.
 * Enables you to publish messages to subscribers and push
 * messages to queues via exchanges.
 */
class Publisher extends Base implements BaseContract {
  /**
   * Creates a new Publisher instance
   */
  create() {
    return new Publisher();
  }

  /**
   * Emits an event via the passed-in `exchange`
   * Works as a pub-sub publisher.
   * @param exchange Exhange to emit the event on
   * @param event Event to be emitted (routing key)
   * @param data The data to be emitted
   * @param options RabbitMQ Publish options
   * @returns {Promise<boolean>}
   */
  async emit(
    exchange: string,
    event: string,
    data: object,
    options?: amqp.Options.Publish,
  ) {
    this.isEventBusInitialized();
    await this.channel.assertExchange(exchange, 'topic');
    const message = Buffer.from(JSON.stringify(data));
    return this.channel.publish(exchange, event, message, options);
  }

  /**
   * Pushes data to the queue `queueName`
   * @param queueName Queue to push data to
   * @param data Data to be pushed to queue `queueName`
   * @param options RabbitMQ Publish options
   * Messages are persistent by default.
   * @return {boolean}
   */
  async queue(queueName: string, data: object, options?: amqp.Options.Publish) {
    this.isEventBusInitialized();
    await this.channel.assertQueue(queueName, { durable: true });
    const message = Buffer.from(JSON.stringify(data));
    return this.channel.sendToQueue(queueName, message, {
      persistent: true,
      ...options,
    });
  }
}

/**
 * The only export is an instance of the Publisher
 * It essentially acts a singleton, reusing the same connection
 * across our app.
 */
export const publisher = new Publisher();
