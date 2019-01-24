import amqp, { ConsumeMessage, connect } from 'amqplib';
import { NO_AMPQ_URL_ERROR, INIT_EVENTBUS_ERROR } from './errors';

type RabbitMQCallback = (msg: amqp.ConsumeMessage) => any;

// export interface IEventBus {
//   init(amqp_url: string, eventsExchange?: string): Promise<Boolean>;
//   close(): Promise<void>;
//   emit(eventName: string, data: object): Promise<boolean>;
//   on(eventName: string, callback: RabbitMQCallback): Promise<any>;
//   queue(queueName: string, data: object): Promise<boolean>;
//   consume(
//     queueName: string,
//     callback: (message: any) => any,
//     limit: number,
//     options?: amqp.Options.Consume,
//   ): Promise<amqp.Replies.Consume>;
//   getPubChannel(): amqp.Channel;
//   getSubChannel(): amqp.Channel;
//   getPubConnection(): amqp.Connection;
//   getSubConnection(): amqp.Connection;
//   setEventsExchange(eventsExchange: string);
//   getEventsExchange(): string;
//   acknowledgeMessage(message: ConsumeMessage);
// }

class EventBus {
  private pubConnection: amqp.Connection;
  private subConnection: amqp.Connection;
  private pubChannel: amqp.Channel;
  private subChannel: amqp.Channel;

  /**
   * Initializes RabbitMQ connection(s) if it doesn't exist already
   * Creates Publish and Subscribe channels and connections.
   * @param amqp_url AMQP url to be used for Event Bus connections
   * @returns {boolean} Returns true if initialization was successful.
   */
  async init(amqp_url: string) {
    if (this.pubConnection && this.subConnection) return true; // prevents us from carelessly creating multiple AMQP connections in our app.

    if (!amqp_url) throw new Error(NO_AMPQ_URL_ERROR);

    // set connection heartbeat to 60
    const connectionUrl = amqp_url + '?heartbeat=60';

    // create pub and sub connections
    this.pubConnection = await amqp.connect(connectionUrl);
    this.subConnection = await amqp.connect(connectionUrl);

    // create pub and sub channels with seperate connections
    this.pubChannel = await this.pubConnection.createChannel();
    this.subChannel = await this.subConnection.createChannel();
    return true;
  }

  /**
   * Checks and throws an error if pub and sub connections have not been created.
   */
  private isEventBusInitialized() {
    if (!this.pubConnection || !this.subConnection)
      throw new Error(INIT_EVENTBUS_ERROR);
  }

  /**
   * Closes the Pub and Sub AMQP connections.
   * Will immediately invalidate any unresolved operations, so it’s best to make sure you’ve done everything you need to before calling this. Will be resolved
   * once the connection, and underlying socket, are closed
   */
  async close() {
    await this.pubConnection.close();
    await this.subConnection.close();
  }

  /**
   * Returns the Event Bus Publish channel
   * @returns {amqp.Channel} Event Bus Publish channel
   */
  getPubChannel() {
    this.isEventBusInitialized();
    return this.pubChannel;
  }

  /**
   * Returns the Event Bus Subscribe channel
   * Useful for acknowledging messages from queue
   * @returns {amqp.Channel} Event Bus Subscribe channel
   */
  getSubChannel() {
    this.isEventBusInitialized();
    return this.subChannel;
  }

  /**
   * Returns the Event Bus Publish connection
   * @returns {amqp.Connection} Event Bus Publish connection
   */
  getPubConnection() {
    this.isEventBusInitialized();
    return this.pubConnection;
  }

  /**
   * Returns the Event Bus Subscribe connection
   * @returns {amqp.Connection} Event Bus Subscribe RabbitMQ connection
   */
  getSubConnection() {
    this.isEventBusInitialized();
    return this.subConnection;
  }

  /**
   * Creates a new publish channel over the publish connection
   * . Replaces the existing publish channel with a new one if an override flag is passed and returns it.
   * If `override` isn't passed or set to false, it returns a new
   * publish channel.
   * @param override Flag for if existing publish channel should be replaced
   * @returns {amqp.Channel} New Pub channel
   */
  async createNewPubChannel(override: boolean): Promise<amqp.Channel> {
    this.isEventBusInitialized();
    if (override)
      return (this.pubChannel = await this.pubConnection.createChannel());
    else return this.pubConnection.createChannel();
  }

  /**
   * Creates a new subscribe channel over the subscribe connection
   * . Replaces the existing subscribe channel with a new one if an override flag is passed and returns it.
   * If `override` isn't passed or set to false, it returns a new
   * subscribe channel.
   * @param override Flag for if existing subscribe channel should be replaced
   * @returns {Promise<amqp.Channel>} New Sub channel
   */
  async createNewSubChannel(override: boolean): Promise<amqp.Channel> {
    this.isEventBusInitialized();
    if (override)
      return (this.subChannel = await this.subConnection.createChannel());
    else return this.subConnection.createChannel();
  }

  /**
   * Emits an event via the passed-in `exchange`
   * @param exchange Exhange to emit the event on
   * @param event Event to be emitted (routing key)
   * @param data The data to be emitted
   * @returns {Promise<boolean>}
   */
  async emit(exchange: string, event: string, data: object) {
    this.isEventBusInitialized();
    await this.pubChannel.assertExchange(exchange, 'topic');
    const content = Buffer.from(JSON.stringify(data));
    return this.pubChannel.publish(exchange, event, content);
  }

  /**
   * Listens for an `event` on an `exchange` and invokes the provided `callback`
   * @param exchange Exchange to listen for events on
   * @param event Event to be consumed (routing key)
   * @param callback Callback to be invoked when event gets emitted
   * @returns Promise<amqp.Replies.Consume> AMQP reply
   */
  async on(exchange: string, event: string, callback: RabbitMQCallback) {
    this.isEventBusInitialized();

    await this.subChannel.assertExchange(exchange, 'topic');

    // create temporary queue that gets deleted when connection closes
    const { queue } = await this.subChannel.assertQueue('', {
      exclusive: true,
    });

    await this.subChannel.bindQueue(queue, exchange, event);

    return this.subChannel.consume(queue, callback, {
      noAck: true,
    });
  }

  /**
   * Pushes data to the queue `queueName`
   * @param queueName Queue to push data to
   * @param data Data to be pushed to queue `queueName`
   * @return {boolean}
   */
  async queue(queueName: string, data: object) {
    this.isEventBusInitialized();

    await this.pubChannel.assertQueue(queueName, { durable: true });

    const message = Buffer.from(JSON.stringify(data));
    return this.pubChannel.sendToQueue(queueName, message, {
      persistent: true,
    });
  }

  /**
   * Consumes tasks from a queue `queueName` and invokes the provided callback
   * @param queueName Queue to consume tasks from
   * @param callback Callback to be invoked for each message that gets sent to the queue
   * @param limit The number of concurrent jobs the consumer can handle. Defaults to 3
   * @param options Optional options. If the noAck option is set to true or not specified, you
   *   are expected to call channel.ack(message) at the end of the supplied
   *   callback inorder to notify the queue that the message has been acknowledged.
   */
  async consume(
    queueName: string,
    callback: RabbitMQCallback,
    limit: number = 3,
    options?: amqp.Options.Consume,
  ) {
    this.isEventBusInitialized();

    // limit number of concurrent jobs
    this.subChannel.prefetch(limit);

    await this.subChannel.assertQueue(queueName, { durable: true });

    return this.subChannel.consume(queueName, callback, options);
  }

  /**
   * Acknowledges a message.
   * @param message The message to be acknowledged
   */
  acknowledgeMessage(message: ConsumeMessage) {
    this.subChannel.ack(message);
  }

  /**
   * Rejects a message and requeues it by default.
   * @param message The message to be reject
   * @param requeue Boolean flag on if the message should be requeued. Defaults to true
   */
  rejectMessage(message: ConsumeMessage, requeue: boolean = true) {
    this.subChannel.nack(message, false, requeue);
  }
}

const eventBus = new EventBus();

/**
 * The default export is an instance of the Event Bus
 * It essentially acts a singleton, reusing the same connection
 * across our app.
 */
export default eventBus;
