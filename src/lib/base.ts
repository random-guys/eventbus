import amqp from "amqplib";
import { NO_AMPQ_URL_ERROR, INIT_EVENTBUS_ERROR } from "./errors";

/**
 * Base class for extending Event Bus behaviour.
 */
export default class Base {
  protected connection: amqp.Connection;
  protected channel: amqp.Channel;

  /**
   * Initializes the AMQP connection if it doesn't exist already
   * Creates AMQP channel and connection
   * @param amqp_url AMQP url to be used for Event Bus connections
   * @returns {boolean} Returns true if initialization was successful.
   */
  async init(amqp_url: string) {
    if (this.connection) return true; // prevents us from carelessly creating multiple AMQP connections in our app.

    if (!amqp_url) throw new Error(NO_AMPQ_URL_ERROR);

    // create connection
    this.connection = await amqp.connect(amqp_url);

    // create channel
    this.channel = await this.connection.createChannel();
    return true;
  }

  /**
   * Checks and throws an error if a connection has not been created.
   */
  protected isEventBusInitialized() {
    if (!this.connection || !this.channel) throw new Error(INIT_EVENTBUS_ERROR);
  }

  /**
   * Closes AMQP connection.
   * Will immediately invalidate any unresolved operations,
   * so it’s best to make sure you’ve done everything you need to before calling this.
   * Will be resolved once the connection, and underlying socket, are closed
   */
  async close() {
    this.isEventBusInitialized();
    await this.connection.close();
  }

  /**
   * Returns the channel
   * @returns {amqp.Channel} Event Bus channel
   */
  getChannel() {
    this.isEventBusInitialized();
    return this.channel;
  }

  /**
   * Returns the connection
   * @returns {amqp.Connection} Event Bus connection
   */
  getConnection() {
    this.isEventBusInitialized();
    return this.connection;
  }
}

export interface BaseContract {
  create();
}
