import { ConsumeMessage } from 'amqplib';
import { subscriber, publisher } from './index';
import { INIT_EVENTBUS_ERROR, NO_AMPQ_URL_ERROR } from './lib/errors';

import dotenv from 'dotenv';
dotenv.config();

beforeAll(async () => {
  await publisher.init(process.env.AMQP_URL);
}, 12000);

afterAll(async () => {
  await publisher.close();
  await subscriber.close();
});

const TEST_EXCHANGE = 'events';
const TEST_QUEUE = 'TestSubQueue';
const TEST_QUEUE_BETA = 'TestSubQueueBeta';

test('it throws if AMQP url is not passed into init function', async () => {
  expect.assertions(1);
  await expect(subscriber.init(undefined)).rejects.toThrow(NO_AMPQ_URL_ERROR);
});

test('it throws if Event Bus methods are called before initializing Event Bus', async () => {
  expect.assertions(7);

  expect(() => subscriber.getChannel()).toThrow(INIT_EVENTBUS_ERROR);
  expect(() => subscriber.getConnection()).toThrow(INIT_EVENTBUS_ERROR);

  await expect(subscriber.close()).rejects.toThrow(INIT_EVENTBUS_ERROR);

  await expect(subscriber.on(undefined, undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(subscriber.consume(undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(() => subscriber.acknowledgeMessage(undefined)).toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(() => subscriber.rejectMessage(undefined)).toThrow(
    INIT_EVENTBUS_ERROR,
  );
});

test('it creates an AMQP connection and channel', async () => {
  await expect(subscriber.init(process.env.AMQP_URL)).resolves.toBeTruthy();
  expect(subscriber.getChannel()).toBeDefined();
  expect(subscriber.getConnection()).toBeDefined();
}, 12000);

test('it creates a new subscriber instance', () => {
  const newSubscriber = subscriber.create();
  expect(Object.getPrototypeOf(newSubscriber)).toBe(
    Object.getPrototypeOf(subscriber),
  );
  expect(() => newSubscriber.getChannel()).toThrow(INIT_EVENTBUS_ERROR);
  expect(() => newSubscriber.getConnection()).toThrow(INIT_EVENTBUS_ERROR);
});

test('it reuses the same subscriber instance for imports', async () => {
  const { subscriber: newSubscriberImport } = await import('./index');

  expect(newSubscriberImport).toBe(subscriber);
  expect(newSubscriberImport.getChannel()).toBe(subscriber.getChannel());
  expect(newSubscriberImport.getConnection()).toBe(subscriber.getConnection());
});

test('it listens on and gets data from an emitted event', async done => {
  const callback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.topic).toBe('test.emit');
    expect(data.type).toBe('test');
    done();
  };

  const result = await subscriber.on(TEST_EXCHANGE, 'test.emit', callback);
  expect(result.consumerTag).toBeDefined();

  // emit mock event
  await publisher.emit(TEST_EXCHANGE, 'test.emit', {
    topic: 'test.emit',
    type: 'test',
  });
});

it('picks up messages from the queue', async done => {
  // send mock task to queue
  await publisher.queue(TEST_QUEUE, {
    name: 'Test this thing!',
    adapter: 'RabbitMQ',
  });

  const callback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.name).toBe('Test this thing!');
    expect(data.adapter).toBe('RabbitMQ');
    subscriber.acknowledgeMessage(message);
    done();
  };

  const result = await subscriber.consume(TEST_QUEUE, callback);
  expect(result.consumerTag).toBeDefined();
});

it('requeues jobs', async done => {
  // send mock task to queue
  await publisher.queue(TEST_QUEUE_BETA, {
    message: 'I shall be requeued',
  });

  // capture the message and requeue it
  const rejectCallback = (message: ConsumeMessage) => {
    subscriber.rejectMessage(message);
  };

  const assertionCallback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.message).toBe('I shall be requeued');
    subscriber.rejectMessage(message);
    done();
  };

  await subscriber.consume(TEST_QUEUE_BETA, rejectCallback);
  await subscriber.consume(TEST_QUEUE_BETA, assertionCallback);
});

it('it picks up stale/requeued jobs', async done => {
  const callback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.message).toBe('I shall be requeued');
    subscriber.acknowledgeMessage(message);
    done();
  };

  await subscriber.consume(TEST_QUEUE_BETA, callback);
});
