import { ConsumeMessage } from 'amqplib';
import EventBus from './index';
import { INIT_EVENTBUS_ERROR, NO_AMPQ_URL_ERROR } from './errors';

import dotenv from 'dotenv';
dotenv.config();

afterAll(() => {
  return EventBus.close();
});

const TEST_EXCHANGE = 'events';
const TEST_QUEUE = 'TEST_QUEUE';
const TEST_QUEUE_BETA = 'TEST_QUEUE_BETA';

test('it throws if AMQP url is not passed into init function', async () => {
  expect.assertions(1);
  await expect(EventBus.init(undefined)).rejects.toThrow(NO_AMPQ_URL_ERROR);
});

test('it throws if Event Bus methods are called before initializing Event Bus', async () => {
  expect.assertions(10);

  expect(() => EventBus.getPubChannel()).toThrow(INIT_EVENTBUS_ERROR);
  expect(() => EventBus.getSubChannel()).toThrow(INIT_EVENTBUS_ERROR);
  expect(() => EventBus.getPubConnection()).toThrow(INIT_EVENTBUS_ERROR);
  expect(() => EventBus.getSubConnection()).toThrow(INIT_EVENTBUS_ERROR);

  await expect(EventBus.createNewPubChannel(undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(EventBus.createNewSubChannel(undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(EventBus.emit(undefined, undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(EventBus.on(undefined, undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(EventBus.queue(undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(EventBus.consume(undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
});

test('it creates an AMQP connection and Publish and Subscribe channels', async () => {
  await expect(EventBus.init(process.env.AMQP_URL)).resolves.toBeTruthy();
  expect(EventBus.getPubChannel()).toBeDefined();
  expect(EventBus.getSubChannel()).toBeDefined();
  expect(EventBus.getPubConnection()).toBeDefined();
  expect(EventBus.getSubConnection()).toBeDefined();
}, 12000);

test('it emits an event', async () => {
  const didEventBusEmit = await EventBus.emit(TEST_EXCHANGE, 'test.emit', {
    topic: 'test.emit',
    type: 'test',
  });
  expect(didEventBusEmit).toBeTruthy();
});

test('it listens on and gets data from an emitted event', async done => {
  const callback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.topic).toBe('test.emit');
    expect(data.type).toBe('test');
    expect(result.consumerTag).toBeDefined();
    done();
  };

  const result = await EventBus.on(TEST_EXCHANGE, 'test.emit', callback);

  // emit mock event
  await EventBus.emit(TEST_EXCHANGE, 'test.emit', {
    topic: 'test.emit',
    type: 'test',
  });
});

test('it sends messages to a queue', async () => {
  const isSentToQueue = await EventBus.queue(TEST_QUEUE, {
    name: 'Test this thing!',
    adapter: 'RabbitMQ',
  });
  expect(isSentToQueue).toBeTruthy();
});

it('picks up messages from the queue', async done => {
  const callback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.name).toBe('Test this thing!');
    expect(data.adapter).toBe('RabbitMQ');
    EventBus.acknowledgeMessage(message);
    done();
  };

  const result = await EventBus.consume(TEST_QUEUE, callback);
  expect(result.consumerTag).toBeDefined();
});

it('requeues jobs', async done => {
  // send mock task to queue
  await EventBus.queue(TEST_QUEUE_BETA, {
    message: 'I shall be requeued',
  });

  // capture the message and requeue it
  const rejectCallback = (message: ConsumeMessage) => {
    EventBus.rejectMessage(message);
  };

  const assertionCallback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.message).toBe('I shall be requeued');
    EventBus.rejectMessage(message);
    done();
  };

  await EventBus.consume(TEST_QUEUE_BETA, rejectCallback);
  await EventBus.consume(TEST_QUEUE_BETA, assertionCallback);
});

it('it picks up stale/requeued jobs', async done => {
  const callback = (message: ConsumeMessage) => {
    const data = JSON.parse(message.content.toString());
    expect(data.message).toBe('I shall be requeued');
    EventBus.acknowledgeMessage(message);
    done();
  };

  await EventBus.consume(TEST_QUEUE_BETA, callback);
});
