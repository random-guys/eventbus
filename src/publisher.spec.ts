import { publisher } from './index';
import { INIT_EVENTBUS_ERROR, NO_AMPQ_URL_ERROR } from './lib/errors';

import dotenv from 'dotenv';
dotenv.config();

afterAll(() => {
  return publisher.close();
});

const TEST_EXCHANGE = 'events';
const TEST_QUEUE = 'TestPubQueue';

test('it throws if AMQP url is not passed into init function', async () => {
  expect.assertions(1);
  await expect(publisher.init(undefined)).rejects.toThrow(NO_AMPQ_URL_ERROR);
});

test('it throws if Event Bus methods are called before initializing Event Bus', async () => {
  expect.assertions(5);

  expect(() => publisher.getChannel()).toThrow(INIT_EVENTBUS_ERROR);
  expect(() => publisher.getConnection()).toThrow(INIT_EVENTBUS_ERROR);

  await expect(publisher.close()).rejects.toThrow(INIT_EVENTBUS_ERROR);
  await expect(publisher.emit(undefined, undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
  await expect(publisher.queue(undefined, undefined)).rejects.toThrow(
    INIT_EVENTBUS_ERROR,
  );
});

test('it creates an AMQP connection and channel', async () => {
  await expect(publisher.init(process.env.AMQP_URL)).resolves.toBeTruthy();
  expect(publisher.getChannel()).toBeDefined();
  expect(publisher.getConnection()).toBeDefined();
}, 12000);

test('it creates a new Publisher instance', async () => {
  const newPublisher = publisher.create();
  expect(Object.getPrototypeOf(newPublisher)).toBe(
    Object.getPrototypeOf(publisher),
  );
  expect(() => newPublisher.getChannel()).toThrow(INIT_EVENTBUS_ERROR);
  expect(() => newPublisher.getConnection()).toThrow(INIT_EVENTBUS_ERROR);
});

test('it reuses the same publisher instance for subsequent imports', async () => {
  const { publisher: newPublisherImport } = await import('./index');
  expect(newPublisherImport).toBe(publisher);
  expect(newPublisherImport.getChannel()).toBe(publisher.getChannel());
  expect(newPublisherImport.getConnection()).toBe(publisher.getConnection());
});

test('it emits an event', async () => {
  const didEventBusEmit = await publisher.emit(TEST_EXCHANGE, 'test.pub.emit', {
    topic: 'test.emit',
    type: 'test',
  });
  expect(didEventBusEmit).toBeTruthy();
});

test('it sends messages to a queue', async done => {
  const isSentToQueue = await publisher.queue(TEST_QUEUE, {
    name: 'Test publish!',
    adapter: 'RabbitMQ',
  });
  expect(isSentToQueue).toBeTruthy();

  // delay for 500ms so we don't disconnect before we send messages
  setTimeout(done, 500);
});
