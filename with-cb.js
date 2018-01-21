const amqp = require('amqp');
const request = require('request');

const GATEWAY_URL = 'https://smsgateway/...';
const BREAKER_THRESHOLD = 5;
const BREAKER_PAUSE = 30000;
const AMQP_URL = 'amqp://...';

let ctag;
let queue;
let breaker = 0;

const subscribe = (processFunction) => {
  queue.subscribe(processFunction).addCallback((ok) => {
    // we have to remember the consumer tag, because we will need it
    // to unsubscribe from the queue later on
    ctag = ok.consumerTag;
  });
};

const pause = (processFunction) => {
  // unsubscribe from the queue, thus stop processing messages
  queue.unsubscribe(ctag);
  // wait for 30 seconds
  setTimeout(() => {
    // reset the circuit breaker counter and re-subscribe to receive messages
    breaker = 0;
    subscribe(processFunction);
  }, BREAKER_PAUSE);
};

const processMessage = (message, headers, info, originalMessage) => {
  const payload = {
    recipient: message.data.recipient,
    body: message.data.body,
  };
  // call the external SMS gateway
  request.post(GATEWAY_URL, payload, (error, response) => {
    // the call failed
    if (error || response.statusCode >= 500) {
      // increment the circuit breaker counter
      breaker += 1;
      // check if the counter is over threshold
      if (breaker >= BREAKER_THRESHOLD) {
        // trip the circuit breaker if over
        pause(processMessage);
      }
      // return the message back to the queue
      originalMessage.reject(true);
    } else {
      // reset the circuit breaker counter
      breaker = 0;
      // acknowledge the message, thus removing it from queue permanently
      originalMessage.acknowledge();
    }
  });
};

// create AMQP connection
const conn = amqp.createConnection(AMQP_URL);

// when connected, create the queue
conn.on('ready', () => {
  conn.queue('myqueue', (createdQueue) => {
    queue = createdQueue;
    // when the queue is created, bind it to myexchange and receive all messages
    queue.bind('myexchange', '#');
    // call the process function for all messages
    subscribe(processMessage);
  });
});
