const amqp = require('amqp');
const request = require('request');

const GATEWAY_URL = 'https://smsgateway/...';
const AMQP_URL = 'amqp://...';

// this is called every time a message is received from queue
const process = (message, headers, info, original) => {
  // construct the payload for the external gateway service
  const payload = {
    recipient: message.data.recipient,
    body: message.data.body,
  };
  // call the external SMS gateway
  request.post(GATEWAY_URL, payload, (error, response) => {
    // the call failed
    if (error || response.statusCode >= 500) {
      // return the message back to the queue
      original.reject(true);
    } else {
      // acknowledge the message, removing it from queue permanently
      original.acknowledge();
    }
  });
};

// create AMQP connection
const conn = amqp.createConnection(AMQP_URL);

// when connected, create the queue
conn.on('ready', () => {
  conn.queue('myqueue', (createdQueue) => {
    // when the queue is created, bind it to
    // myexchange and receive all messages
    createdQueue.bind('myexchange', '#');
    // call the process function for all messages
    createdQueue.subscribe(process);
  });
});
