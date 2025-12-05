// consumer/src/queue.js
const EventEmitter = require('events');

class BoundedQueue extends EventEmitter {
  constructor(maxLength, concurrency) {
    super();
    this.maxLength = maxLength;
    this.concurrency = concurrency;
    this.queue = [];
    this.active = 0;
  }

  // Try enqueue; return true if enqueued, false if dropped.
  tryEnqueue(job) {
    if (this.queue.length >= this.maxLength) {
      console.log(`[QUEUE] DROPPED! Queue full (${this.queue.length}/${this.maxLength}), Active: ${this.active}/${this.concurrency}`);
      return false;
    }
    this.queue.push(job);
    console.log(`[QUEUE] Enqueued. Queue size: ${this.queue.length}/${this.maxLength}, Active: ${this.active}/${this.concurrency}`);
    process.nextTick(() => this._drain());
    return true;
  }

  _drain() {
    while (this.active < this.concurrency && this.queue.length > 0) {
      const job = this.queue.shift();
      this.active++;
      Promise.resolve()
        .then(() => job.run())
        .catch(err => this.emit('error', err))
        .finally(() => {
          this.active--;
          this.emit('done', job);
          this._drain();
        });
    }
  }
}

module.exports = { BoundedQueue };
