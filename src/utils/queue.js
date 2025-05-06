export class QueueService {
  queueList = [];
  isProcessing = false;
  processQueue = () => {
    if (this.isProcessing || this.queueList.length === 0) {
      return;
    }
    this.isProcessing = true;
    const cb = this.queueList.shift();
    cb(() => {
      this.isProcessing = false;
      this.processQueue();
    });
  };

  push = (cb) => {
    if (typeof cb !== "function") {
      throw new Error("Callback must be a function");
    }
    this.queueList.push(cb);
    this.processQueue();
  };

  asyncPush = () => {
    return new Promise((resolve) => {
      this.push((next) => {
        resolve(next);
      });
    });
  };
}
