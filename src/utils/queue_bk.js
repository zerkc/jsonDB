class QueueService {
  queueList = [];
  isProcessing = false;
  processQueue = () => {
    if (this.isProcessing || this.queueList.length === 0) {
      return;
    }
    this.isProcessing = true;
    const cb = this.queueList.shift();
    cb(() => {
      console.log("finished", this.queueList.length);
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

const globalQueue = new QueueService();

export const Queue = {
  push: globalQueue.push,
  asyncPush: globalQueue.asyncPush,
  create: () => new QueueService(),
};
