const queueList = [];

let isProcessing = false;

const processQueue = () => {
  if (isProcessing || queueList.length === 0) {
    return;
  }
  isProcessing = true;
  const cb = queueList.shift();
  cb(() => {
    isProcessing = false;
    processQueue();
  });
};

const push = (cb) => {
  if (typeof cb !== "function") {
    throw new Error("Callback must be a function");
  }
  queueList.push(cb);
  processQueue();
};

const asyncPush = () => {
  return new Promise((resolve) => {
    push((next) => {
      resolve(next);
    });
  });
};

export const Queue = {
  push,
  asyncPush,
};
