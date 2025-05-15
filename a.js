import { JSONDB } from "./src/index.js";

const getRandomNumber = (min, max) => {
  return Math.floor(Math.random() * (max - min + 1)) + min;
};

const randomExec = (cb, min = 10, max = 3000) => {
  const rand = Math.floor(Math.random() * (max - min + 1)) + min;
  setInterval(() => {
    cb();
  }, rand);
};

const workers = 10;

async function main() {
  const db = new JSONDB();

  const table = await db.getTable("test");
  //random inserts
  for (let i = 0; i < workers; i++) {
    randomExec(() => {
      console.log("insert");
      table.insert({
        id: getRandomNumber(0, 10000),
        name: "test",
        age: 10,
        createdAt: new Date(),
      });
    });
  }

  //random update
  for (let i = 0; i < workers; i++) {
    randomExec(() => {
      console.log("update");
      table.update(
        { id: getRandomNumber(0, 10000) },
        {
          name: "test",
          age: 10,
          createdAt: new Date(),
        },
      );
    });
  }

  // random delete

  for (let i = 0; i < workers; i++) {
    randomExec(() => {
      console.log("delete");
      table.remove({ id: getRandomNumber(0, 10000) });
    });
  }

  //random get
  for (let i = 0; i < workers; i++) {
    randomExec(() => {
      console.log("get");
      table.find({ id: getRandomNumber(0, 10000) });
    });
  }
}

main();
