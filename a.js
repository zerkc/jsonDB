import { JSONDB } from "./src/index.js";

async function main() {
  const db = new JSONDB();

  console.log(await db.count("stats"));
  await db.remove("stats", {
    filter: (e) => {
      let eDate = new Date(e.timestamp);
      eDate.setMilliseconds(0);
      return eDate.getTime() <= new Date("2025-05-16T19:42:16.683Z").getTime();
    },
  });

  console.log(await db.count("stats"));
}

main();
