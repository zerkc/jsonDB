import { JSONDB } from "./src/index.js";

async function main() {
  const db = new JSONDB();

  console.log(await db.count("stats"));
}

main();
