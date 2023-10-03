import { JSONDB } from "../src/index.js";

const db = new JSONDB(undefined, { keepInRam: true, transactionWrite: true, writeTime: 5000 });

async function main() {
  console.log(await db.find("useradsfasdfaas", {}));

  for (let i = 0; i < 1; i++) {
    await db.insert("users", { username: "a", passwd: "a" });
    await db.insert("users", { username: "b", passwd: "b" });
    await db.insert("users", { username: "c", passwd: "c" });
    await db.insert("users", { username: "d", passwd: "d" });
    await db.insert("users", { username: "e", passwd: "e" });
    await db.insert("users", { username: "f", passwd: "f" });
    await db.insert("users", { username: "g", passwd: "g" });
  }

  let users = await db.find("users", { filter: (e) => e.username > "a", limit: 1000 });
  console.log(
    users.length
  );

  await db.update("users", { where: { username: "a" } }, { username: "aa" });

  console.log(await db.find("users", {}));

  await db.remove("users", { filter: (i) => i.username == "d" });

  console.log(await db.find("users", {}));

  await db.remove("users", { filter: (i) => i.username == "d" });

  console.log(await db.find("users", {}));

  //await db.remove("users", {});
}

main();
