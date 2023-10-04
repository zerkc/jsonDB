import { JSONDB } from "../src/index.js";

const db = new JSONDB(undefined, { keepInRam: true, transactionWrite: true, writeTime: 5000 });

async function main() {

  for (let i = 0; i < 1; i++) {
    db.insert("users", { username: "a", passwd: "a" });
    db.insert("users", { username: "b", passwd: "b" });
    db.insert("users", { username: "c", passwd: "c" });
    db.insert("users", { username: "d", passwd: "d" });
    db.insert("users", { username: "e", passwd: "e" });
    db.insert("users", { username: "f", passwd: "f" });
    db.insert("users", { username: "g", passwd: "g" });
  }

  let users = await db.count("users");
  console.log(2,
    users
  );

  await db.update("users", { where: { username: "a" } }, { username: "aa" });


  await db.remove("users", { filter: (i) => i.username == "d" });


  await db.remove("users", { filter: (i) => i.username == "d" });

  console.log(await db.count("users", {}));

  db.remove("users", {});


  console.log(await db.count("users", {}));
}

main();
