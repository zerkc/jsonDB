import { JSONDB } from "../src/index.js";

const db = new JSONDB();

async function main() {
	console.log(await db.find("useradsfasdfaas", {}));

	await db.insert("users", { username: "a", passwd: "a" });
	console.log("find");
	await db.insert("users", { username: "b", passwd: "b" });
	await db.insert("users", { username: "c", passwd: "c" });
	await db.insert("users", { username: "d", passwd: "d" });
	await db.insert("users", { username: "e", passwd: "e" });
	await db.insert("users", { username: "f", passwd: "f" });
	await db.insert("users", { username: "g", passwd: "g" });

	console.log(
		await db.find("users", { filter: (e) => e.username > "a", limit: 1000 })
	);

	await db.update("users", { where: { username: "a" } }, { username: "aa" });

	console.log(await db.find("users", {}));

	await db.remove("users", { filter: (i) => i.username == "d" });

	console.log(await db.find("users", {}));

	await db.remove("users", { filter: (i) => i.username == "d" });

	console.log(await db.find("users", {}));

	await db.remove("users", {});
}

main();
