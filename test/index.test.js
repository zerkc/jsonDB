import { JSONDB } from "../src/index.js";
import { expect, test } from "@jest/globals";
import fs from "fs";

if (fs.existsSync(".db/")) {
    fs.rmSync(".db/", { recursive: true });
}
const db = new JSONDB(undefined, {
    keepInRam: true,
    transactionWrite: true,
    writeTime: 5000,
});

test("Creacion u1", async () => {
    const user = { username: "a", passwd: "a" };
    await db.insert("users", { ...user });
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("Creacion u2", async () => {
    const user = { username: "b", passwd: "b" };
    await db.insert("users", { ...user });
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("Creacion u3", async () => {
    const user = { username: "c", passwd: "c" };
    await db.insert("users", { ...user });
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("Update u1", async () => {
    const user = { username: "a", passwd: "b" };
    await db.update(
        "users",
        { where: { username: user.username } },
        { passwd: user.passwd }
    );
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("Update u2", async () => {
    const user = { username: "b", passwd: "c" };
    await db.update(
        "users",
        { where: { username: user.username } },
        { passwd: user.passwd }
    );
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("Update u3", async () => {
    const user = { username: "c", passwd: "d" };
    await db.update(
        "users",
        { where: { username: user.username } },
        { passwd: user.passwd }
    );
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("update 4 testing full update", async () => {
    await db.insert("flags", { name: "testing", value: -1 });

    for (let i = 0; i < 10_000; i++) {
        // await db.update("flags", { where: { name: "testing" } }, { value: 1 });
        await db.update("flags", { where: { name: "testing" } }, { value: i });
    }
});

test("delete u1", async () => {
    const user = { username: "a" };
    await db.remove("users", { where: { username: user.username } });
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeUndefined();
});

test("delete u2", async () => {
    const user = { username: "b" };
    await db.remove("users", { where: { username: user.username } });
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeUndefined();
});

test("delete u3", async () => {
    const user = { username: "c" };
    await db.remove("users", { where: { username: user.username } });
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeUndefined();
});

test("Error insert", async () => {
    fs.chmodSync(".db/", "0222");
    const user = { username: "e", passwd: "e" };

    await expect(db.insert("users", { ...user })).rejects.toMatch("permission");

    fs.chmodSync(".db/", "0777");
    const u = await db.find("users", { where: user });
    expect(u[0]).toBeUndefined();
});

test("Error update", async () => {
    const user = { username: "e", passwd: "e" };
    await db.insert("users", { ...user });
    fs.chmodSync(".db/", "0222");

    await expect(
        db.update("users", { username: user.username }, { passwd: "h" })
    ).rejects.toMatch("permission");

    fs.chmodSync(".db/", "0777");
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("Error delete", async () => {
    const user = { username: "h", passwd: "h" };
    await db.insert("users", { ...user });
    fs.chmodSync(".db/", "0222");

    await expect(
        db.remove("users", { username: user.username })
    ).rejects.toMatch("permission");

    fs.chmodSync(".db/", "0777");
    const u = await db.find("users", { where: user });
    expect(u).toBeDefined();
    expect(u[0]).toBeDefined();
    expect(user.username).toBe(u[0].username);
    expect(user.passwd).toBe(u[0].passwd);
});

test("Error find", async () => {
    fs.chmodSync(".db/", "0444");
    const user = { username: "e", passwd: "e" };
    await expect(db.find("users", { username: user.username })).rejects.toMatch(
        "permission"
    );
    fs.chmodSync(".db/", "0777");
});

test("all data", async () => {
    let now = new Date();
    now.setMonth(now.getMonth() - 3);
    let last_id = 0;
    console.time("insert");
    for (let i = 0; i < 1_000_000; i++) {
        await db.insert("stats", {
            sc_order: `${i}`,
            scene: `stats - akjfshsk - asdjfhakf - assad - ${i + 1}`,
            scene_image: ``,
            id_scene: `${i}`,
            id_campaign: 0,
            timestamp: now,
        });
        now.setMinutes(now.getMinutes() + 8);
    }
    console.timeEnd("insert");

    for (let i = 0; i < 6; i++) {
        console.time("find");
        let info = await db.find("stats", {
            filter: (e) => {
                return new Date(e.timestamp).getTime() > (last_id || 0);
            },
            limit: 1000,
        });
        console.timeEnd("find");
        console.log(info.length);

        console.time("count");
        let count = await db.count("stats");
        console.timeEnd("count");

        expect(info.length).toBeGreaterThanOrEqual(1000);

        last_id = new Date(info[info.length - 1].timestamp).getTime();
        console.time("delete");
        await db.remove("stats", {
            filter: (e) => {
                let eDate = new Date(e.timestamp);
                eDate.setMilliseconds(0);
                return eDate.getTime() <= (last_id || 0);
            },
        });
        console.timeEnd("delete");

        let beforecount = await db.count("stats");

        expect(beforecount).toBe(count - info.length);
    }

    // await db.remove("stats");
});

// async function main() {
// 	for (let i = 0; i < 1; i++) {
// 		db.insert("users", { username: "a", passwd: "a" });
// 		db.insert("users", { username: "b", passwd: "b" });
// 		db.insert("users", { username: "c", passwd: "c" });
// 		db.insert("users", { username: "d", passwd: "d" });
// 		db.insert("users", { username: "e", passwd: "e" });
// 		db.insert("users", { username: "f", passwd: "f" });
// 		db.insert("users", { username: "g", passwd: "g" });
// 	}

// 	let users = await db.count("users");
// 	console.log(2, users);

// 	await db.update("users", { where: { username: "a" } }, { username: "aa" });

// 	await db.remove("users", { filter: (i) => i.username == "d" });

// 	await db.remove("users", { filter: (i) => i.username == "d" });

// 	console.log(await db.count("users", {}));

// 	db.remove("users", {});

// 	console.log(await db.count("users", {}));
// }

// main();
