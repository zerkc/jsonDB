import fs from "fs";
import split from "split";

export class JSONDB {
	lockingTables = {};

	_generateUUID() {
		// Public Domain/MIT
		var d = new Date().getTime(); //Timestamp
		var d2 =
			(typeof performance !== "undefined" &&
				performance.now &&
				performance.now() * 1000) ||
			0; //Time in microseconds since page-load or 0 if unsupported
		return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(
			/[xy]/g,
			function (c) {
				var r = Math.random() * 16; //random number between 0 and 16
				if (d > 0) {
					//Use timestamp until depleted
					r = (d + r) % 16 | 0;
					d = Math.floor(d / 16);
				} else {
					//Use microseconds since page-load if supported
					r = (d2 + r) % 16 | 0;
					d2 = Math.floor(d2 / 16);
				}
				return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
			}
		);
	}

	async _getTableLock(name) {
		if (!fs.existsSync(`${name}__lock`)) {
			fs.writeFileSync(`${name}__lock`, "");
			return true;
		}
		return new Promise((done) => {
			if (!this.lockingTables[name]) {
				this.lockingTables[name] = [];
			}
			this.lockingTables[name].push(done);
		});
	}

	async _releaseTableLock(name) {
		if (fs.existsSync(`${name}__lock`)) {
			fs.unlinkSync(`${name}__lock`);
		}
		if (this.lockingTables[name]) {
			for (let d of this.lockingTables[name]) {
				await d();
			}
		}
	}

	async insert(table, data) {
		await this._getTableLock(table);
		data._id = this._generateUUID();
		fs.appendFileSync(table, JSON.stringify(data) + "\n", {
			encoding: "utf8",
		});
		await this._releaseTableLock(table);
		return data._id
	}

	async update(table, opts, data) {
		const { filter, limit } = opts;
		let tmptable = `##${table}`;
		await this._getTableLock(table);
		let results = await this.find(table, {
			filter,
			limit,
			extendLine: true,
		});
		let lineIndex = 0;
		if (results.length) {
			fs.writeFileSync(tmptable, "",{encoding:"utf8"})
			await new Promise((done) => {
				fs.createReadStream(table)
					.pipe(split())
					.on("data", function (line) {
						let findex = results.findIndex(
							(r) => r.__i__ == lineIndex
						);
						line = line.trim();
						if (line && findex == -1) {
							fs.appendFileSync(tmptable, line + "\n", {
								encoding: "utf8",
							});
						} else if (findex != -1) {
							line = JSON.parse(line);
							fs.appendFileSync(
								tmptable,
								JSON.stringify({ ...line, ...data }) + "\n",
								{ encoding: "utf8" }
							);
						}

						lineIndex++;
					})
					.on("end", function () {
						done();
					});
			});
			if (fs.existsSync(tmptable)) {
				fs.renameSync(tmptable, table);
			}
		}
		await this._releaseTableLock(table);
	}
	async remove(table, opts) {
		const { filter, limit } = opts;
		let tmptable = `##${table}`;
		await this._getTableLock(table);
		let results = await this.find(table, {
			filter,
			limit,
			extendLine: true,
		});
		let lineIndex = 0;
		if (results.length) {
			fs.writeFileSync(tmptable, "",{encoding:"utf8"})
			await new Promise((done) => {
				fs.createReadStream(table)
					.pipe(split())
					.on("data", function (line) {
						let findex = results.findIndex(
							(r) => r.__i__ == lineIndex
						);
						line = line.trim();
						if (line && findex == -1) {
							fs.appendFileSync(tmptable, line + "\n", {
								encoding: "utf8",
							});
						}

						lineIndex++;
					})
					.on("end", function () {
						done();
					});
			});
			if (fs.existsSync(tmptable)) {
				fs.renameSync(tmptable, table);
			}
		}
		await this._releaseTableLock(table);
	}

	find(table, opts = {}) {
		const { filter, limit, extendLine } = opts;
		return new Promise((d) => {
			let filtered = [];
			let lineIndex = 0;
			let w = fs
				.createReadStream(table)
				.pipe(split())
				.on("data", function (line) {
					line = line.trim();
					if (line) {
						line = JSON.parse(line);
						if (!filter || filter(line)) {
							if (extendLine) {
								line.__i__ = lineIndex;
							}
							filtered.push(line);
							if (limit && limit == filtered.length) {
								w.end();
							}
						}
					}
					lineIndex++;
				})
				.on("end", function () {
					d(filtered);
				});
		});
	}
}
