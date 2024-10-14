import { UUIDV4 } from "./utils/uuid.js";
import fs from "fs";
import path from "path";
import { FileSystem } from "./utils/FileSystem.js";

export class TableController {
	tableName = "";
	tableDisk = "";
	pathdb = "";
	locked = false;
	actionsQueue = [];
	updateTableDiskcb = false;
	writers_queue = {};

	constructor(pathdb, tableName, tableDisk = "") {
		if (!tableDisk) {
			tableDisk = UUIDV4();
		}

		this.pathdb = pathdb;
		this.tableName = tableName;
		this.tableDisk = tableDisk;
	}

	getPath() {
		return path.resolve(this.pathdb, this.tableDisk);
	}

	updateTableDisk() {
		this.tableDisk = UUIDV4();
	}

	setUpdateEvent(cb) {
		if (typeof cb == "function") {
			this.updateTableDiskcb = cb;
		}
	}

	consolidateTable() {
		if (this.updateTableDiskcb) {
			this.updateTableDiskcb(this.tableName, this.tableDisk);
		}
	}

	addActionQueue(action, params) {
		return new Promise((done) => {
			this.actionsQueue.push({
				action,
				params,
				resolve: done,
			});
		});
	}

	async processQueue() {
		const queue = this.actionsQueue.shift();
		if (queue) {
			if (queue.action == "INSERT") {
				await this.insert(queue.params.data);
				queue.resolve();
			} else if (queue.action == "UPDATE") {
				await this.update(queue.params.options, queue.params.data);
				queue.resolve();
			} else if (queue.action == "REMOVE") {
				await this.remove(queue.params.options);
				queue.resolve();
			} else if (queue.action == "FIND") {
				const results = await this.find(queue.params.options);
				queue.resolve(results);
			}
		}
	}

	verifyLineWhere(options, data) {
		if (!options) {
			return true;
		}
		if (options.where) {
			for (let k in options.where) {
				if (options.where.hasOwnProperty(k)) {
					if (options.where[k] instanceof RegExp) {
						if (options.where[k].test(data[k])) {
							return true;
						}
					} else {
						if (data[k] == options.where[k]) {
							return true;
						}
					}
					return false;
				}
			}
		} else if (options.filter) {
			return options.filter(data);
		} else {
			return true;
		}

		return false;
	}

	async insert(data = {}) {
		if (this.locked) {
			return this.addActionQueue("INSERT", { data });
		}
		this.locked = true;

		let FSWriter;
		try {
			FSWriter = await FileSystem.CreateAppendWriter(this.getPath());
			if (Array.isArray(data)) {
				for (const d of data) {
					await this.insert(d);
				}
			} else {
				data._id = UUIDV4();
				await FSWriter.writeLine(JSON.stringify(data));
				await FSWriter.close();
			}
			this.locked = false;
			this.processQueue();
		} catch (ex) {
			this.locked = false;
			if (FSWriter) {
				await FSWriter.close();
			}
			throw ex.message;
		}
	}

	async update(options = {}, data = {}) {
		if (this.locked) {
			return this.addActionQueue("UPDATE", { data, options });
		}
		this.locked = true;

		const FSReader = await FileSystem.CreateReader(this.getPath());
		this.updateTableDisk();
		const fileDescriptorWriter = fs.openSync(this.getPath(), "w");

		let line;
		let updates = 0;
		let endUpdated = false;
		while ((line = await FSReader.readLine())) {
			if (!line) {
				break;
			}
			try {
				let idata = JSON.parse(line);
				if (this.verifyLineWhere(options, idata) && !endUpdated) {
					idata = { ...idata, ...data };
					updates++;
				}
				if (options.limit && options.limit <= updates) {
					endUpdated = true;
				}
				fs.writeSync(
					fileDescriptorWriter,
					`${JSON.stringify(idata)}\n`
				);
			} catch (ex) {
				console.log(ex.message);
			}
		}

		await FSReader.close();
		await FSReader.delete();
		fs.closeSync(fileDescriptorWriter);
		this.consolidateTable();
		this.locked = false;
		this.processQueue();
	}

	async remove(options = {}) {
		if (this.locked) {
			return this.addActionQueue("REMOVE", { options });
		}
		this.locked = true;

		const FSReader = await FileSystem.CreateReader(this.getPath());
		this.updateTableDisk();
		const FSWriter = await FileSystem.CreateWriter(this.getPath());

		let line;
		let deleted = 0;
		let endDeleted = false;
		while ((line = await FSReader.readLine())) {
			if (!line) {
				break;
			}
			try {
				let idata = JSON.parse(line);

				if (!this.verifyLineWhere(options, idata) || endDeleted) {
					await FSWriter.writeLine(JSON.stringify(idata));
				} else {
					deleted++;
				}

				if (options.limit && options.limit <= deleted) {
					endDeleted = true;
				}
			} catch (ex) {}
		}

		await FSReader.close();
		await FSReader.delete();
		await FSWriter.close();

		this.consolidateTable();
		this.locked = false;
		this.processQueue();
	}

	async find(options = {}) {
		if (this.locked) {
			return this.addActionQueue("FIND", { options });
		}
		this.locked = true;
		const results = [];

		const FSReader = await FileSystem.CreateReader(this.getPath());
		let line;
		while ((line = await FSReader.readLine())) {
			if (!line) {
				break;
			}
			try {
				let idata = JSON.parse(line);
				if (this.verifyLineWhere(options, idata)) {
					if (!options.limit || options.limit > results.length) {
						results.push(idata);
					} else {
						break;
					}
				}
			} catch (ex) {}
		}
		await FSReader.close();

		this.locked = false;
		this.processQueue();
		return results;
	}

	async count(options = {}) {
		const results = await this.find(options);
		return results.length;
	}

	drop() {
		fs.unlinkSync(this.getPath());
	}
}
