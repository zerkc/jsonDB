import fs from "fs";
import path from "path";
import split from "split";

export class JSONDB {
	lockingTables = {};
	pathStore = "";

	constructor(pathdb = "./.db/", opts = {autoRemoveLock: true}){
		this.pathStore = pathdb;
		if(!fs.existsSync(path.resolve(this.pathStore))){
			fs.mkdirSync(path.resolve(this.pathStore));
		}
		if(opts.autoRemoveLock){
			this.removeLockings();
		}
	}

	removeLockings(){
		let tables = fs.readdirSync(this.pathStore);
		for(let table of tables){
			if(table.startsWith("##") || table.endsWith("__lock")){
				fs.unlinkSync(table);
			}
		}
	}

	_appendFile(filepath, content, opts = {encoding:"utf8"}){
		return fs.appendFileSync(path.resolve(this.pathStore, filepath),content,opts);
	}

	_writeFile(filepath, content, opts = {encoding:"utf8"}){
		return fs.writeFileSync(path.resolve(this.pathStore, filepath),content,opts);
	}
	_existsFile(filepath){
		return fs.existsSync(path.resolve(this.pathStore,filepath));
	}
	_readStream(filepath){
		return fs.createReadStream(path.resolve(this.pathStore,filepath));
	}	
	_deleteFile(filepath){
		return fs.unlinkSync(path.resolve(this.pathStore,filepath));
	}

	_renameFile(oldFilePath, newFilePath){
		return fs.renameSync(path.resolve(this.pathStore,oldFilePath), path.resolve(this.pathStore,newFilePath));
	}


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
		if (!this._existsFile(`${name}__lock`)) {
			this._writeFile(`${name}__lock`, "");
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
		if (this._existsFile(`${name}__lock`)) {
			this._deleteFile(`${name}__lock`);
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
		this._appendFile(table, JSON.stringify(data) + "\n", {
			encoding: "utf8",
		});
		await this._releaseTableLock(table);
		return data._id
	}

	async update(table, opts, data) {
		const { filter, limit, where } = opts;
		let tmptable = `##${table}`;
		await this._getTableLock(table);
		let results = await this.find(table, {
			filter,
			limit,
			extendLine: true,
			where
		});
		let lineIndex = 0;
		if (results.length) {
			this._writeFile(tmptable, "",{encoding:"utf8"})
			await new Promise((done) => {
				this._readStream(table)
					.pipe(split())
					.on("data",  (line) => {
						let findex = results.findIndex(
							(r) => r.__i__ == lineIndex
						);
						line = line.trim();
						if (line && findex == -1) {
							this._appendFile(tmptable, line + "\n", {
								encoding: "utf8",
							});
						} else if (findex != -1) {
							line = JSON.parse(line);
							this._appendFile(
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
			if (this._existsFile(tmptable)) {
				this._renameFile(tmptable, table);
			}
		}
		await this._releaseTableLock(table);
	}
	async remove(table, opts) {
		const { filter, limit, where } = opts;
		let tmptable = `##${table}`;
		await this._getTableLock(table);
		let results = await this.find(table, {
			filter,
			limit,
			extendLine: true,
			where
		});
		let lineIndex = 0;
		if (results.length) {
			this._writeFile(tmptable, "",{encoding:"utf8"})
			await new Promise((done) => {
				this._readStream(table)
					.pipe(split())
					.on("data", (line) => {
						let findex = results.findIndex(
							(r) => r.__i__ == lineIndex
						);
						line = line.trim();
						if (line && findex == -1) {
							this._appendFile(tmptable, line + "\n", {
								encoding: "utf8",
							});
						}

						lineIndex++;
					})
					.on("end", function () {
						done();
					});
			});
			if (this._existsFile(tmptable)) {
				this._renameFile(tmptable, table);
			}
		}
		await this._releaseTableLock(table);
	}

	async count(table,opts = {}){
		const results = await this.find(table,opts);
		return (results || []).length;
	}

	find(table, opts = {}) {
		const { filter, limit, extendLine, where } = opts;
		if(!this._existsFile(table)){
			return [];
		}
		return new Promise((d, reject) => {
			let filtered = [];
			let lineIndex = 0;
			try {
				let w = this._readStream(table)
					.pipe(split())
					.on("data", (line) => {
						line = line.trim();
						if (line) {
							try {
								line = JSON.parse(line);
								if(!where && !filter){
									if (extendLine) {
										line.__i__ = lineIndex;
									}
									filtered.push(line);
									if (limit && limit == filtered.length) {
										w.end();
									}
								} else if (filter && filter(line)) {
									if (extendLine) {
										line.__i__ = lineIndex;
									}
									filtered.push(line);
									if (limit && limit == filtered.length) {
										w.end();
									}
								}else if(where){
									let find = true;
									for(let k in where){
										if (where.hasOwnProperty(k)) {
											if(typeof where[k] == "string"){
												if(where[k] != line[k]){
													find = false;
												}
											}else if( where[k] instanceof RegExp){
												if(!where[k].test(line[k])){
													find = false;
												}
											}
										}
									}
									if(find){
										if (extendLine) {
											line.__i__ = lineIndex;
										}
										filtered.push(line);
										if (limit && limit == filtered.length) {
											w.end();
										}
									}
								}
							} catch (ex) {
								//skip error
								//reject(ex.message);
							}
						}
						lineIndex++;
					})
					.on("error", function (err) {
						reject(`${err}`);
					})
					.on("end", function () {
						d(filtered);
					});
			} catch (ex) {
				reject(ex.message);
			}
		});
		
	}
}
