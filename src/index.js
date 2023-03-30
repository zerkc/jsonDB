import fs from "fs";
import path from "path";
import split from "split";

function promisify(fun) {
    return function (...args) {
        return new Promise((resolve, reject) => {
            fun.apply(fun, [].concat(args, (err, res) => (err) ? reject(err) : resolve(res)))
        })
    }
}

fs.appendFileAsync = promisify(fs.appendFile);
fs.readdirAsync = promisify(fs.readdir);
fs.renameAsync = promisify(fs.rename);
fs.existsAsync = s=>new Promise(r=>fs.stat(s, (e) => r(!e)));
fs.mkdirAsync = promisify(fs.mkdir);
fs.statAsync = promisify(fs.stat);
fs.readFileAsync = promisify(fs.readFile);
fs.writeFileAsync = promisify(fs.writeFile);
fs.unlinkAsync = promisify(fs.unlink);
fs.rmDirAsync = promisify(fs.rmdir);
fs.copyFileAsync = promisify(fs.copyFile);



export class JSONDB {
	opts = {};
	lockingTables = {};
	pathStore = "";
	tableDefinition = {};
	inMemory = {};



	constructor(pathdb = "./.db/", opts = {autoRemoveLock: true, transactionWrite: false, writeTime: 60000, keepInRam: false}){
		this.pathStore = pathdb;
		this.opts = {...opts, opts};
			
		this._init();	
	}

	async _init(){
		if(!await fs.existsAsync(path.resolve(this.pathStore))){
			await fs.mkdirAsync(path.resolve(this.pathStore));
		}
		if(await fs.existsAsync(path.resolve(this.pathStore,"_tables_"))){
			try{
				this.tableDefinition = JSON.parse(await fs.readFileAsync(path.resolve(this.pathStore,"_tables_"),{encoding:"utf8"}));
			}catch(ex){
			}
		}
		if(this.opts.transactionWrite){
			this.keepInRam = true;
			this.startWrites();
		}
		if(this.opts.keepInRam){
			for(let k in this.tableDefinition){
				await this.loadTableInRam(k);
			}
		}
		if(this.loaded){
			this.loaded();
		}else{
			this.loaded = Promise.resolve();
		}
	}

	WaitForLoaded(){
		if(this.loaded){
			return;
		}
		return new Promise(d=>{
			this.loaded = d;
		})
	}

	async loadTableInRam(table){
		let rtable = await this.getRealTable(table);
		if(!await this._existsFile(rtable)){
			return;
		}
		let memory = this._getTableMemory(table);
		return new Promise((done) => {
			let w = this._readStream(rtable)
				.pipe(split())
				.on("data", (line) => {
					line = line.trim();
					if (line) {
						try {
							line = JSON.parse(line);
							memory.rows.push(line);
						} catch (ex) {}
					}
				})
				.on("error", function (err) {
					console.log(`ERROR: ${err}`);
				})
				.on("end", function () {
					if (!w.destroyed) {
						w.destroy();
					}
					done();
				});
		});	
	}

	async startWrites(){

		if(this.tableDefinition["#update"]){
			delete this.tableDefinition["#update"];
			await fs.writeFileAsync(path.resolve(this.pathStore,"_tables_"),JSON.stringify(this.tableDefinition));
		}

		for(let tableName in this.inMemory){
			let table = this.inMemory[tableName];
			if(table.hasChange){
				let rTableName = await this.getRealTable(tableName);
				await this._writeFile(rTableName, "");
				for(let row of table.rows){
					await this._appendFile(rTableName, `${JSON.stringify(row)}\n`)
				}
			}
		}

		setTimeout(()=>this.startWrites(),this.opts.writeTime);
	}

	async getRealTable(name){
		if(!this.tableDefinition[name]){
			this.tableDefinition[name] = {
				name: this._generateUUID()
			};
			await this._updateTableDefinition();
		}
		return this.tableDefinition[name].name
	}

	async getNewNameTable(name){
		if(!this.tableDefinition[name]){
			this.tableDefinition[name] = {
				name: this._generateUUID()
			};
		}
		await this._updateTableDefinition();
		return this.tableDefinition[name].name;
	}

	async tableDefinitionSync(){
		let canWritePending =false;
		for(let k in this.tableDefinition){

			if(this.tableDefinition[k].newName){
				canWritePending = true;
				this.tableDefinition[k].oldName = this.tableDefinition[k].name;
				this.tableDefinition[k].name = this.tableDefinition[k].newName;
			}
		}
		if(canWritePending){
			await this._updateTableDefinition();
		}
	}
	
	async _updateTableDefinition(){
		if(this.opts.keepInRam){
			this.tableDefinition["#update"] = true;
		}else{
			await fs.writeFileAsync(path.resolve(this.pathStore,"_tables_"),JSON.stringify(this.tableDefinition));
		}
	}

	_appendFile(filepath, content, opts = {encoding:"utf8"}){
		return fs.appendFileAsync(path.resolve(this.pathStore, filepath),content,opts);
	}

	_writeFile(filepath, content, opts = {encoding:"utf8"}){
		return fs.writeFileAsync(path.resolve(this.pathStore, filepath),content,opts);
	}
	_existsFile(filepath){
		return fs.existsAsync(path.resolve(this.pathStore,filepath));
	}
	_readStream(filepath){
		return fs.createReadStream(path.resolve(this.pathStore,filepath));
	}	
	_deleteFile(filepath){
		return fs.unlinkAsync(path.resolve(this.pathStore,filepath));
	}

	async _renameFile(oldFilePath, newFilePath){
		await fs.unlinkAsync(path.resolve(this.pathStore,newFilePath));
		await fs.copyFileAsync(path.resolve(this.pathStore,oldFilePath), path.resolve(this.pathStore,newFilePath));

		return;
		return fs.renameAsync(path.resolve(this.pathStore,oldFilePath), path.resolve(this.pathStore,newFilePath));
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
		if (this.lockingTables[name] === undefined) {
			this.lockingTables[name] = [];
			return true;
		}
		return await new Promise((done) => {
			this.lockingTables[name].push(done);
		});
	}

	async _releaseTableLock(name) {
		if (this.lockingTables[name]) {
			for (let d of this.lockingTables[name]) {
				await d();
			}
			delete this.lockingTables[name];
		}
	}

	_getTableMemory(table){
		if(!this.inMemory[table]){
			this.inMemory[table] = {
				rows:[]
			};
		}
		return this.inMemory[table];
	}

	async insert(table, data) {
		await this.WaitForLoaded();
		await this._getTableLock(table);
		let _table = await this.getRealTable(table);
		data._id = this._generateUUID();
		if(this.opts.keepInRam){
			let memory = this._getTableMemory(table);
			memory.hasChange = true;
			memory.rows.push(data);
		}else{
			await this._appendFile(_table, JSON.stringify(data) + "\n", {
				encoding: "utf8",
			});
		}
		await this._releaseTableLock(table);
		return data._id
	}

	async update(table, opts, data) {
		await this.WaitForLoaded();
		const { filter, limit, where } = opts;
		await this._getTableLock(table);
		let _table = await this.getRealTable(table);
		let results = await this.find(table, {
			filter,
			limit,
			extendLine: true,
			where
		});
		let lineIndex = 0;
		if(this.opts.keepInRam){
			let memory = this._getTableMemory(table);
			if(results.length){
				memory.hasChange = true;
				for(let r of results){
					memory.rows[r.__i__] = {
						...memory.rows[r.__i__],
						...data,
						__i__:undefined
					};
				
				}
			}

			await this._releaseTableLock(table);
			return;
		}
		if (results.length) {
			let tmptable = await this.getNewNameTable(table);
			await this._writeFile(tmptable, "",{encoding:"utf8"})
			await new Promise((done) => {
				const w = this._readStream(_table)
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
						/*
						if(!w.destroyed){
							w.destroy();
						}
						*/
						done();
					});
			});
		}
		await this.tableDefinitionSync();
		await this._releaseTableLock(table);
	}
	async remove(table, opts) {
		await this.WaitForLoaded();
		const { filter, limit, where } = opts;
		await this._getTableLock(table);
		let _table = await this.getRealTable(table);
		let results = await this.find(table, {
			filter,
			limit,
			extendLine: true,
			where
		});
		let lineIndex = 0;
		if(this.opts.keepInRam){
			let memory = this._getTableMemory(table);
			if(results.length){
				results.sort((a,b)=>b.__i__-a.__i__)
				memory.hasChange = true;
				for(let r of results){
					memory.rows.splice(r.__i__,1);
				}
			}

			await this._releaseTableLock(table);
			return;
		}
		if (results.length) {
			let tmptable = await this.getNewNameTable(table);
			await this._writeFile(tmptable, "",{encoding:"utf8"})
			await new Promise((done) => {
				const w = this._readStream(_table)
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
						/*
						if(!w.destroyed){
							w.destroy();
						}
						*/
						done();
					});
			});
		}
		await this.tableDefinitionSync();
		await this._releaseTableLock(table);
	}

	async count(table,opts = {}){
		const results = await this.find(table,opts);
		return (results || []).length;
	}

	async find(table, opts = {}) {
		await this.WaitForLoaded();
		const { filter, limit, extendLine, where } = opts;
		if(this.opts.keepInRam){
			let results = [];
			let memory = this._getTableMemory(table);
			let lineIndex = 0;
			for(let line of memory.rows){
				if (!where && !filter) {
					if (extendLine) {
						line.__i__ = lineIndex;
					}
					results.push(line);
					if (limit && limit == results.length) {
						return;
					}
				} else if (filter && filter(line)) {
					if (extendLine) {
						line.__i__ = lineIndex;
					}
					results.push(line);
					if (limit && limit == results.length) {
						return;
					}
				} else if (where) {
					let find = true;
					for (let k in where) {
						if (where.hasOwnProperty(k)) {
							if (typeof where[k] == "string") {
								if (where[k] != line[k]) {
									find = false;
								}
							} else if (where[k] instanceof RegExp) {
								if (!where[k].test(line[k])) {
									find = false;
								}
							}
						}
					}
					if (find) {
						if (extendLine) {
							line.__i__ = lineIndex;
						}
						results.push(line);
						if (limit && limit == results.length) {
							return;
						}
					}
				}
				lineIndex++;
			}

			return results;
		}
		table = await this.getRealTable(table);
		if(!await this._existsFile(table)){
			return [];
		}
		return await new Promise((d, reject) => {
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
						/*
						if(!w.destroyed){
							w.destroy();
						}
						*/
						d(filtered);
					});
			} catch (ex) {
				reject(ex.message);
			}
		});
		
	}
}
