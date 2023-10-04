import fs from "fs";
import path from "path";
import split from "split";

function promisify(fun) {
  return function(...args) {
    return new Promise((resolve, reject) => {
      fun.apply(fun, [].concat(args, (err, res) => (err) ? reject(err) : resolve(res)))
    })
  }
}

fs.appendFileAsync = promisify(fs.appendFile);
fs.readdirAsync = promisify(fs.readdir);
fs.renameAsync = promisify(fs.rename);
fs.existsAsync = s => new Promise(r => fs.stat(s, (e) => r(!e)));
fs.mkdirAsync = promisify(fs.mkdir);
fs.statAsync = promisify(fs.stat);
fs.readFileAsync = promisify(fs.readFile);
fs.writeFileAsync = promisify(fs.writeFile);
fs.unlinkAsync = promisify(fs.unlink);
fs.rmDirAsync = promisify(fs.rmdir);
fs.copyFileAsync = promisify(fs.copyFile);


class TableController {
  tableName = "";
  tableDisk = "";
  pathdb = "";
  locked = false;
  actionsQueue = [];
  updateTableDiskcb = false;

  constructor(pathdb, tableName, tableDisk = "") {
    if (!tableDisk) {
      tableDisk = this._generateUUID();
    }

    this.pathdb = pathdb;
    this.tableName = tableName;
    this.tableDisk = tableDisk;
  }

  setUpdateEvent(cb) {
    if (typeof cb == "function") {
      this.updateTableDiskcb = cb;
    }
  }


  addActionQueue(action, params) {
    return new Promise(done => {
      this.actionsQueue.push({
        action,
        params,
        resolve: done
      })
    })
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

  async insert(data = {}) {
    if (this.locked) {
      return this.addActionQueue("INSERT", { data });
    }
    this.locked = true;
    if (Array.isArray(data)) {
      for (const d of data) {
        await this.insert(d);
      }
    } else {
      data._id = this._generateUUID();
      const writer = fs.createWriteStream(path.resolve(this.pathdb, this.tableDisk), {
        flags: 'a'
      });
      await new Promise(done => {
        writer.write(`${JSON.stringify(data)}\n`, done);
      })
      writer.close();
    }
    this.locked = false;
    this.processQueue();
  }
  updateTableDisk() {
    this.tableDisk = this._generateUUID();
    if (this.updateTableDiskcb) {
      this.updateTableDiskcb(this.tableName, this.tableDisk);
    }
  }

  async update(options = {}, data = {}) {
    if (this.locked) {
      return this.addActionQueue("UPDATE", { options, data });
    }
    this.locked = true;
    await new Promise(async (done) => {
      const results = [];
      if (!fs.existsSync(path.resolve(this.pathdb, this.tableDisk))) {
        fs.writeFileSync(path.resolve(this.pathdb, this.tableDisk), "")
      }
      const reader = fs.createReadStream(path.resolve(this.pathdb, this.tableDisk));
      this.updateTableDisk();
      const writer = fs.createWriteStream(path.resolve(this.pathdb, this.tableDisk), {
        flags: 'w'
      });

      reader.pipe(split())
        .on("data", async (line) => {
          try {
            let idata = JSON.parse(line);
            if (this.verifyLineWhere(options, idata)) {
              if (options.limit) {
                if (options.limit > results.length) {
                  idata = { ...idata, ...data };
                }
              }
            }
            results.push(await new Promise(done => {
              writer.write(`${JSON.stringify(idata)}\n`, done);
            }));
          } catch (ex) {
          }

        })
        .on('error', console.error)
        .on('close', async () => {
          if (await Promise.all(results)) {
            await fs.unlinkAsync(reader.path)
            done();
          }
        });
    })

    this.locked = false;
    this.processQueue();
  }

  async remove(options) {
    if (this.locked) {
      return this.addActionQueue("REMOVE", { options });
    }
    this.locked = true;
    await new Promise((done) => {
      const results = [];
      if (!fs.existsSync(path.resolve(this.pathdb, this.tableDisk))) {
        fs.writeFileSync(path.resolve(this.pathdb, this.tableDisk), "")
      }
      const reader = fs.createReadStream(path.resolve(this.pathdb, this.tableDisk));
      this.updateTableDisk();
      const writer = fs.createWriteStream(path.resolve(this.pathdb, this.tableDisk), {
        flags: 'w'
      });

      reader.pipe(split())
        .on("data", async (line) => {
          try {
            let data = JSON.parse(line);
            if (!this.verifyLineWhere(options, data)) {
              results.push(await new Promise(done => {
                writer.write(`${JSON.stringify(data)}\n`, done);
              }));
            }
          } catch (ex) {
          }
          if (options.limit) {
            if (options.limit == results.length) {
              //reader.close();
            }
          }
        })
        .on('error', console.error)
        .on('close', async () => {
          if (await Promise.all(results)) {
            await fs.unlinkAsync(reader.path)
            done(results);
          }
        });
    })
    this.locked = false;
    this.processQueue();
  }


  async find(options = {}) {
    if (this.locked) {
      return this.addActionQueue("FIND", { options });
    }
    this.locked = true;


    const response = await new Promise((done) => {
      const results = [];
      if (!fs.existsSync(path.resolve(this.pathdb, this.tableDisk))) {
        fs.writeFileSync(path.resolve(this.pathdb, this.tableDisk), "")
      }
      const reader = fs.createReadStream(path.resolve(this.pathdb, this.tableDisk));
      reader.pipe(split())
        .on("data", (line) => {
          try {
            let data = JSON.parse(line);
            if (this.verifyLineWhere(options, data)) {
              results.push(data);
            }
          } catch (ex) {
          }
          if (options.limit) {
            if (options.limit == results.length) {
              reader.close();
              done(results)
            }
          }
        })
        .on('error', console.error)
        .on('close', () => {
          done(results);
        });
    })

    this.locked = false;
    this.processQueue();
    return response;
  }

  async count(options = {}) {
    const results = await this.find(options);
    return results.length;
  }


  verifyLineWhere(options, data) {
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

    return false
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
      function(c) {
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
}



export class JSONDB {
  opts = {};
  pathStore = "";
  tableDefinition = {};
  tableInstances = {};

  constructor(pathdb = "./.db/", opts = {}) {
    this.pathStore = pathdb;
    this.opts = { ...opts, opts };
    this.loaded = new Promise(done => {
      this._init(done);
    })
  }

  async _init(done) {
    if (!await fs.existsAsync(path.resolve(this.pathStore))) {
      await fs.mkdirAsync(path.resolve(this.pathStore));
    }
    if (await fs.existsAsync(path.resolve(this.pathStore, "_tables_"))) {
      try {
        this.tableDefinition = JSON.parse(await fs.readFileAsync(path.resolve(this.pathStore, "_tables_"), { encoding: "utf8" }));
      } catch (ex) {
        console.log(ex);
      }
    }
    done();
  }

  async updateDefinitions() {
    try {
      await fs.writeFileAsync(path.resolve(this.pathStore, "_tables_"), JSON.stringify(this.tableDefinition), { encoding: "utf8" });
    } catch (ex) { }
  }



  async getTable(table) {
    await this.loaded;
    if (this.tableInstances[table]) {
      return this.tableInstances[table];
    }
    if (this.tableDefinition[table]) {
      this.tableInstances[table] = new TableController(this.pathStore, table, this.tableDefinition[table].name);

      this.tableInstances[table].setUpdateEvent((name, uuid) => {
        this.tableDefinition[name] = {
          name: uuid
        };
        this.updateDefinitions();
      })

      return this.tableInstances[table];
    }

    this.tableInstances[table] = new TableController(this.pathStore, table);
    this.tableDefinition[table] = {
      name: this.tableInstances[table].tableDisk
    };
    this.tableInstances[table].setUpdateEvent((name, uuid) => {
      this.tableDefinition[name] = {
        name: uuid
      };
      this.updateDefinitions();
    })
    await this.updateDefinitions()
    return this.tableInstances[table]
  }

  async insert(table, data) {
    const tablei = await this.getTable(table);
    return tablei.insert(data);
  }

  async update(table, opts, data) {
    const tablei = await this.getTable(table);
    return tablei.update(opts, data);
  }
  async remove(table, opts) {
    const tablei = await this.getTable(table);
    return tablei.remove(opts);
  }

  async count(table, opts = {}) {
    const tablei = await this.getTable(table);
    return tablei.count(opts);
  }

  async find(table, opts = {}) {
    const tablei = await this.getTable(table);
    return tablei.find(opts);
  }
}
