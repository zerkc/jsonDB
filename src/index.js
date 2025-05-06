import fs from "fs";
import path from "path";
import { TableController } from "./TableController.js";
import { QueueService } from "./utils/queue.js";

function promisify(fun) {
  return function (...args) {
    return new Promise((resolve, reject) => {
      fun.apply(
        fun,
        [].concat(args, (err, res) => (err ? reject(err) : resolve(res))),
      );
    });
  };
}

fs.appendFileAsync = promisify(fs.appendFile);
fs.readdirAsync = promisify(fs.readdir);
fs.renameAsync = promisify(fs.rename);
fs.existsAsync = (s) => new Promise((r) => fs.stat(s, (e) => r(!e)));
fs.mkdirAsync = promisify(fs.mkdir);
fs.statAsync = promisify(fs.stat);
fs.readFileAsync = promisify(fs.readFile);
fs.writeFileAsync = promisify(fs.writeFile);
fs.unlinkAsync = fs.unlinkSync;
fs.rmDirAsync = promisify(fs.rmdir);
fs.copyFileAsync = promisify(fs.copyFile);

export class JSONDB {
  opts = {};
  pathStore = "";
  tableDefinition = {};
  tableInstances = {};
  queue = new QueueService();

  constructor(pathdb = "./.db/", opts = {}) {
    this.pathStore = pathdb;
    this.opts = { ...opts, opts };
    this._init();
  }

  async _init() {
    const next = await this.queue.asyncPush();
    if (!(await fs.existsAsync(path.resolve(this.pathStore)))) {
      await fs.mkdirAsync(path.resolve(this.pathStore));
    }
    if (await fs.existsAsync(path.resolve(this.pathStore, "_tables_"))) {
      try {
        this.tableDefinition = JSON.parse(
          await fs.readFileAsync(path.resolve(this.pathStore, "_tables_"), {
            encoding: "utf8",
          }),
        );
        console.log(this.tableDefinition);
      } catch (ex) {
        console.log(ex);
      }
    }
    next();
  }

  async updateDefinitions() {
    try {
      await fs.writeFileAsync(
        path.resolve(this.pathStore, "_tables_"),
        JSON.stringify(this.tableDefinition),
        { encoding: "utf8" },
      );
    } catch (ex) {}
  }

  async getTable(table) {
    const next = await this.queue.asyncPush();
    next();
    if (this.tableInstances[table]) {
      return this.tableInstances[table];
    }
    if (this.tableDefinition[table]) {
      this.tableInstances[table] = new TableController(this.pathStore, {
        name: table,
        disk: this.tableDefinition[table].name,
      });

      this.tableInstances[table].setUpdateEvent((name, uuid) => {
        this.tableDefinition[name] = {
          name: uuid,
        };
        this.updateDefinitions();
      });

      return this.tableInstances[table];
    }

    this.tableInstances[table] = new TableController(this.pathStore, {
      name: table,
    });
    this.tableDefinition[table] = {
      name: this.tableInstances[table].tableDisk,
    };
    this.tableInstances[table].setUpdateEvent((name, uuid) => {
      this.tableDefinition[name] = {
        name: uuid,
      };
      this.updateDefinitions();
    });
    await this.updateDefinitions();
    return this.tableInstances[table];
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
