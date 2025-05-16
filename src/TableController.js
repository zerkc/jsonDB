import { UUIDV4 } from "./utils/uuid.js";
import fs from "fs";
import path from "path";
import { FileSystem } from "./utils/FileSystem.js";
import { deepmerge } from "./utils/deepmerge.js";
import { QueueService } from "./utils/queue.js";

export class TableController {
  tableDefinition = {
    name: "",
    disk: "",
    index: {
      _id: {
        type: "string",
        unique: true,
      },
    },
    syncTable: 100000,
    syncSave: 0,
  };
  pathdb = "";
  rows = [];
  rowsIndex = {};
  tableSyncOperation = 0;
  tableSaveOperation = 0;
  updateTableDiskcb = false;
  queue = new QueueService();

  constructor(pathdb, tableDescriptor = {}) {
    this.tableDefinition = deepmerge(this.tableDefinition, tableDescriptor);
    if (!this.tableDefinition.disk) {
      this.tableDefinition.disk = UUIDV4();
    }
    this.pathdb = pathdb;
    this._loadTableData();
    this._syncTable();
  }

  getPath() {
    return path.resolve(this.pathdb, this.tableDefinition.disk);
  }

  setUpdateEvent(cb) {
    if (typeof cb == "function") {
      this.updateTableDiskcb = cb;
    }
  }

  consolidateTable() {
    if (this.updateTableDiskcb) {
      this.updateTableDiskcb(
        this.tableDefinition.name,
        this.tableDefinition.disk,
      );
    }
  }

  _verifyId(id) {
    if (!this.rowsIndex["_id"]) {
      return true;
    }
    return !this.rowsIndex["_id"][id];
  }

  _addRow(data) {
    data = JSON.parse(JSON.stringify(data));
    let canAdded = true;
    Object.keys(this.tableDefinition.index).forEach((key) => {
      if (!this.rowsIndex[key]) {
        if (!this.tableDefinition.index[key].unique) {
          this.rowsIndex[key] = [];
        } else {
          this.rowsIndex[key] = {};
        }
      }
      if (this.tableDefinition.index[key].unique) {
        if (!this.rowsIndex[key][data[key]]) {
          this.rowsIndex[key][data[key]] = data;
        } else {
          canAdded = false;
          deepmerge(this.rowsIndex[key][data[key]], data);
        }
      } else {
        if (!this.rowsIndex[key][data[key]]) {
          this.rowsIndex[key][data[key]] = [];
        }
        this.rowsIndex[key][data[key]].push(data);
      }
    });
    if (canAdded) {
      this.rows.push(data);
    }
  }

  async _loadTableData() {
    const next = await this.queue.asyncPush();
    if (!fs.existsSync(this.getPath())) {
      next();
      return;
    }

    try {
      const FSReader = FileSystem.CreateReader(this.getPath());
      let line;
      while ((line = FSReader.readLine())) {
        if (!line) {
          break;
        }
        try {
          let idata = JSON.parse(line);
          this._addRow(idata);
        } catch (ex) {}
      }
      FSReader.close();
    } catch (ex) {}

    next();
  }

  async _syncTable() {
    const next = await this.queue.asyncPush();
    let FSWriter;
    try {
      FSWriter = FileSystem.CreateAppendWriter(this.getPath() + ".bk~~");
      for (const data of this.rows) {
        if (!data.$$deleted) {
          FSWriter.writeLine(JSON.stringify(data));
        }
      }
      FSWriter.close();
      if (fs.existsSync(this.getPath())) {
        fs.renameSync(this.getPath(), this.getPath() + ".bk~");
      }
      fs.renameSync(this.getPath() + ".bk~~", this.getPath());
      if (fs.existsSync(this.getPath() + ".bk~")) {
        fs.unlinkSync(this.getPath() + ".bk~");
      }
    } catch (ex) {
      if (FSWriter) {
        FSWriter.close();
      }
    }
    this.consolidateTable();
    next();
  }

  async insert(data = {}) {
    let _id = UUIDV4();
    while (!this._verifyId(_id)) {
      _id = UUIDV4();
    }
    data._id = _id;
    this._addRow(data);

    let FSWriter;
    try {
      FSWriter = FileSystem.CreateWriter(this.getPath());
      FSWriter.writeLine(JSON.stringify(data));
      FSWriter.close();
    } catch (err) {
      if (FSWriter) {
        FSWriter.close();
      }
    }
  }

  _getCandidates(options) {
    const rows = this._getFilterCandidates(options);
    if (options.limit) {
      return rows.slice(0, options.limit);
    }
    return rows;
  }

  _getFilterCandidates(options) {
    if (
      !options ||
      Object.keys(options).length == 0 ||
      (!options.where && !options.filter)
    ) {
      return this.rows.filter((row) => !row.$$deleted);
    }
    if (options.where) {
      for (let key of Object.keys(options.where)) {
        if (options.where[key] instanceof RegExp) {
          if (this.tableDefinition.index[key]) {
            let keys = Object.keys(this.rowsIndex).find((k) =>
              k.test(options.where[key]),
            );
            return keys
              .map((k) => this.rowsIndex[key][k])
              .filter((row) => !row.$$deleted);
          } else {
            return this.rows
              .filter((row) => row[key].test(options.where[key]))
              .filter((row) => !row.$$deleted);
          }
        } else if (typeof options.where[key] != "object") {
          if (this.tableDefinition.index[key]) {
            const result = this.rowsIndex[key][options.where[key]];
            return (Array.isArray(result) ? result : [result]).filter(
              (row) => !row.$$deleted,
            );
          } else {
            return this.rows
              .filter((row) => {
                return row[key] == options.where[key];
              })
              .filter((row) => !row.$$deleted);
          }
        }
      }
    } else if (options.filter) {
      return this.rows.filter((row) => !row.$$deleted).filter(options.filter);
    }
  }

  async update(options = {}, data = {}) {
    const next = await this.queue.asyncPush();
    const rows = this._getCandidates(options);
    if (rows) {
      let FSWriter;
      try {
        FSWriter = FileSystem.CreateAppendWriter(this.getPath());
        for (const row of rows) {
          this.tableSyncOperation++;
          deepmerge(row, data);
          FSWriter.writeLine(JSON.stringify(row));
        }
        FSWriter.close();
      } catch (err) {
        if (FSWriter) {
          FSWriter.close();
        }
      }
    }

    if (this.tableSyncOperation >= this.tableDefinition.syncTable) {
      this._syncTable();
      this.tableSyncOperation = 0;
    }

    next();
  }

  async remove(options = {}) {
    const next = await this.queue.asyncPush();
    const rows = this._getCandidates(options);
    if (rows) {
      let FSWriter;
      try {
        FSWriter = FileSystem.CreateAppendWriter(this.getPath());
        for (const row of rows) {
          this.tableSyncOperation++;
          deepmerge(row, { $$deleted: true });
          FSWriter.writeLine(JSON.stringify(row));
        }
        FSWriter.close();
      } catch (err) {
        if (FSWriter) {
          FSWriter.close();
        }
      }
    }

    if (this.tableSyncOperation >= this.tableDefinition.syncTable) {
      this._syncTable();
      this.tableSyncOperation = 0;
    }

    next();
  }

  async find(options = {}) {
    const next = await this.queue.asyncPush();
    const results = this._getCandidates(options);
    const rows = JSON.parse(JSON.stringify(results));
    next();
    if (rows) {
      return rows;
    }
    return [];
  }

  async count(options = {}) {
    return (await this.find(options)).length;
  }

  drop() {
    fs.unlinkSync(this.getPath());
  }
}
