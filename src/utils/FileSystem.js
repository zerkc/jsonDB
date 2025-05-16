import fs from "fs";

export class FileSystem {
  filepath = null;
  remaining = "";
  loaded = false;
  flags = null;

  constructor(filepath) {
    this.filepath = filepath;
  }

  init(flag) {
    this.flags = flag;
    if (!fs.existsSync(this.filepath)) {
      fs.writeFileSync(this.filepath, "");
    }
  }

  readLine() {
    if (this.remaining === "" && loaded === false) {
      this.loaded = true;
      this.remaining = fs.readFileSync(this.filepath, "utf8");
    }
    let lines = this.remaining.split("\n");
    const line = lines.shift();
    this.remaining = lines.join("\n");
    if (line) {
      return line;
    }
    return null;
  }

  writeLine(data) {
    fs.writeFileSync(this.filepath, `${data}\n`, {
      flag: this.flags,
      encoding: "utf8",
    });
  }

  close() {
    return;
  }

  delete() {
    return fs.unlinkSync(this.filepath);
  }

  static CreateReader(filepath) {
    const fd = new FileSystem(filepath);
    fd.init("r");
    return fd;
  }

  static CreateWriter(filepath) {
    const fd = new FileSystem(filepath);
    fd.init("w");
    return fd;
  }
  static CreateAppendWriter(filepath) {
    const fd = new FileSystem(filepath);
    fd.init("a");
    return fd;
  }
}
