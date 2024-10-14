import fs from "fs";

export class FileSystem {
	filepath = null;
	fileDescriptor = null;
	BUFFER_SIZE = 1024;
	remaining = "";

	constructor(filepath) {
		this.filepath = filepath;
	}

	async init(flag) {
		return new Promise((done, reject) => {
			if (!fs.existsSync(this.filepath)) {
				fs.writeFileSync(this.filepath, "");
			}
			fs.open(this.filepath, flag, undefined, (err, fd) => {
				if (err) {
					return reject(err);
				}
				this.fileDescriptor = fd;
				done();
			});
		});
	}

	async readLine() {
		return new Promise((done, reject) => {
			let buffer = Buffer.alloc(this.BUFFER_SIZE);
			fs.read(
				this.fileDescriptor,
				buffer,
				0,
				this.BUFFER_SIZE,
				null,
				(err, bytesRead) => {
					if (err) {
						reject(err);
					}
					if (bytesRead > 0) {
						this.remaining += buffer.toString("utf8", 0, bytesRead);
					}

					// Procesar línea por línea
					let lines = this.remaining.split("\n");
					const line = lines.shift();
					this.remaining = lines.join("\n"); // Guardar la última línea parcial para la próxima lectura
					done(line ? line : null);
				}
			);
		});
	}

	async writeLine(data) {
		return new Promise((done, reject) => {
			fs.write(this.fileDescriptor, `${data}\n`, (err) => {
				if (err) {
					return reject(err);
				}
				done();
			});
		});
	}

	async close() {
		return new Promise((done) => {
			fs.close(this.fileDescriptor, () => {
				done();
			});
		});
	}

	async delete() {
		return fs.unlinkSync(this.filepath);
	}

	static async CreateReader(filepath) {
		const fd = new FileSystem(filepath);
		await fd.init("r");
		return fd;
	}

	static async CreateWriter(filepath) {
		const fd = new FileSystem(filepath);
		await fd.init("w");
		return fd;
	}
	static async CreateAppendWriter(filepath) {
		const fd = new FileSystem(filepath);
		await fd.init("a");
		return fd;
	}
}
