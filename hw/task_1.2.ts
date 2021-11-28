import path from 'path';
import { createReadStream, createWriteStream } from 'fs';
import csvToJson from 'csvtojson/v2';
import { Writable, WritableOptions } from 'stream';
import { StringDecoder } from 'string_decoder';

class StringWritable extends Writable {
    _decoder: StringDecoder;
    data: string;

    constructor(options: WritableOptions) {
        super(options);
        this._decoder = new StringDecoder(options.defaultEncoding);
        this.data = '';
    }

    _write(chunk: Buffer | string, encoding: string, callback: () => void) {
        if (encoding === 'buffer') {
            chunk = this._decoder.write(chunk as Buffer);
        }
        this.data += chunk;
        console.log(this.data);
        callback();
    }

    _final(callback: () => void) {
        this.data += this._decoder.end();
        callback();
    }
}

const inputFilePath = path.join(__dirname, 'csv', 'input.csv');
const outputFilePath = path.resolve(__dirname, 'csv', 'output.txt');

const inputStream = createReadStream(inputFilePath);
const outputStream = createWriteStream(outputFilePath, { highWaterMark: 2 });
const dbStream = new StringWritable({
    highWaterMark: 1,
    defaultEncoding: 'utf8',
});

inputStream
    .on('error', (e) => {
        console.error(e);
    })
    .on('end', () => {
        outputStream.end();
        dbStream.end();
    });

outputStream.on('error', (e) => {
    console.error(e);
});

dbStream.on('error', (e) => {
    console.error(e);
});

type Book = {
    Book: string;
    Author: string;
    Amount: string;
    Price: string;
};

csvToJson()
    .fromStream(inputStream)
    .subscribe(
        (book: Book) => {
            return new Promise((resolve, reject) => {
                const line = `${JSON.stringify(book)}\n`;

                outputStream.write(line, (e) => {
                    if (e) {
                        console.error(e);

                        reject(e);
                    }
                });
                dbStream.write(line, (e) => {
                    if (e) {
                        console.error(e);

                        reject(e);
                    }
                });

                resolve();
            });
        },
        (e) => {
            console.error(e);
        },
    );
